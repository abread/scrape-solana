use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::mem::{size_of, MaybeUninit};
use std::ops::Deref;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::ptr::addr_of;

use eyre::Context;
use mmap_vec::MmapVec;
use vector_trees::btree::{BVecTreeMapData, BVecTreeNode};
use vector_trees::BVecTreeMap;

type MmapMapInner<K, V> = BVecTreeMap<MmapVec<BVecTreeNode<K, V>>, K, V>;
pub struct MmapMap<K, V> {
    map: MmapMapInner<K, V>,
    meta_storage: MapMetaStorage,
}

impl<K: Unpin + Ord + Debug, V: Unpin + Debug> MmapMap<K, V> {
    pub unsafe fn with_name(path: PathBuf) -> eyre::Result<Self> {
        let (meta_storage, meta) = MapMetaStorage::open(path.with_extension("map_meta"))
            .wrap_err("failed to open map metadata")?;

        let map_vec = MmapVec::with_name(path).wrap_err("failed to open map inner vec")?;

        let map = unsafe {
            BVecTreeMap::from_raw(BVecTreeMapData {
                root: meta.root,
                free_head: meta.free_head,
                tree_buf: map_vec,
                len: meta.len as usize,
                _phantom: std::marker::PhantomData,
            })
        };

        Ok(Self { map, meta_storage })
    }
}

impl<K, V> Deref for MmapMap<K, V> {
    type Target = MmapMapInner<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<K, V> MmapMap<K, V>
where
    K: Unpin + Ord + Debug,
    V: Unpin + Debug,
{
    pub fn clear(&mut self) -> eyre::Result<()> {
        self.map.clear();
        self.sync_meta()
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.map.get_mut(key)
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.map.insert(key, value)
    }

    pub fn remove(&mut self, key: &K) -> eyre::Result<Option<V>> {
        let res = self.map.remove(key);
        self.sync_meta()?;
        Ok(res)
    }

    pub fn remove_entry(&mut self, key: &K) -> eyre::Result<Option<(K, V)>> {
        let res = self.map.remove_entry(key);
        self.sync_meta()?;
        Ok(res)
    }
}

impl<K: Unpin, V: Unpin> MmapMap<K, V> {
    fn sync_meta(&mut self) -> eyre::Result<()> {
        self.map.inner().tree_buf.sync_meta()?;
        self.meta_storage.write(self.map.inner())?;
        Ok(())
    }

    pub fn sync(&mut self) -> eyre::Result<()> {
        self.map.inner().tree_buf.sync()?;
        self.meta_storage.write(self.map.inner())?;
        Ok(())
    }
}

struct MapMetaStorage(File);

#[repr(C)]
struct MapMetaRaw {
    root: Option<nonmax::NonMaxU64>,
    free_head: Option<nonmax::NonMaxU64>,
    len: u64,
}

impl MapMetaStorage {
    fn open(path: impl AsRef<Path>) -> eyre::Result<(Self, MapMetaRaw)> {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .wrap_err("could not open map metadata file")?;

        let f_meta = f
            .metadata()
            .wrap_err("could not read map metadata file metadata")?;
        eyre::ensure!(
            f_meta.is_file(),
            "map metadata file is corrupted: must be a regular file"
        );

        if f_meta.size() == 0 {
            Ok((MapMetaStorage(f), MapMetaRaw::default()))
        } else if f_meta.size() == size_of::<MapMetaRaw>() as u64 {
            let mut map_meta = MaybeUninit::uninit();
            {
                let mutref = unsafe {
                    core::slice::from_raw_parts_mut(
                        map_meta.as_mut_ptr() as *mut u8,
                        size_of::<MapMetaRaw>(),
                    )
                };

                f.read_exact(mutref)
                    .wrap_err("could not read map metadata from file")?;

                // drop mutref
            }

            Ok((MapMetaStorage(f), unsafe { map_meta.assume_init() }))
        } else {
            Err(eyre::eyre!(
                "map metadata file is corrupted: must be empty or exactly-sized"
            ))
        }
    }

    fn write<S, K, V>(&mut self, map_data: &BVecTreeMapData<S, K, V>) -> eyre::Result<()> {
        self.0
            .seek(io::SeekFrom::Start(0))
            .wrap_err("could not seek to start of map metadata file")?;

        let meta_raw = MapMetaRaw {
            root: map_data.root,
            free_head: map_data.free_head,
            len: map_data.len as u64,
        };
        let meta_raw_bytes = unsafe {
            core::slice::from_raw_parts(addr_of!(meta_raw) as *const u8, size_of::<MapMetaRaw>())
        };

        self.0
            .write_all(meta_raw_bytes)
            .wrap_err("could not write map metadata file")?;

        self.0
            .sync_all()
            .wrap_err("could not fsync map metadata file")?;

        Ok(())
    }
}

impl Default for MapMetaRaw {
    fn default() -> Self {
        MapMetaRaw {
            root: None,
            free_head: None,
            len: 0,
        }
    }
}
