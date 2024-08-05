use std::cell::RefCell;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use eyre::WrapErr;
use nonmax::NonMaxU64;
use serde::Deserialize;
use serde::{de::DeserializeOwned, Serialize};
use vector_trees::btree::{BVecTreeMapData, BVecTreeNode};
use vector_trees::BVecTreeMap;

use crate::huge_vec::{self, Chunk, FsStore, FsStoreError, HugeVec, IOTransformer, IndexedStorage};

type HugeMapInner<K, V, MStore: MapStore<K, V, SZ>, const SZ: usize> =
    BVecTreeMap<HugeVec<BVecTreeNode<K, V>, MStore::VecStore, SZ>, K, V>;
pub struct HugeMap<K: Debug, V: Debug, MStore: MapStore<K, V, SZ>, const SZ: usize = 4096> {
    map: HugeMapInner<K, V, MStore, SZ>,
    meta_store: MStore::MapMetaStore,
}

impl<K, V, MStore, const CHUNK_SZ: usize> HugeMap<K, V, MStore, CHUNK_SZ>
where
    K: Debug,
    V: Debug,
    MStore: MapStore<K, V, CHUNK_SZ>,
    <MStore::VecStore as huge_vec::IndexedStorage<Chunk<BVecTreeNode<K, V>, CHUNK_SZ>>>::Error:
        std::error::Error + Send + Sync + 'static,
{
    pub fn open(store: MStore) -> eyre::Result<Self> {
        let (meta_store, vec_store) = store.open()?;

        let meta = meta_store.load_metadata()?;

        let map_vec = HugeVec::new(vec_store).wrap_err("failed to open data store")?;

        let map = unsafe {
            BVecTreeMap::from_raw(BVecTreeMapData {
                root: meta.root,
                free_head: meta.free_head,
                tree_buf: map_vec,
                len: meta.len as usize,
                _phantom: std::marker::PhantomData,
            })
        };

        Ok(Self { map, meta_store })
    }
}

impl<K, V, MStore, const CHUNK_SZ: usize> Deref for HugeMap<K, V, MStore, CHUNK_SZ>
where
    K: Debug,
    V: Debug,
    MStore: MapStore<K, V, CHUNK_SZ>,
{
    type Target = HugeMapInner<K, V, MStore, CHUNK_SZ>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<K, V, MStore, const CHUNK_SZ: usize> HugeMap<K, V, MStore, CHUNK_SZ>
where
    K: Debug + Ord,
    V: Debug,
    MStore: MapStore<K, V, CHUNK_SZ>,
    <MStore::VecStore as huge_vec::IndexedStorage<Chunk<BVecTreeNode<K, V>, CHUNK_SZ>>>::Error:
        std::error::Error + Send + Sync + 'static,
{
    pub fn clear(&mut self) {
        self.map.clear();
        self.write_metadata().unwrap();
    }

    pub fn get(&self, key: &K) -> Option<impl vector_trees::Ref<'_, V>> {
        self.map.get(key)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<impl vector_trees::RefMut<'_, V>> {
        self.map.get_mut(key)
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let v = self.map.insert(key, value);
        self.write_metadata().unwrap();
        v
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let v = self.map.remove(key);
        self.write_metadata().unwrap();
        v
    }

    pub fn remove_entry(&mut self, key: &K) -> Option<(K, V)> {
        let v = self.map.remove_entry(key);
        self.write_metadata().unwrap();
        v
    }

    pub fn sync(&mut self) -> eyre::Result<()> {
        unsafe { self.map.inner_mut() }
            .tree_buf
            .sync()
            .wrap_err("failed to sync map data")?;
        self.write_metadata()?;
        Ok(())
    }

    fn write_metadata(&mut self) -> eyre::Result<()> {
        let meta = self.map.inner();
        self.meta_store.store_metadata(StoredMapMeta {
            root: meta.root.map(|r| r.into()),
            free_head: meta.free_head.map(|fh| fh.into()),
            len: meta.len as u64,
        })?;
        Ok(())
    }
}

pub trait MapStore<K, V, const CHUNK_SZ: usize> {
    type VecStore: IndexedStorage<Chunk<BVecTreeNode<K, V>, CHUNK_SZ>>;
    type MapMetaStore: MapMetaStore;
    type Error: std::error::Error + Send + Sync + 'static;

    fn open(self) -> Result<(Self::MapMetaStore, Self::VecStore), Self::Error>;
}

pub struct MapFsStore<IOT>(PathBuf, IOT);
impl<IOT: IOTransformer> MapFsStore<IOT> {
    pub fn new(path: impl Into<PathBuf>, io_transformer: IOT) -> Self {
        Self(path.into(), io_transformer)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum MapFsStoreError<IOTErr> {
    #[error("error in map metadata store")]
    MetaStoreError(#[from] bincode::Error),
    #[error("error in map data store")]
    DataStoreError(#[from] FsStoreError<IOTErr>),
}

impl<IOT: IOTransformer, K, V, const CHUNK_SZ: usize> MapStore<K, V, CHUNK_SZ> for MapFsStore<IOT>
where
    BVecTreeNode<K, V>: Serialize + DeserializeOwned + Debug,
    FsStore<Chunk<BVecTreeNode<K, V>, CHUNK_SZ>, IOT>:
        IndexedStorage<Chunk<BVecTreeNode<K, V>, CHUNK_SZ>>,
    MapFsStoreError<IOT::Error>: std::error::Error + Send + Sync + 'static,
{
    type MapMetaStore = MapMetaFsStore;
    type VecStore = FsStore<Chunk<BVecTreeNode<K, V>, CHUNK_SZ>, IOT>;
    type Error = MapFsStoreError<IOT::Error>;

    fn open(self) -> Result<(Self::MapMetaStore, Self::VecStore), Self::Error> {
        let map_meta_store = MapMetaFsStore::open(self.0.join("map_meta"))?;
        let vec_store = FsStore::open(self.0.join("map_data"), self.1)?;
        Ok((map_meta_store, vec_store))
    }
}

pub trait MapMetaStore {
    type Error: std::error::Error + Send + Sync + 'static;

    fn load_metadata(&self) -> Result<StoredMapMeta, Self::Error>;
    fn store_metadata(&mut self, meta: StoredMapMeta) -> Result<(), Self::Error>;
}

struct MapMetaFsStore(RefCell<File>);
impl MapMetaFsStore {
    fn open(path: impl AsRef<Path>) -> Result<Self, bincode::Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .map_err(|e| Box::new(bincode::ErrorKind::Io(e)))?;
        Ok(Self(RefCell::new(file)))
    }
}

impl MapMetaStore for MapMetaFsStore {
    type Error = bincode::Error;

    fn load_metadata(&self) -> Result<StoredMapMeta, Self::Error> {
        let mut metadata_file = self.0.borrow_mut();

        let metadata_size = metadata_file
            .seek(io::SeekFrom::End(0))
            .and_then(|_| metadata_file.stream_position())
            .and_then(|size| metadata_file.seek(io::SeekFrom::Start(0)).map(|_| size))?;

        if metadata_size == 0 {
            Ok(StoredMapMeta::default())
        } else {
            bincode::deserialize_from(&*metadata_file)
        }
    }

    fn store_metadata(&mut self, metadata: StoredMapMeta) -> Result<(), Self::Error> {
        let metadata_file = self.0.get_mut();

        metadata_file
            .seek(io::SeekFrom::Start(0))
            .map_err(|e| Box::new(bincode::ErrorKind::Io(e)))?;

        let mut writer = BufWriter::new(metadata_file);
        bincode::serialize_into(&mut writer, &metadata)?;
        writer
            .flush()
            .map_err(|e| Box::new(bincode::ErrorKind::Io(e)))?;

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct StoredMapMeta {
    root: Option<NonMaxU64>,
    free_head: Option<NonMaxU64>,
    len: u64,
}

impl Default for StoredMapMeta {
    fn default() -> Self {
        StoredMapMeta {
            root: None,
            free_head: None,
            len: 0,
        }
    }
}
