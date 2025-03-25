#![doc = r"
The storage module is responsible for vector persistence.
To this end, it divides the vector into contiguous chunks, (de)serializes them with bincode, (de)compresses their serialized form with zstd and (reads)writes them to disk.

When storing a vector, it is important to ensure the entire vector is written, as missing chunks prevent the rest of the vector from being read.
The storage module aids in enforcing vector contiguity by preventing discontinuous chunks from being allocated. However, this is limited to chunk allocation, meaning multiple chunks can be allocated in the end of the vector and stored in any order.
It permits unordered writes to allow for write concurrency (implemented in the chunk cache module).

The storage module is implemented in terms of a low-level module -- chunk_io -- containing primitives to load/store objects to/from disk, taking care of (de)compression, (de)serialization and write atomicity (writing to a temporary file and atomically renaming it to the destination).
"]

use std::{
    fs, io,
    path::{Path, PathBuf},
};

mod chunk_io;
mod errors;
mod metadata;
mod stored_chunk;

pub use errors::*;
use metadata::Metadata;

use serde::{de::DeserializeOwned, Serialize};

use metadata::CURRENT_VERSION;

/// Chunk store manager.
///
/// Provides primitives for loading and storing chunks to/from disk, and loading/storing vector metadata.
/// It guarantees atomic writes for each chunk, and ensures the set of chunks is always contiguous on-disk.
/// It also provides a mechanism to truncate the vector, removing chunks from the end.
///
/// The store is implemented as a tree of directories, with each level of the tree representing a different part of the chunk index.
pub(crate) struct Store<Chunk> {
    root: PathBuf,
    metadata: Metadata<Chunk>,
    chunk_count: ChunkIdx,
}

impl<Chunk: Serialize + DeserializeOwned> Store<Chunk> {
    /// Open a store at the given root directory, creating it if it doesn't exist.
    ///
    /// This method performs a number of sanity checks on the store's metadata, and will fail if the
    /// store uses a different underlying chunk type or was created with a more recent version of hugevec.
    pub(crate) fn open(root: PathBuf) -> Result<Self, StoreError> {
        // ensure we're not opening a random directory as if it were a hugevec store
        let metadata = match Store::load_metadata(&root) {
            Ok(metadata) => metadata,
            Err(IoError::Io(e)) if e.kind() == io::ErrorKind::NotFound => {
                let metadata = Metadata::default();
                Store::store_metadata(&root, &metadata).map_err(StoreError::MetadataStore)?;
                metadata
            }
            Err(e) => return Err(StoreError::MetadataLoad(e)),
        };
        if metadata.version != CURRENT_VERSION {
            return Err(StoreError::UnsupportedVersion(metadata.version));
        }

        let mut store = Self {
            root,
            metadata,
            chunk_count: 0,
        };

        store.chunk_count = count_chunks_greedy(&store.root).map_err(StoreError::ChunkCount)?;

        Ok(store)
    }

    fn load_metadata(root: impl AsRef<Path>) -> Result<Metadata<Chunk>, IoError> {
        let path = root.as_ref().join("metadata");
        chunk_io::load(path)
    }
    fn store_metadata(root: impl AsRef<Path>, metadata: &Metadata<Chunk>) -> Result<(), IoError> {
        let path = root.as_ref().join("metadata");
        chunk_io::store(metadata, &path)
    }

    /// Store metadata.
    pub(crate) fn metadata(&self) -> &Metadata<Chunk> {
        &self.metadata
    }

    /// Update store metadata, writing it to disk.
    pub(crate) fn update_metadata(&mut self, metadata: Metadata<Chunk>) -> Result<(), StoreError> {
        self.metadata = metadata;
        Store::store_metadata(&self.root, &self.metadata).map_err(StoreError::MetadataStore)
    }

    pub(crate) fn load_chunk(&self, idx: ChunkIdx) -> Result<Chunk, ChunkLoadError> {
        let path = self.chunk_path(idx)?;
        let chunk = chunk_io::load(&path)?;
        Ok(chunk)
    }

    pub(crate) fn alloc_chunk(&mut self) -> Result<u64, StoreError> {
        if self.chunk_count == ChunkIdx::MAX {
            return Err(StoreError::MaxSize);
        }

        let new_chunk_idx = self.chunk_count;
        self.chunk_count += 1;

        // We need to create directories for the new chunks sometimes.
        // either we call create_dir_all in every store() call, or we do it here only when needed.
        // Since needing to create directories is rare (every ~2^16 chunks), we choose the latter.
        if new_chunk_idx & FIRST_LEVEL_BITMASK == 0 {
            let path = self
                .chunk_path(new_chunk_idx)
                .expect("chunk_count changed?");

            if let Some(dir) = path.parent() {
                match fs::create_dir_all(dir) {
                    Ok(()) => (),
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),

                    // HACK: this is kind of a chunk store error
                    Err(e) => return Err(StoreError::ChunkStore(ChunkStoreError::Io(e.into()))),
                }
            }
        }

        Ok(self.chunk_count)
    }

    pub(crate) fn store_chunk(&self, idx: ChunkIdx, chunk: Chunk) -> Result<(), ChunkStoreError> {
        let path = self.chunk_path(idx)?;
        chunk_io::store(&chunk, &path)?;

        Ok(())
    }

    pub(crate) fn truncate(&mut self, len: u64) -> Result<(), StoreError> {
        if self.chunk_count <= len {
            return Ok(());
        }

        // remove truncated chunks, starting from the end
        // TODO: use tree levels to optimize this (removing dirs rather than individual chunks when possible)
        for i in (len..=self.chunk_count).rev() {
            let path = self.chunk_path(i).expect("chunk_count changed?");
            fs::remove_file(&path).map_err(StoreError::ChunkDrop)?;
        }

        self.chunk_count = len;

        Ok(())
    }

    pub(crate) fn len(&self) -> u64 {
        self.chunk_count
    }

    fn chunk_path(&self, idx: ChunkIdx) -> Result<PathBuf, ChunkIndexOutOfBounds> {
        if idx >= self.chunk_count || idx == ChunkIdx::MAX {
            return Err(ChunkIndexOutOfBounds {
                idx,
                len: self.chunk_count,
            });
        }

        let mut path = self.root.clone();

        for i in 0..TREE_LEVELS {
            let segment_mask = (1 << LEVEL_BITS) - 1;
            let segment = (idx >> ((TREE_LEVELS - i - 1) * LEVEL_BITS)) & segment_mask;
            path.push(format!("{:04X}", segment));
        }
        Ok(path)
    }
}

pub type ChunkIdx = u64;
pub const TREE_LEVELS: usize = 4; // adjust chunk_path format accordingly if changed
const LEVEL_BITS: usize = std::mem::size_of::<ChunkIdx>() * 8 / TREE_LEVELS;
const FIRST_LEVEL_BITMASK: ChunkIdx = (1 << LEVEL_BITS) - 1;

fn count_chunks_greedy(root: &Path) -> Result<ChunkIdx, io::Error> {
    let mut count = 0;
    let mut path = root.to_owned();

    // descend through all levels of the tree but the last
    for lvl in 0..TREE_LEVELS - 1 {
        let mut max = None;
        let mut subd_count = 0;
        for subd in fs::read_dir(&path)? {
            let subd = subd?;
            if subd.file_type()?.is_dir() {
                let name = subd.file_name();
                let n =
                    u64::from_str_radix(name.to_str().expect("corrupted dir name (not utf-8)"), 16)
                        .expect("corrupted dir name (not hex string)");

                assert!(
                    n < 1 << LEVEL_BITS,
                    "corrupted dir name: level idx larger than max size"
                );

                if max.as_ref().map(|(max_n, _)| *max_n < n).unwrap_or(true) {
                    max = Some((n, name));
                }
                subd_count += 1;
            }
        }

        if let Some((max_n, name)) = max {
            assert_eq!(
                max_n + 1,
                subd_count,
                "subdir count inconsistent with dir contents"
            );
            count += subd_count.saturating_sub(1) << (LEVEL_BITS * (TREE_LEVELS - lvl - 1));
            path.push(name);
        } else if count == 0 && lvl == 0 {
            return Ok(0);
        } else {
            // TODO: backtrack on empty dirs
            panic!("corrupted chunk storage dir structure");
        }
    }

    // handle last level
    let mut max = None;
    let mut subf_count = 0;
    for subf in fs::read_dir(&path)? {
        let subf = subf?;
        if subf.file_type()?.is_file() {
            // last level only has leaf nodes - the chunk files themselves
            let name = subf.file_name();
            let n =
                u64::from_str_radix(name.to_str().expect("corrupted file name (not utf-8)"), 16)
                    .expect("corrupted file name (not hex string)");

            if max.as_ref().map(|max_n| *max_n < n).unwrap_or(true) {
                max = Some(n);
            }
            subf_count += 1;
        }
    }

    if let Some(max_n) = max {
        assert_eq!(
            max_n + 1,
            subf_count,
            "subfile count inconsistent with dir contents"
        );
        count += subf_count;
    } else if count != 0 {
        // TODO: backtrack on empty dirs
        panic!("corrupted chunk storage dir structure");
    }

    Ok(count)
}

#[cfg(test)]
mod test {
    use super::*;

    fn create_store<T: Serialize + DeserializeOwned + Default>() -> (tempfile::TempDir, Store<T>) {
        let tmpdir = tempfile::tempdir().unwrap();
        let store = Store::<T>::open(tmpdir.path().to_owned()).unwrap();
        (tmpdir, store)
    }

    #[test]
    fn initially_empty() {
        let (_handle, store) = create_store::<i32>();
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn loads_see_stores() {
        let (_handle, mut store) = create_store();

        let data = [0, 1, 2, 3, 4];

        for (idx, _v) in data.iter().copied().enumerate() {
            assert!(
                matches!(
                    store.load_chunk(idx as ChunkIdx),
                    Err(ChunkLoadError::IndexOutOfBounds(ChunkIndexOutOfBounds {
                        idx: idx_,
                        len: 0
                    })) if idx_ == idx as ChunkIdx
                ),
                "load before store"
            );
        }

        for (idx, v) in data.iter().copied().enumerate() {
            store.alloc_chunk().expect("morechunks fail");
            store.store_chunk(idx as ChunkIdx, v).expect("store fail");
        }
        assert_eq!(store.len(), data.len() as ChunkIdx, "len mismatch");

        for (idx, v) in data.iter().copied().enumerate() {
            assert_eq!(
                store.load_chunk(idx as ChunkIdx).expect("load fail"),
                v,
                "load mismatch"
            );
        }
    }

    #[test]
    fn reopen() {
        let (dir, mut store) = create_store();

        let data = [0, 1, 2, 3, 4];

        for (idx, v) in data.iter().copied().enumerate() {
            store.alloc_chunk().expect("morechunks fail");
            store.store_chunk(idx as ChunkIdx, v).expect("store fail");
        }

        std::mem::drop(store);
        let store = Store::<i32>::open(dir.path().to_owned()).expect("reopen fail");
        assert_eq!(
            store.len(),
            data.len() as ChunkIdx,
            "len mismatch after reopen"
        );

        for (idx, v) in data.iter().copied().enumerate() {
            assert_eq!(
                store.load_chunk(idx as ChunkIdx).expect("load fail"),
                v,
                "load mismatch after reopen"
            );
        }
    }

    #[test]
    fn store_fail_full() {
        let (_handle, store) = create_store();
        assert!(matches!(
            store.store_chunk(0, 0),
            Err(ChunkStoreError::IndexOutOfBounds(ChunkIndexOutOfBounds {
                idx: 0,
                len: 0
            }))
        ));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn store_load_many_reopen() {
        let tempdir = tempfile::tempdir_in("/dev/shm").unwrap();
        let mut store: Store<()> = Store::open(tempdir.path().to_owned()).unwrap();

        for i in 0..3 * FIRST_LEVEL_BITMASK {
            store.alloc_chunk().expect("alloc fail");
            store.store_chunk(i, ()).expect("store fail");
        }

        assert_eq!(store.len(), 3 * FIRST_LEVEL_BITMASK);
        std::mem::drop(store);

        let store: Store<()> = Store::open(tempdir.path().to_owned()).unwrap();
        assert_eq!(store.len(), 3 * FIRST_LEVEL_BITMASK);
        for i in 0..3 * FIRST_LEVEL_BITMASK {
            store.load_chunk(i).expect("load fail");
        }
    }
}
