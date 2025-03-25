use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    borrow::Borrow,
    collections::HashMap,
    fmt::Debug,
    fs,
    io::{self, BufReader, BufWriter, Seek},
    marker::PhantomData,
    ops::BitAnd,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc, Condvar, LazyLock, Mutex,
    },
};
use tempfile::NamedTempFile;

static ASYNC_WRITE_THREADPOOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
    rayon::ThreadPoolBuilder::new()
        .num_threads(0)
        .thread_name(|i| format!("async-write-{i}"))
        .build()
        .expect("failed to create async write threadpool")
});

pub trait IndexedStorage<T> {
    type Error: std::error::Error + Send + Sync + 'static;

    fn load(&self, idx: usize) -> Result<T, Self::Error>;
    fn store(&mut self, idx: usize, object: impl Borrow<T>) -> Result<(), Self::Error>;
    fn truncate(&mut self, len: usize) -> Result<(), Self::Error>;
    fn len(&self) -> Result<usize, Self::Error>;
    fn is_empty(&self) -> Result<bool, Self::Error> {
        self.len().map(|len| len == 0)
    }
    fn clear(&mut self) -> Result<(), Self::Error> {
        self.truncate(0)
    }

    fn sync(&mut self) -> Result<(), Self::Error>;
}

use crate::huge_vec::IOTransformer;

pub struct FsStore<T, IOT>
where
    IOT: IOTransformer + Send + Sync + 'static,
    T: Serialize + DeserializeOwned + Debug + Send + Sync + Clone + 'static,
{
    root: Arc<PathBuf>,
    clock: Arc<AtomicU64>,
    metadata: BasicStorageMeta,
    io_transformer: Arc<IOT>,
    async_write_state: Arc<AsyncWriteState<T>>,
    write_err_tx: SyncSender<FsStoreError<IOT::Error>>,
    write_err_rx: Mutex<Receiver<FsStoreError<IOT::Error>>>,
    metadata_write_lock: Arc<Mutex<()>>,
    _t: PhantomData<T>,
}

struct AsyncWriteState<T> {
    pending: Mutex<HashMap<usize, Arc<T>>>,
    cond_pending_empty: Condvar,
}

#[derive(thiserror::Error, Debug)]
pub enum FsStoreError<IOTErr> {
    #[error("Could not create directory")]
    DirCreate(#[source] io::Error),

    #[error("Could not open metadata file")]
    MetadataOpen(#[source] io::Error),

    #[error("Could not read/parse metadata")]
    MetadataRead(#[source] Box<bincode::ErrorKind>),

    #[error("Could not write metadata")]
    MetadataWrite(#[source] Box<bincode::ErrorKind>),

    #[error("Could not persist new metadata")]
    MetadataPersist(#[source] tempfile::PersistError),

    #[error("Could not open stored object file")]
    DataOpen(#[source] io::Error),

    #[error("Could not delete stored object file")]
    DataRemove(#[source] io::Error),

    #[error("Could not load stored object")]
    DataLoad(#[source] Box<bincode::ErrorKind>),

    #[error("Could not store object")]
    DataStore(#[source] Box<bincode::ErrorKind>),

    #[error("Could not persist new object version")]
    DataPersist(#[source] tempfile::PersistError),

    #[error("Tried to store non-contiguous data (expected index<={len}, got {idx})")]
    NonContiguousStore { len: usize, idx: usize },

    #[error("Cannot load index {idx}: out of bounds (len={len})")]
    OutOfBoundsLoad { len: usize, idx: usize },

    #[error("I/O error (from I/O transformer)")]
    IOError(#[from] IOTErr),

    #[error("Store corrupted")]
    StoreCorrupted,
}

impl<T, IOT> FsStore<T, IOT>
where
    IOT: IOTransformer + Send + Sync + 'static,
    T: Serialize + DeserializeOwned + Debug + Send + Sync + Clone + 'static,
{
    pub fn open(
        root: impl AsRef<Path>,
        io_transformer: IOT,
    ) -> Result<Self, FsStoreError<IOT::Error>> {
        fs::create_dir_all(root.as_ref()).map_err(FsStoreError::DirCreate)?;

        let metadata = BasicStorageMeta::read_or_default(&root, &io_transformer)?;

        let (write_err_tx, write_err_rx) =
            sync_channel(ASYNC_WRITE_THREADPOOL.current_num_threads() * 2);

        let mut store = Self {
            root: Arc::new(root.as_ref().to_owned()),
            clock: Arc::new(AtomicU64::new(0)),
            metadata,
            io_transformer: Arc::new(io_transformer),
            async_write_state: Arc::new(AsyncWriteState {
                pending: Mutex::new(HashMap::with_capacity(
                    ASYNC_WRITE_THREADPOOL.current_num_threads() * 2,
                )),
                cond_pending_empty: Condvar::new(),
            }),
            write_err_tx,
            write_err_rx: Mutex::new(write_err_rx),
            metadata_write_lock: Arc::new(Mutex::new(())),
            _t: PhantomData,
        };
        store.heal()?;

        Ok(store)
    }

    fn heal(&mut self) -> Result<(), FsStoreError<IOT::Error>> {
        if self.is_empty()? {
            return Ok(());
        }

        // prune corrupted trailing objects
        let orig_len = self.metadata.len;
        while self.metadata.len > 0 {
            if self.load(self.metadata.len - 1).is_ok() {
                break;
            }

            match std::fs::remove_file(self.index_path(self.metadata.len - 1)) {
                Ok(_) => (),
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                Err(e) => return Err(FsStoreError::DataRemove(e)),
            }
            self.metadata.len -= 1;
        }

        if self.metadata.len != orig_len {
            self.metadata.write_to(&*self.root, &*self.io_transformer)?;
        }

        Ok(())
    }

    fn index_path(&self, idx: usize) -> PathBuf {
        let idx = idx as u64;
        let idx_prefix1 = idx.bitand(0xffff_0000_0000_0000) >> (16 * 3);
        let idx_prefix2 = idx.bitand(0x0000_ffff_0000_0000) >> (16 * 2);
        let idx_prefix3 = idx.bitand(0x0000_0000_ffff_0000) >> 16;
        self.root.as_path().join(format!(
            "data.{idx_prefix1:04x}/{idx_prefix2:04x}/{idx_prefix3:04x}/{idx:016x}"
        ))
    }
}

impl<T, IOT> IndexedStorage<T> for FsStore<T, IOT>
where
    IOT: IOTransformer + Send + Sync + 'static,
    T: Serialize + DeserializeOwned + Debug + Send + Sync + Clone + 'static,
{
    type Error = FsStoreError<IOT::Error>;

    fn load(&self, idx: usize) -> Result<T, Self::Error> {
        if idx >= self.metadata.len {
            return Err(FsStoreError::OutOfBoundsLoad {
                len: self.metadata.len,
                idx,
            });
        }

        {
            // consistency: make pending writes visible
            let pending_stores = self
                .async_write_state
                .pending
                .lock()
                .expect("lock poisoned");
            if let Some(obj) = pending_stores.get(&idx) {
                return Ok(obj.as_ref().clone());
            }
        }

        let path = self.index_path(idx);
        let file = fs::OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(FsStoreError::DataOpen)?;
        let reader = BufReader::new(file);

        self.io_transformer.wrap_read(reader, |r| {
            bincode::deserialize_from(r).map_err(FsStoreError::DataLoad)
        })
    }

    fn store(&mut self, idx: usize, object: impl Borrow<T>) -> Result<(), Self::Error> {
        let metadata_time = match idx.cmp(&self.metadata.len) {
            std::cmp::Ordering::Equal => {
                self.metadata.len += 1;
                self.clock.fetch_add(1, Ordering::SeqCst) + 1
            }
            std::cmp::Ordering::Greater => {
                return Err(FsStoreError::NonContiguousStore {
                    len: self.metadata.len,
                    idx,
                });
            }
            _ => 0,
        };

        let path = self.index_path(idx);

        let do_write = move |object: &T,
                             clock: &AtomicU64,
                             root: &Path,
                             io_transformer: &IOT,
                             new_metadata: &BasicStorageMeta,
                             meta_write_lock: &Mutex<()>|
              -> Result<(), FsStoreError<IOT::Error>> {
            fs::create_dir_all(path.parent().unwrap()).map_err(FsStoreError::DataOpen)?; // unwrap is safe because we know the path has a parent
            let mut tempfile = NamedTempFile::new_in(root).map_err(FsStoreError::DataOpen)?;

            io_transformer.wrap_write(&mut tempfile, |w| {
                bincode::serialize_into(w, object).map_err(FsStoreError::DataStore)
            })?;

            // persist new version of object without the risk of partial writes
            tempfile.persist(path).map_err(FsStoreError::DataPersist)?;

            if metadata_time != 0 && metadata_time >= clock.load(Ordering::SeqCst) {
                let _lock = meta_write_lock.lock().expect("lock poisoned");
                if metadata_time >= clock.load(Ordering::SeqCst) {
                    new_metadata.write_to(root, io_transformer)?;
                }
            }

            Ok(())
        };

        let mut pending_stores = self
            .async_write_state
            .pending
            .lock()
            .expect("lock poisoned");

        // ensure there are no overwrites
        while pending_stores.contains_key(&idx) {
            pending_stores = self
                .async_write_state
                .cond_pending_empty
                .wait(pending_stores)
                .expect("lock poisoned");
        }

        if pending_stores.len() < pending_stores.capacity() {
            // queue write
            let object = Arc::new(object.borrow().clone());
            pending_stores.insert(idx, Arc::clone(&object));
            std::mem::drop(pending_stores); // we don't need the lock anymore

            let clock = Arc::clone(&self.clock);
            let root = Arc::clone(&self.root);
            let io_transformer = Arc::clone(&self.io_transformer);
            let new_metadata = self.metadata.clone();
            let meta_write_lock = Arc::clone(&self.metadata_write_lock);
            let write_err_tx = self.write_err_tx.clone();
            let async_write_state = Arc::clone(&self.async_write_state);
            ASYNC_WRITE_THREADPOOL.spawn(move || {
                if let Err(e) = do_write(
                    &object,
                    &clock,
                    &root,
                    &io_transformer,
                    &new_metadata,
                    &meta_write_lock,
                ) {
                    let _ = write_err_tx.send(e);
                }

                async_write_state
                    .pending
                    .lock()
                    .expect("lock poisoned")
                    .remove(&idx);
                async_write_state.cond_pending_empty.notify_all();
            });

            Ok(())
        } else {
            if let Ok(err) = self
                .write_err_rx
                .get_mut()
                .expect("poisoned lock")
                .try_recv()
            {
                // revert metadata update
                if metadata_time != 0 {
                    self.metadata.len -= 1;
                }

                return Err(err);
            }

            do_write(
                object.borrow(),
                &self.clock,
                &self.root,
                &self.io_transformer,
                &self.metadata,
                &self.metadata_write_lock,
            )
        }
    }

    fn len(&self) -> Result<usize, Self::Error> {
        Ok(self.metadata.len)
    }

    fn truncate(&mut self, new_len: usize) -> Result<(), Self::Error> {
        if new_len >= self.metadata.len {
            return Ok(());
        }

        // must execute after all pending stores finish to avoid overwrites
        self.sync()?;

        // remove old objects
        let old_len = self.metadata.len;
        for idx in new_len..old_len {
            let path = self.index_path(idx);
            match fs::remove_file(path) {
                Ok(_) => Ok(()),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e),
            }
            .map_err(FsStoreError::DataRemove)?;
        }

        // update len
        self.metadata.len = new_len;
        // Note: safe to write without lock because there are no concurrent async writes
        // we called sync() and are holding a &mut reference to Self
        self.metadata.write_to(&*self.root, &*self.io_transformer)?;

        Ok(())
    }

    fn sync(&mut self) -> Result<(), Self::Error> {
        let mut pending_stores = self
            .async_write_state
            .pending
            .lock()
            .expect("lock poisoned");
        while !pending_stores.is_empty() {
            if let Ok(err) = self.write_err_rx.lock().expect("lock poisoned").try_recv() {
                return Err(err);
            }

            pending_stores = self
                .async_write_state
                .cond_pending_empty
                .wait(pending_stores)
                .expect("lock poisoned");
        }

        // call sync to ensure changes are truly persisted to disk
        rustix::fs::sync();

        Ok(())
    }
}

impl<T, IOT> Drop for FsStore<T, IOT>
where
    IOT: IOTransformer + Send + Sync + 'static,
    T: Serialize + DeserializeOwned + Debug + Send + Sync + Clone + 'static,
{
    fn drop(&mut self) {
        self.sync().expect("storage sync fail");
        if let Ok(err) = self
            .write_err_rx
            .get_mut()
            .expect("poisoned lock")
            .try_recv()
        {
            panic!("sync fail: {err:#?}");
        }
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct BasicStorageMeta {
    len: usize,
}

impl BasicStorageMeta {
    fn read_or_default<IOT: IOTransformer>(
        root: impl AsRef<Path>,
        io_transformer: &IOT,
    ) -> Result<Self, FsStoreError<IOT::Error>> {
        let mut metadata_file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(root.as_ref().join("meta"))
            .map_err(FsStoreError::MetadataOpen)?;

        let metadata_size = metadata_file
            .seek(io::SeekFrom::End(0))
            .and_then(|_| metadata_file.stream_position())
            .and_then(|size| metadata_file.seek(io::SeekFrom::Start(0)).map(|_| size))
            .map_err(FsStoreError::MetadataOpen)?;

        let metadata = if metadata_size == 0 {
            BasicStorageMeta::default()
        } else {
            let reader = BufReader::new(&metadata_file);
            io_transformer.wrap_read(reader, |r| {
                bincode::deserialize_from(r).map_err(FsStoreError::MetadataRead)
            })?
        };

        Ok(metadata)
    }

    fn write_to<IOT: IOTransformer>(
        &self,
        root: impl AsRef<Path>,
        io_transformer: &IOT,
    ) -> Result<(), FsStoreError<IOT::Error>> {
        let meta_path = root.as_ref().join("meta");
        let mut metadata_file = NamedTempFile::new_in(root).map_err(FsStoreError::MetadataOpen)?;

        let writer = BufWriter::new(&mut metadata_file);
        io_transformer.wrap_write(writer, |w| {
            bincode::serialize_into(w, self).map_err(FsStoreError::MetadataWrite)
        })?;

        // persist new version of metadata without the risk of partial writes
        metadata_file
            .persist(meta_path)
            .map_err(FsStoreError::MetadataPersist)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    // write a test for the FsStore struct using tempdir crate
    use super::*;
    use tempdir::TempDir;

    #[test]
    fn fs_store_test() {
        let temp_dir = TempDir::new("fs_store_test").unwrap();
        let store_builder = || FsStore::open(temp_dir.path(), ()).unwrap();

        {
            let mut store = store_builder();

            // store a bunch of stuff
            for idx in 0..100 {
                store.store(idx, idx * 2).unwrap();
            }

            // can't store non-contiguous objects
            assert!(matches!(
                store.store(101, 0).unwrap_err(),
                FsStoreError::NonContiguousStore { len: 100, idx: 101 }
            ));

            // check everything was stored in memory
            assert_eq!(store.len().unwrap(), 100);

            for idx in 0..100 {
                let val = store.load(idx).unwrap();
                assert_eq!(val, idx * 2);
            }

            // can't read out of bounds
            assert!(matches!(
                store.load(101).unwrap_err(),
                FsStoreError::OutOfBoundsLoad { len: 100, idx: 101 }
            ));
        }

        {
            // reopen the store and check that everything is still there
            let mut store = store_builder();

            assert_eq!(store.len().unwrap(), 100);

            for idx in 0..100 {
                let val = store.load(idx).unwrap();
                assert_eq!(val, idx * 2);
            }

            // still can't store non-contiguous objects
            assert!(matches!(
                store.store(101, 0).unwrap_err(),
                FsStoreError::NonContiguousStore { len: 100, idx: 101 }
            ));
            // still can't read out of bounds
            assert!(matches!(
                store.load(101).unwrap_err(),
                FsStoreError::OutOfBoundsLoad { len: 100, idx: 101 }
            ));
        }
    }
}
