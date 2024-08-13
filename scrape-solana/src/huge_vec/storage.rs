use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    borrow::Borrow,
    fmt::Debug,
    fs,
    io::{self, BufReader, BufWriter, Seek},
    marker::PhantomData,
    ops::BitAnd,
    path::{Path, PathBuf},
};

pub trait IndexedStorage<T> {
    type Error: std::error::Error + Send + Sync + 'static;

    fn load(&self, idx: usize) -> Result<T, Self::Error>;
    fn store(&mut self, idx: usize, object: impl Borrow<T>) -> Result<(), Self::Error>;
    fn truncate(&mut self, len: usize) -> Result<(), Self::Error>;
    fn len(&self) -> Result<usize, Self::Error>;
    fn clear(&mut self) -> Result<(), Self::Error> {
        self.truncate(0)
    }
}

use crate::huge_vec::IOTransformer;

pub struct FsStore<T, IOT> {
    root: PathBuf,
    metadata: BasicStorageMeta,
    metadata_file: fs::File,
    io_transformer: IOT,
    _t: PhantomData<T>,
}

#[derive(thiserror::Error, Debug)]
pub enum FsStoreError<IOTErr> {
    #[error("Could not create directory")]
    DirCreate(#[source] io::Error),

    #[error("Could not open metadata file")]
    MetadataOpen(#[source] io::Error),

    #[error("Could not seek within metadata")]
    MetadataSeek(#[source] io::Error),

    #[error("Could not read/parse metadata")]
    MetadataRead(#[source] Box<bincode::ErrorKind>),

    #[error("Could not write metadata")]
    MetadataWrite(#[source] Box<bincode::ErrorKind>),

    #[error("Could not open stored object file")]
    DataOpen(#[source] io::Error),

    #[error("Could not delete stored object file")]
    DataRemove(#[source] io::Error),

    #[error("Could not load stored object")]
    DataLoad(#[source] Box<bincode::ErrorKind>),

    #[error("Could not store object")]
    DataStore(#[source] Box<bincode::ErrorKind>),

    #[error("Tried to store non-contiguous data (expected index<={len}, got {idx})")]
    NonContiguousStore { len: usize, idx: usize },

    #[error("Cannot load index {idx}: out of bounds (len={len})")]
    OutOfBoundsLoad { len: usize, idx: usize },

    #[error("I/O error (from I/O transformer)")]
    IOError(#[from] IOTErr),
}

impl<T, IOT> FsStore<T, IOT>
where
    IOT: IOTransformer,
    T: Serialize + DeserializeOwned + Debug,
{
    pub fn open(
        root: impl AsRef<Path>,
        io_transformer: IOT,
    ) -> Result<Self, FsStoreError<IOT::Error>> {
        fs::create_dir_all(root.as_ref()).map_err(FsStoreError::DirCreate)?;

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
            .map_err(FsStoreError::MetadataSeek)?;

        let metadata: BasicStorageMeta = if metadata_size == 0 {
            BasicStorageMeta::default()
        } else {
            let reader = BufReader::new(&metadata_file);
            io_transformer.wrap_read(reader, |r| {
                bincode::deserialize_from(r).map_err(FsStoreError::MetadataRead)
            })?
        };

        Ok(Self {
            root: root.as_ref().to_owned(),
            metadata,
            metadata_file,
            io_transformer,
            _t: PhantomData,
        })
    }

    fn write_metadata(&mut self) -> Result<(), FsStoreError<IOT::Error>> {
        self.metadata_file
            .seek(io::SeekFrom::Start(0))
            .map_err(FsStoreError::MetadataSeek)?;

        let writer = BufWriter::new(&mut self.metadata_file);
        self.io_transformer.wrap_write(writer, |w| {
            bincode::serialize_into(w, &self.metadata).map_err(FsStoreError::MetadataWrite)
        })?;

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
    IOT: IOTransformer,
    T: Serialize + DeserializeOwned + Debug,
{
    type Error = FsStoreError<IOT::Error>;

    fn load(&self, idx: usize) -> Result<T, Self::Error> {
        if idx >= self.metadata.len {
            return Err(FsStoreError::OutOfBoundsLoad {
                len: self.metadata.len,
                idx,
            });
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
        match idx.cmp(&self.metadata.len) {
            std::cmp::Ordering::Equal => {
                self.metadata.len += 1;
                self.write_metadata()?;
            }
            std::cmp::Ordering::Greater => {
                return Err(FsStoreError::NonContiguousStore {
                    len: self.metadata.len,
                    idx,
                });
            }
            _ => (),
        }

        let path = self.index_path(idx);
        fs::create_dir_all(path.parent().unwrap()).map_err(FsStoreError::DataOpen)?; // unwrap is safe because we know the path has a parent

        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
            .map_err(FsStoreError::DataOpen)?;

        self.io_transformer.wrap_write(file, |w| {
            bincode::serialize_into(w, object.borrow()).map_err(FsStoreError::DataStore)
        })?;

        Ok(())
    }

    fn len(&self) -> Result<usize, Self::Error> {
        Ok(self.metadata.len)
    }

    fn truncate(&mut self, new_len: usize) -> Result<(), Self::Error> {
        if new_len >= self.metadata.len {
            return Ok(());
        }

        let old_len = self.metadata.len;
        self.metadata.len = new_len;

        match self.write_metadata() {
            Ok(_) => (),
            Err(e) => {
                self.metadata.len = old_len;
                return Err(e);
            }
        }

        for idx in new_len..old_len {
            let path = self.index_path(idx);
            fs::remove_file(path).map_err(FsStoreError::DataRemove)?;
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Default)]
struct BasicStorageMeta {
    len: usize,
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
