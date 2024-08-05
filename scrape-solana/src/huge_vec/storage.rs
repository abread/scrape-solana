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
    fn len(&self) -> Result<usize, Self::Error>;
    fn clear(&mut self) -> Result<(), Self::Error>;
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
    DirCreateError(#[source] io::Error),

    #[error("Could not open metadata file")]
    MetadataOpenError(#[source] io::Error),

    #[error("Could not seek within metadata")]
    MetadataSeekError(#[source] io::Error),

    #[error("Could not create metadata reader")]
    MetadataReadPrepError(#[source] IOTErr),

    #[error("Could not read/parse metadata")]
    MetadataReadError(#[source] Box<bincode::ErrorKind>),

    #[error("Could not create metadata writer")]
    MetadataWritePrepError(#[source] IOTErr),

    #[error("Could not write metadata")]
    MetadataWriteError(#[source] Box<bincode::ErrorKind>),

    #[error("Could not open stored object file")]
    DataOpenError(#[source] io::Error),

    #[error("Could not delete stored object file")]
    DataRemoveError(#[source] io::Error),

    #[error("Could not create object loader")]
    DataLoadPrepError(#[source] IOTErr),

    #[error("Could not load stored object")]
    DataLoadError(#[source] Box<bincode::ErrorKind>),

    #[error("Could not create object storer")]
    DataStorePrepError(#[source] IOTErr),

    #[error("Could not store object")]
    DataStoreError(#[source] Box<bincode::ErrorKind>),

    #[error("Tried to store non-contiguous data (expected index<={len}, got {idx})")]
    NonContiguousStoreError { len: usize, idx: usize },

    #[error("Cannot load index {idx}: out of bounds (len={len})")]
    OutOfBoundsLoadError { len: usize, idx: usize },
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
        fs::create_dir_all(root.as_ref()).map_err(FsStoreError::DirCreateError)?;

        let mut metadata_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(root.as_ref().join("meta"))
            .map_err(FsStoreError::MetadataOpenError)?;

        let metadata_size = metadata_file
            .seek(io::SeekFrom::End(0))
            .and_then(|_| metadata_file.stream_position())
            .and_then(|size| metadata_file.seek(io::SeekFrom::Start(0)).map(|_| size))
            .map_err(FsStoreError::MetadataSeekError)?;

        let metadata: BasicStorageMeta = if metadata_size == 0 {
            BasicStorageMeta::default()
        } else {
            let reader = BufReader::new(&metadata_file);
            let reader = io_transformer
                .wrap_reader(reader)
                .map_err(FsStoreError::MetadataReadPrepError)?;
            bincode::deserialize_from(reader).map_err(FsStoreError::MetadataReadError)?
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
        use io::Write;

        self.metadata_file
            .seek(io::SeekFrom::Start(0))
            .map_err(FsStoreError::MetadataSeekError)?;

        let writer = BufWriter::new(&mut self.metadata_file);
        let mut writer = self
            .io_transformer
            .wrap_writer(writer)
            .map_err(FsStoreError::MetadataWritePrepError)?;
        bincode::serialize_into(&mut writer, &self.metadata)
            .map_err(FsStoreError::MetadataWriteError)?;
        writer
            .flush()
            .map_err(|e| FsStoreError::MetadataWriteError(Box::new(bincode::ErrorKind::Io(e))))?;

        Ok(())
    }

    fn index_path(&self, idx: usize) -> PathBuf {
        let idx_suffix = idx.bitand(0xff);
        self.root
            .as_path()
            .join(format!("data.{idx_suffix:02x}/{idx:02x}"))
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
            return Err(FsStoreError::OutOfBoundsLoadError {
                len: self.metadata.len,
                idx,
            });
        }

        let path = self.index_path(idx);
        let file = fs::OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(FsStoreError::DataOpenError)?;
        let reader = BufReader::new(file);

        bincode::deserialize_from(
            self.io_transformer
                .wrap_reader(reader)
                .map_err(FsStoreError::DataLoadPrepError)?,
        )
        .map_err(FsStoreError::DataLoadError)
    }

    fn store(&mut self, idx: usize, object: impl Borrow<T>) -> Result<(), Self::Error> {
        use io::Write;
        if idx > self.metadata.len {
            return Err(FsStoreError::NonContiguousStoreError {
                len: self.metadata.len,
                idx,
            });
        } else if idx == self.metadata.len {
            self.metadata.len += 1;
            self.write_metadata()?;
        }

        let path = self.index_path(idx);
        fs::create_dir_all(path.parent().unwrap()).map_err(FsStoreError::DataOpenError)?; // unwrap is safe because we know the path has a parent

        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(path)
            .map_err(FsStoreError::DataOpenError)?;

        let mut writer = self
            .io_transformer
            .wrap_writer(file)
            .map_err(FsStoreError::DataStorePrepError)?;
        bincode::serialize_into(&mut writer, object.borrow())
            .map_err(FsStoreError::DataStoreError)?;
        writer
            .flush()
            .map_err(|e| FsStoreError::DataStoreError(Box::new(bincode::ErrorKind::Io(e))))?;

        Ok(())
    }

    fn len(&self) -> Result<usize, Self::Error> {
        Ok(self.metadata.len)
    }

    fn clear(&mut self) -> Result<(), Self::Error> {
        let old_len = self.metadata.len;

        self.metadata.len = 0;
        match self.write_metadata() {
            Ok(_) => (),
            Err(e) => {
                self.metadata.len = old_len;
                return Err(e);
            }
        }

        for idx in 0..old_len {
            let path = self.index_path(idx);
            fs::remove_file(path).map_err(FsStoreError::DataRemoveError)?;
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
                FsStoreError::NonContiguousStoreError { len: 100, idx: 101 }
            ));

            // check everything was stored in memory
            assert_eq!(store.len(), 100);

            for idx in 0..100 {
                let val = store.load(idx).unwrap();
                assert_eq!(val, idx * 2);
            }

            // can't read out of bounds
            assert!(matches!(
                store.load(101).unwrap_err(),
                FsStoreError::OutOfBoundsLoadError { len: 100, idx: 101 }
            ));
        }

        {
            // reopen the store and check that everything is still there
            let mut store = store_builder();

            assert_eq!(store.len(), 100);

            for idx in 0..100 {
                let val = store.load(idx).unwrap();
                assert_eq!(val, idx * 2);
            }

            // still can't store non-contiguous objects
            assert!(matches!(
                store.store(101, 0).unwrap_err(),
                FsStoreError::NonContiguousStoreError { len: 100, idx: 101 }
            ));
            // still can't read out of bounds
            assert!(matches!(
                store.load(101).unwrap_err(),
                FsStoreError::OutOfBoundsLoadError { len: 100, idx: 101 }
            ));
        }
    }
}