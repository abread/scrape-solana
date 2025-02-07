use std::{fs::File, io, path::Path};

use serde::{de::DeserializeOwned, Serialize};

#[derive(thiserror::Error, Debug)]
pub enum IoError {
    #[error("Failed reading/writing object from disk.")]
    Io(#[from] io::Error),

    #[error("Object (de)serialization error.")]
    Serialization(#[from] bincode::Error),
}

pub(crate) fn store<T: Serialize, P: AsRef<Path>>(value: &T, path: P) -> Result<(), IoError> {
    let mut f = tempfile::NamedTempFile::new_in(path.as_ref().parent().unwrap_or(Path::new(".")))?;
    wrap_write_compress(&mut f, |f| bincode::serialize_into(f, value))?;
    f.persist(path)?;
    Ok(())
}

pub(crate) fn load<T: DeserializeOwned, P: AsRef<Path>>(path: P) -> Result<T, IoError> {
    let file = File::open(path.as_ref())?;
    let reader = io::BufReader::new(file);
    let reader = wrap_reader_decompress(reader);
    bincode::deserialize_from(reader).map_err(|e| e.into())
}

const ZSTD_COMPRESSION_LEVEL: i32 = 15;
#[inline(always)]
fn wrap_write_compress<T, E, W>(
    writer: W,
    write_fn: impl FnOnce(&mut dyn io::Write) -> Result<T, E>,
) -> Result<T, E>
where
    E: From<io::Error>,
    W: io::Write,
{
    let mut writer = zstd::stream::write::Encoder::new(writer, ZSTD_COMPRESSION_LEVEL).unwrap();
    let result = write_fn(&mut writer)?;
    writer.finish()?.flush()?;
    Ok(result)
}

#[inline(always)]
fn wrap_reader_decompress<R: io::Read>(reader: R) -> impl io::Read {
    zstd::stream::read::Decoder::new(reader).unwrap()
}

impl From<tempfile::PersistError> for IoError {
    fn from(e: tempfile::PersistError) -> Self {
        e.error.into()
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn load_store_ok() {
        let testdir = tempfile::tempdir().unwrap();
        let path = testdir.path().join("test");
        let value = 42u64;
        super::store(&value, &path).unwrap();
        let loaded = super::load(&path).unwrap();
        assert_eq!(value, loaded);
    }
}
