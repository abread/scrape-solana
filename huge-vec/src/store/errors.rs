use std::io;

pub use super::chunk_io::IoError;

#[derive(thiserror::Error, Debug)]
pub enum StoreError {
    #[error("Failed reading vector metadata.")]
    MetadataLoad(#[source] IoError),

    #[error("Failed writing vector metadata.")]
    MetadataStore(#[source] IoError),

    #[error("Unsupported version: {0}.")]
    UnsupportedVersion(u64),

    #[error("Failed determining chunk count.")]
    ChunkCount(#[source] io::Error),

    #[error("Failed loading chunk.")]
    ChunkLoad(#[from] ChunkLoadError),

    #[error("Failed storing chunk.")]
    ChunkStore(#[from] ChunkStoreError),

    #[error("Failed dropping chunk.")]
    ChunkDrop(#[source] io::Error),

    #[error("Store reached maximum possible size.")]
    MaxSize,
}

#[derive(thiserror::Error, Debug)]
pub enum ChunkLoadError {
    #[error(transparent)]
    IndexOutOfBounds(#[from] ChunkIndexOutOfBounds),

    #[error("Failed reading chunk from disk.")]
    Io(#[from] IoError),
}

#[derive(thiserror::Error, Debug)]
pub enum ChunkStoreError {
    #[error(transparent)]
    IndexOutOfBounds(#[from] ChunkIndexOutOfBounds),

    #[error("Failed writing chunk to disk.")]
    Io(#[from] IoError),
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Chunk index is out of bounds: idx={idx} must be less than len={len}.")]
pub struct ChunkIndexOutOfBounds {
    pub idx: u64,
    pub len: u64,
}
