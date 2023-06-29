use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Chunk store error: {0}")]
    ChunkStore(#[from] crate::chunks::error::Error),
    #[error("Meta store error: {0}")]
    MetaStore(#[from] crate::meta::error::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Chunking error: {0}")]
    Chunking(#[from] fastcdc::v2020::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
