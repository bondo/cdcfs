use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Chunk not found")]
    NotFound,

    #[error("Chunk already exists")]
    AlreadyExists,

    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
