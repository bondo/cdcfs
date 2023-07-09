use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Chunk not found")]
    NotFound,

    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
