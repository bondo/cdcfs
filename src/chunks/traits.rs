use core::fmt::Debug;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChunkStoreError {
    #[error("Chunk not found")]
    NotFound,

    #[error("Chunk already exists")]
    AlreadyExists,

    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

pub trait ChunkStore: Debug {
    fn get(&self, hash: &u64) -> Result<Vec<u8>, ChunkStoreError>;

    fn insert(&mut self, hash: u64, chunk: Vec<u8>) -> Result<(), ChunkStoreError>;

    fn remove(&mut self, hash: &u64) -> Result<(), ChunkStoreError>;
}
