use core::fmt::Debug;

use async_trait::async_trait;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct Meta {
    pub hashes: Vec<u64>,
    pub size: usize,
}

#[derive(Debug, Error)]
pub enum MetaStoreError {
    #[error("File not found")]
    NotFound,

    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

#[async_trait]
pub trait MetaStore: Debug {
    type Key: Debug;

    async fn get(&self, key: &Self::Key) -> Result<Meta, MetaStoreError>;

    async fn upsert(&mut self, key: Self::Key, meta: Meta) -> Result<(), MetaStoreError>;

    async fn remove(&mut self, key: &Self::Key) -> Result<(), MetaStoreError>;
}
