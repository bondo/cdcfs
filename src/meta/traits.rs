use core::fmt::Debug;

use async_trait::async_trait;

use super::error::Result;

#[derive(Clone, Debug, PartialEq)]
pub struct Meta {
    pub hashes: Vec<u64>,
    pub size: usize,
}

#[async_trait]
pub trait MetaStore: Debug {
    type Key: Debug;

    async fn get(&self, key: &Self::Key) -> Result<Meta>;

    async fn upsert(&mut self, key: Self::Key, meta: Meta) -> Result<()>;

    async fn remove(&mut self, key: &Self::Key) -> Result<()>;
}
