use core::fmt::Debug;

use async_trait::async_trait;

#[derive(Clone, Debug, PartialEq)]
pub struct Meta {
    pub hashes: Vec<u64>,
    pub size: usize,
}

#[async_trait]
pub trait MetaStore: Debug {
    type Key: Debug;
    type Error: Debug;

    async fn get(&self, key: &Self::Key) -> Result<Option<Meta>, Self::Error>;

    async fn upsert(&mut self, key: Self::Key, meta: Meta) -> Result<(), Self::Error>;

    async fn remove(&mut self, key: &Self::Key) -> Result<(), Self::Error>;
}
