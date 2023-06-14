use core::fmt::Debug;

use async_trait::async_trait;

#[derive(Clone, Debug, PartialEq)]
pub struct File {
    pub hashes: Vec<u64>,
    pub size: usize,
}

#[async_trait]
pub trait FileStore: Debug {
    type Key: Debug;
    type Error: Debug;

    async fn get(&self, id: &Self::Key) -> Result<Option<&File>, Self::Error>;

    async fn upsert(&mut self, id: Self::Key, file: File) -> Result<(), Self::Error>;

    async fn remove(&mut self, id: &Self::Key) -> Result<(), Self::Error>;
}
