use core::fmt::Debug;

use async_trait::async_trait;

#[async_trait]
pub trait ChunkStore: Debug {
    type Error: Debug;

    async fn get(&self, hash: &u64) -> Result<Vec<u8>, Self::Error>;

    async fn insert(&mut self, hash: u64, chunk: Vec<u8>) -> Result<(), Self::Error>;

    async fn remove(&mut self, hash: &u64) -> Result<(), Self::Error>;
}
