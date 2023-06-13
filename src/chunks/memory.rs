use std::{collections::HashMap, hash::BuildHasherDefault};

use async_trait::async_trait;
use nohash_hasher::NoHashHasher;

use super::traits::ChunkStore;

#[derive(Debug)]
pub struct MemoryChunkStore(HashMap<u64, Vec<u8>, BuildHasherDefault<NoHashHasher<u64>>>);

impl MemoryChunkStore {
    pub fn new() -> Self {
        Self(HashMap::with_hasher(BuildHasherDefault::default()))
    }
}

impl Default for MemoryChunkStore {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum MemoryChunkStoreError {
    NotFound,
}

#[async_trait]
impl ChunkStore for MemoryChunkStore {
    type Error = MemoryChunkStoreError;

    async fn get(&self, hash: &u64) -> Result<&Vec<u8>, Self::Error> {
        if let Some(chunk) = self.0.get(hash) {
            Ok(chunk)
        } else {
            Err(MemoryChunkStoreError::NotFound)
        }
    }

    async fn insert(&mut self, hash: u64, chunk: Vec<u8>) -> Result<(), Self::Error> {
        self.0.insert(hash, chunk);
        Ok(())
    }

    async fn remove(&mut self, hash: &u64) -> Result<(), Self::Error> {
        self.0.remove(hash);
        Ok(())
    }
}
