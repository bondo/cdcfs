use std::{
    collections::{hash_map::Entry, HashMap},
    hash::BuildHasherDefault,
};

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

#[derive(Debug, PartialEq)]
pub enum MemoryChunkStoreError {
    NotFound,
    AlreadyExists,
}

#[async_trait]
impl ChunkStore for MemoryChunkStore {
    type Error = MemoryChunkStoreError;

    async fn get(&self, hash: &u64) -> Result<Vec<u8>, Self::Error> {
        if let Some(chunk) = self.0.get(hash) {
            Ok(chunk.to_owned())
        } else {
            Err(MemoryChunkStoreError::NotFound)
        }
    }

    async fn insert(&mut self, hash: u64, chunk: Vec<u8>) -> Result<(), Self::Error> {
        match self.0.entry(hash) {
            Entry::Vacant(entry) => {
                entry.insert(chunk);
                Ok(())
            }
            Entry::Occupied(_) => Err(MemoryChunkStoreError::AlreadyExists),
        }
    }

    async fn remove(&mut self, hash: &u64) -> Result<(), Self::Error> {
        if self.0.remove(hash).is_some() {
            Ok(())
        } else {
            Err(MemoryChunkStoreError::NotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_can_read_and_write() {
        let source = "Here are some bytes!".as_bytes();
        let mut store = MemoryChunkStore::new();
        assert_eq!(store.insert(10, source.to_owned()).await, Ok(()));

        let result = store.get(&10).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), source);
    }

    #[tokio::test]
    async fn it_cannot_update() {
        let mut store = MemoryChunkStore::new();

        let initial_source = "Initial contents".as_bytes();
        assert_eq!(store.insert(42, initial_source.to_owned()).await, Ok(()));

        let updated_source = "Updated contents".as_bytes();
        assert_eq!(
            store.insert(42, updated_source.to_owned()).await,
            Err(MemoryChunkStoreError::AlreadyExists)
        );

        let result = store.get(&42).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), initial_source);
    }

    #[tokio::test]
    async fn it_cannot_read_missing_item() {
        let store = MemoryChunkStore::new();
        assert_eq!(store.get(&60).await, Err(MemoryChunkStoreError::NotFound));
    }

    #[tokio::test]
    async fn it_cannot_remove_missing_item() {
        let mut store = MemoryChunkStore::new();
        assert_eq!(
            store.remove(&60).await,
            Err(MemoryChunkStoreError::NotFound)
        );
    }
}
