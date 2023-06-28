use std::{
    collections::{hash_map::Entry, HashMap},
    hash::BuildHasherDefault,
};

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

impl std::fmt::Display for MemoryChunkStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "Chunk not found"),
            Self::AlreadyExists => write!(f, "Chunk already exists"),
        }
    }
}

impl ChunkStore for MemoryChunkStore {
    type Error = MemoryChunkStoreError;

    fn get(&self, hash: &u64) -> Result<Vec<u8>, Self::Error> {
        if let Some(chunk) = self.0.get(hash) {
            Ok(chunk.to_owned())
        } else {
            Err(MemoryChunkStoreError::NotFound)
        }
    }

    fn insert(&mut self, hash: u64, chunk: Vec<u8>) -> Result<(), Self::Error> {
        match self.0.entry(hash) {
            Entry::Vacant(entry) => {
                entry.insert(chunk);
                Ok(())
            }
            Entry::Occupied(_) => Err(MemoryChunkStoreError::AlreadyExists),
        }
    }

    fn remove(&mut self, hash: &u64) -> Result<(), Self::Error> {
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

    #[test]
    fn it_can_read_and_write() {
        let source: &[u8] = b"Here are some bytes!";
        let mut store = MemoryChunkStore::new();
        assert_eq!(store.insert(10, source.to_owned()), Ok(()));

        let result = store.get(&10);
        assert_eq!(result, Ok(source.to_owned()));
    }

    #[test]
    fn it_cannot_update() {
        let mut store = MemoryChunkStore::new();

        let initial_source: &[u8] = b"Initial contents";
        assert_eq!(store.insert(42, initial_source.to_owned()), Ok(()));

        let updated_source: &[u8] = b"Updated contents";
        assert_eq!(
            store.insert(42, updated_source.to_owned()),
            Err(MemoryChunkStoreError::AlreadyExists)
        );

        let result = store.get(&42);
        assert_eq!(result, Ok(initial_source.to_owned()));
    }

    #[test]
    fn it_cannot_read_missing_item() {
        let store = MemoryChunkStore::new();
        assert_eq!(store.get(&60), Err(MemoryChunkStoreError::NotFound));
    }

    #[test]
    fn it_cannot_remove_missing_item() {
        let mut store = MemoryChunkStore::new();
        assert_eq!(store.remove(&60), Err(MemoryChunkStoreError::NotFound));
    }
}
