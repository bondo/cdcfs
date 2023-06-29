use std::{
    collections::{hash_map::Entry, HashMap},
    hash::BuildHasherDefault,
};

use nohash_hasher::NoHashHasher;

use super::{
    error::{Error, Result},
    traits::ChunkStore,
};

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

impl ChunkStore for MemoryChunkStore {
    fn get(&self, hash: &u64) -> Result<Vec<u8>> {
        if let Some(chunk) = self.0.get(hash) {
            Ok(chunk.to_owned())
        } else {
            Err(Error::NotFound)
        }
    }

    fn insert(&mut self, hash: u64, chunk: Vec<u8>) -> Result<()> {
        match self.0.entry(hash) {
            Entry::Vacant(entry) => {
                entry.insert(chunk);
                Ok(())
            }
            Entry::Occupied(_) => Err(Error::AlreadyExists),
        }
    }

    fn remove(&mut self, hash: &u64) -> Result<()> {
        if self.0.remove(hash).is_some() {
            Ok(())
        } else {
            Err(Error::NotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_can_read_and_write() {
        let source = b"Here are some bytes!".to_vec();
        let mut store = MemoryChunkStore::new();
        store.insert(10, source.clone()).unwrap();

        let result = store.get(&10).unwrap();
        assert_eq!(result, source);
    }

    #[test]
    fn it_cannot_update() {
        let mut store = MemoryChunkStore::new();

        let initial_source = b"Initial contents".to_vec();
        store.insert(42, initial_source.clone()).unwrap();

        let updated_source = b"Updated contents".to_vec();
        assert!(matches!(
            store.insert(42, updated_source),
            Err(Error::AlreadyExists)
        ));

        let result = store.get(&42).unwrap();
        assert_eq!(result, initial_source);
    }

    #[test]
    fn it_cannot_read_missing_item() {
        let store = MemoryChunkStore::new();
        assert!(matches!(store.get(&60), Err(Error::NotFound)));
    }

    #[test]
    fn it_cannot_remove_missing_item() {
        let mut store = MemoryChunkStore::new();
        assert!(matches!(store.remove(&60), Err(Error::NotFound)));
    }
}
