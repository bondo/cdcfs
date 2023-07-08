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
