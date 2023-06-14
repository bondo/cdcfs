use core::fmt::Debug;
use std::{collections::HashMap, convert::Infallible, hash::Hash};

use async_trait::async_trait;

use super::traits::{File, FileStore};

#[derive(Debug)]
pub struct MemoryFileStore<K: Eq + Hash>(HashMap<K, File>);

impl<K: Eq + Hash> MemoryFileStore<K> {
    pub fn new() -> Self {
        Self(HashMap::new())
    }
}

impl<K: Eq + Hash> Default for MemoryFileStore<K> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<K: Debug + Eq + Hash + Send + Sync> FileStore for MemoryFileStore<K> {
    type Key = K;
    type Error = Infallible;

    async fn get(&self, id: &Self::Key) -> Result<Option<&File>, Self::Error> {
        Ok(self.0.get(id))
    }

    async fn upsert(&mut self, id: Self::Key, file: File) -> Result<(), Self::Error> {
        self.0.insert(id, file);
        Ok(())
    }

    async fn remove(&mut self, id: &Self::Key) -> Result<(), Self::Error> {
        self.0.remove(id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_can_read_and_write() {
        let mut store = MemoryFileStore::new();
        let id = 42;

        assert_eq!(store.get(&id).await, Ok(None));

        let initial_file = File {
            hashes: "Here's some stuff for hashes"
                .as_bytes()
                .iter()
                .map(|v| *v as u64)
                .collect(),
            size: 1234,
        };
        assert_eq!(store.upsert(id, initial_file.clone()).await, Ok(()));
        assert_eq!(store.get(&id).await, Ok(Some(&initial_file)));

        let updated_file = File {
            hashes: "Here's some stuff other stuff"
                .as_bytes()
                .iter()
                .map(|v| *v as u64)
                .collect(),
            size: 4321,
        };
        assert_eq!(store.upsert(id, updated_file.clone()).await, Ok(()));
        assert_eq!(store.get(&id).await, Ok(Some(&updated_file)));
    }

    #[tokio::test]
    async fn it_can_remove_file() {
        let mut store = MemoryFileStore::new();
        let id = "abcdefg";

        assert_eq!(store.get(&id).await, Ok(None));
        assert_eq!(store.remove(&id).await, Ok(()));

        let file = File {
            hashes: [10; 20].into(),
            size: 1234,
        };
        assert_eq!(store.upsert(id, file.clone()).await, Ok(()));
        assert_eq!(store.get(&id).await, Ok(Some(&file)));

        assert_eq!(store.remove(&id).await, Ok(()));
        assert_eq!(store.get(&id).await, Ok(None));

        assert_eq!(store.remove(&id).await, Ok(()));
    }
}
