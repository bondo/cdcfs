use core::fmt::Debug;
use std::{collections::HashMap, convert::Infallible, hash::Hash};

use async_trait::async_trait;

use super::traits::{Meta, MetaStore};

#[derive(Debug)]
pub struct MemoryMetaStore<K: Eq + Hash>(HashMap<K, Meta>);

impl<K: Eq + Hash> MemoryMetaStore<K> {
    pub fn new() -> Self {
        Self(HashMap::new())
    }
}

impl<K: Eq + Hash> Default for MemoryMetaStore<K> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<K: Debug + Eq + Hash + Send + Sync> MetaStore for MemoryMetaStore<K> {
    type Key = K;
    type Error = Infallible;

    async fn get(&self, key: &Self::Key) -> Result<Option<Meta>, Self::Error> {
        Ok(self.0.get(key).map(|v| v.to_owned()))
    }

    async fn upsert(&mut self, key: Self::Key, meta: Meta) -> Result<(), Self::Error> {
        self.0.insert(key, meta);
        Ok(())
    }

    async fn remove(&mut self, key: &Self::Key) -> Result<(), Self::Error> {
        self.0.remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_can_read_and_write() {
        let mut store = MemoryMetaStore::new();
        let key = 42;

        assert_eq!(store.get(&key).await, Ok(None));

        let initial_meta = Meta {
            hashes: b"Here's some stuff for hashes".map(Into::into).to_vec(),
            size: 1234,
        };
        assert_eq!(store.upsert(key, initial_meta.clone()).await, Ok(()));
        assert_eq!(store.get(&key).await, Ok(Some(initial_meta)));

        let updated_meta = Meta {
            hashes: b"Here's some stuff other stuff".map(Into::into).to_vec(),
            size: 4321,
        };
        assert_eq!(store.upsert(key, updated_meta.clone()).await, Ok(()));
        assert_eq!(store.get(&key).await, Ok(Some(updated_meta)));
    }

    #[tokio::test]
    async fn it_can_remove_meta() {
        let mut store = MemoryMetaStore::new();
        let key = "abcdefg";

        assert_eq!(store.get(&key).await, Ok(None));
        assert_eq!(store.remove(&key).await, Ok(()));

        let meta = Meta {
            hashes: [10; 20].into(),
            size: 1234,
        };
        assert_eq!(store.upsert(key, meta.clone()).await, Ok(()));
        assert_eq!(store.get(&key).await, Ok(Some(meta)));

        assert_eq!(store.remove(&key).await, Ok(()));
        assert_eq!(store.get(&key).await, Ok(None));

        assert_eq!(store.remove(&key).await, Ok(()));
    }
}
