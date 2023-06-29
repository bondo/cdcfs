use core::fmt::Debug;
use std::{collections::HashMap, hash::Hash};

use async_trait::async_trait;

use super::traits::{Meta, MetaStore, MetaStoreError};

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

    async fn get(&self, key: &Self::Key) -> Result<Meta, MetaStoreError> {
        self.0
            .get(key)
            .map(|v| v.to_owned())
            .ok_or(MetaStoreError::NotFound)
    }

    async fn upsert(&mut self, key: Self::Key, meta: Meta) -> Result<(), MetaStoreError> {
        self.0.insert(key, meta);
        Ok(())
    }

    async fn remove(&mut self, key: &Self::Key) -> Result<(), MetaStoreError> {
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

        assert!(matches!(
            store.get(&key).await,
            Err(MetaStoreError::NotFound)
        ));

        let initial_meta = Meta {
            hashes: b"Here's some stuff for hashes".map(Into::into).to_vec(),
            size: 1234,
        };
        store.upsert(key, initial_meta.clone()).await.unwrap();
        assert_eq!(store.get(&key).await.unwrap(), initial_meta);

        let updated_meta = Meta {
            hashes: b"Here's some stuff other stuff".map(Into::into).to_vec(),
            size: 4321,
        };
        store.upsert(key, updated_meta.clone()).await.unwrap();
        assert_eq!(store.get(&key).await.unwrap(), updated_meta);
    }

    #[tokio::test]
    async fn it_can_remove_meta() {
        let mut store = MemoryMetaStore::new();
        let key = "abcdefg";

        assert!(matches!(
            store.get(&key).await,
            Err(MetaStoreError::NotFound)
        ));
        store.remove(&key).await.unwrap();

        let meta = Meta {
            hashes: [10; 20].into(),
            size: 1234,
        };
        store.upsert(key, meta.clone()).await.unwrap();
        assert_eq!(store.get(&key).await.unwrap(), meta);

        store.remove(&key).await.unwrap();
        assert!(matches!(
            store.get(&key).await,
            Err(MetaStoreError::NotFound)
        ));

        store.remove(&key).await.unwrap();
    }
}
