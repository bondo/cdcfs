use core::fmt::Debug;
use std::{collections::HashMap, hash::Hash};

use async_trait::async_trait;

use super::{
    error::{Error, Result},
    traits::{Meta, MetaStore},
};

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

    async fn get(&self, key: &Self::Key) -> Result<Meta> {
        self.0.get(key).map(|v| v.to_owned()).ok_or(Error::NotFound)
    }

    async fn upsert(&mut self, key: Self::Key, meta: Meta) -> Result<()> {
        self.0.insert(key, meta);
        Ok(())
    }

    async fn remove(&mut self, key: &Self::Key) -> Result<()> {
        self.0.remove(key);
        Ok(())
    }
}
