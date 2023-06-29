use anyhow::Context;
use redis::{Client, Commands, IntoConnectionInfo};

use super::{error::Result, traits::ChunkStore};

#[derive(Debug)]
pub struct RedisChunkStore(Client);

impl RedisChunkStore {
    pub fn new<T: IntoConnectionInfo>(params: T) -> Result<RedisChunkStore> {
        let client = Client::open(params).context("Redis error")?;
        Ok(Self(client))
    }
}

impl ChunkStore for RedisChunkStore {
    fn get(&self, hash: &u64) -> Result<Vec<u8>> {
        let val = self
            .0
            .get_connection()
            .context("Redis error")?
            .get(hash)
            .context("Redis error")?;
        Ok(val)
    }

    fn insert(&mut self, hash: u64, chunk: Vec<u8>) -> Result<()> {
        self.0.set(hash, chunk).context("Redis error")?;
        Ok(())
    }

    fn remove(&mut self, hash: &u64) -> Result<()> {
        self.0.del(hash).context("Redis error")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use test_log::test;

    use crate::test::redis::with_redis_ready;

    use super::*;

    #[test]
    fn it_can_read_and_write() {
        with_redis_ready(|url| async move {
            let mut store = RedisChunkStore::new(url).unwrap();

            let source = b"Here are some bytes!".to_vec();
            store.insert(10, source.clone()).unwrap();

            let result = store.get(&10).unwrap();
            assert_eq!(result, source);
        });
    }

    #[test]
    fn it_returns_empty_data_when_reading_missing_item() {
        with_redis_ready(|url| async move {
            let store = RedisChunkStore::new(url).unwrap();

            assert_eq!(store.get(&60).unwrap(), Vec::<u8>::new());
        });
    }

    #[test]
    fn it_ignores_remove_of_missing_item() {
        with_redis_ready(|url| async move {
            let mut store = RedisChunkStore::new(url).unwrap();

            store.remove(&60).unwrap();
        });
    }
}
