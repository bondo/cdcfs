use anyhow::Context;
use redis::{Client, Commands, IntoConnectionInfo};

use super::{error::Result, traits::ChunkStore, Error};

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
        let mut conn = self.0.get_connection().context("Redis error")?;
        if !conn.exists(hash).context("Redis error")? {
            return Err(Error::NotFound);
        }
        let val: Vec<u8> = conn.get(hash).context("Redis error")?;
        Ok(val)
    }

    fn insert(&mut self, hash: u64, chunk: Vec<u8>) -> Result<()> {
        let mut conn = self.0.get_connection().context("Redis error")?;
        if conn.exists(hash).context("Redis error")? {
            return Err(Error::AlreadyExists);
        }
        conn.set(hash, chunk).context("Redis error")?;
        Ok(())
    }

    fn remove(&mut self, hash: &u64) -> Result<()> {
        let mut conn = self.0.get_connection().context("Redis error")?;
        if !conn.exists(hash).context("Redis error")? {
            return Err(Error::NotFound);
        }
        conn.del(hash).context("Redis error")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use test_log::test;

    use crate::tests::with_redis_ready;

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
    fn it_cannot_update() {
        with_redis_ready(|url| async move {
            let mut store = RedisChunkStore::new(url).unwrap();

            let initial_source = b"Initial contents".to_vec();
            store.insert(42, initial_source.clone()).unwrap();

            let updated_source = b"Updated contents".to_vec();
            assert!(matches!(
                store.insert(42, updated_source),
                Err(Error::AlreadyExists)
            ));

            let result = store.get(&42).unwrap();
            assert_eq!(result, initial_source);

            store.remove(&42).unwrap();
            assert!(matches!(store.get(&42), Err(Error::NotFound)));
        });
    }

    #[test]
    fn it_cannot_read_missing_item() {
        with_redis_ready(|url| async move {
            let store = RedisChunkStore::new(url).unwrap();
            assert!(matches!(store.get(&60), Err(Error::NotFound)));
        });
    }

    #[test]
    fn it_cannot_remove_missing_item() {
        with_redis_ready(|url| async move {
            let mut store = RedisChunkStore::new(url).unwrap();
            assert!(matches!(store.remove(&60), Err(Error::NotFound)));
        });
    }
}
