use redis::{Client, Commands, IntoConnectionInfo, RedisError};

use super::traits::ChunkStore;

#[derive(Debug)]
pub struct RedisChunkStore(Client);

impl RedisChunkStore {
    pub fn new<T: IntoConnectionInfo>(params: T) -> Result<RedisChunkStore, RedisError> {
        Client::open(params).map(Self)
    }
}

impl ChunkStore for RedisChunkStore {
    type Error = RedisError;

    fn get(&self, hash: &u64) -> Result<Vec<u8>, Self::Error> {
        self.0.get_connection()?.get(hash)
    }

    fn insert(&mut self, hash: u64, chunk: Vec<u8>) -> Result<(), Self::Error> {
        self.0.set(hash, chunk)
    }

    fn remove(&mut self, hash: &u64) -> Result<(), Self::Error> {
        self.0.del(hash)
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

            let source = "Here are some bytes!".as_bytes();
            assert_eq!(store.insert(10, source.to_owned()), Ok(()));

            let result = store.get(&10);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), source);
        });
    }

    #[test]
    fn it_returns_empty_data_when_reading_missing_item() {
        with_redis_ready(|url| async move {
            let store = RedisChunkStore::new(url).unwrap();

            assert_eq!(store.get(&60), Ok(vec![]));
        });
    }

    #[test]
    fn it_ignores_remove_of_missing_item() {
        with_redis_ready(|url| async move {
            let mut store = RedisChunkStore::new(url).unwrap();

            assert_eq!(store.remove(&60), Ok(()));
        });
    }
}
