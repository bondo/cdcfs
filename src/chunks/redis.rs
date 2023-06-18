use async_trait::async_trait;
use redis::{Client, Commands, IntoConnectionInfo, RedisError};

use super::traits::ChunkStore;

#[derive(Debug)]
pub struct RedisChunkStore(Client);

impl RedisChunkStore {
    pub fn new<T: IntoConnectionInfo>(params: T) -> Result<RedisChunkStore, RedisError> {
        Client::open(params).map(Self)
    }
}

#[async_trait]
impl ChunkStore for RedisChunkStore {
    type Error = RedisError;

    async fn get(&self, hash: &u64) -> Result<Vec<u8>, Self::Error> {
        self.0.get_connection()?.get(hash)
    }

    async fn insert(&mut self, hash: u64, chunk: Vec<u8>) -> Result<(), Self::Error> {
        self.0.set(hash, chunk)
    }

    async fn remove(&mut self, hash: &u64) -> Result<(), Self::Error> {
        self.0.del(hash)
    }
}

// To run tests, first run `docker pull redis:6.0.19-alpine3.18` locally

#[cfg(test)]
mod tests {
    use std::{future::Future, net::Ipv4Addr};

    use dockertest::{waitfor::RunningWait, Composition, DockerTest, Image};
    use test_log::test;

    use super::*;

    fn with_redis_running<T, Fut>(f: T)
    where
        T: FnOnce(Ipv4Addr) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut test = DockerTest::new();

        let image = Image::with_repository("redis").tag("6.0.19-alpine3.18");
        let composition = Composition::with_image(image).with_wait_for(Box::new(RunningWait {
            check_interval: 1,
            max_checks: 10,
        }));
        test.add_composition(composition);

        test.run(|ops| f(ops.handle("redis").ip().to_owned()));
    }

    #[test]
    fn it_can_read_and_write() {
        with_redis_running(|ip| async move {
            let source = "Here are some bytes!".as_bytes();
            let mut store =
                RedisChunkStore::new(format!("redis://{}", ip)).expect("Can create store");
            assert_eq!(store.insert(10, source.to_owned()).await, Ok(()));

            let result = store.get(&10).await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), source);
        });
    }

    #[test]
    fn it_return_empty_data_when_read_missing_item() {
        with_redis_running(|ip| async move {
            let store = RedisChunkStore::new(format!("redis://{}", ip)).expect("Can create store");
            assert_eq!(store.get(&60).await, Ok(vec![]));
        });
    }

    #[test]
    fn it_ignores_remove_of_missing_item() {
        with_redis_running(|ip| async move {
            let mut store =
                RedisChunkStore::new(format!("redis://{}", ip)).expect("Can create store");
            assert_eq!(store.remove(&60).await, Ok(()));
        });
    }
}
