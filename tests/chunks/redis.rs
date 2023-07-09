use cdcfs::{
    chunks::{ChunkStore, Error},
    RedisChunkStore,
};

use crate::utils::with_redis_ready;

#[test]
fn it_can_read_and_write() {
    with_redis_ready(|url| async move {
        let mut store = RedisChunkStore::new(url).unwrap();

        let source = b"Here are some bytes!".to_vec();
        store.upsert(10, source.clone()).unwrap();

        let result = store.get(&10).unwrap();
        assert_eq!(result, source);
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
