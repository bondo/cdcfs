use cdcfs::{
    chunks::{ChunkStore, Error},
    MemoryChunkStore,
};

#[test]
fn it_can_read_and_write() {
    let source = b"Here are some bytes!".to_vec();
    let mut store = MemoryChunkStore::new();
    store.upsert(10, source.clone()).unwrap();

    let result = store.get(&10).unwrap();
    assert_eq!(result, source);
}

#[test]
fn it_cannot_read_missing_item() {
    let store = MemoryChunkStore::new();
    assert!(matches!(store.get(&60), Err(Error::NotFound)));
}

#[test]
fn it_cannot_remove_missing_item() {
    let mut store = MemoryChunkStore::new();
    assert!(matches!(store.remove(&60), Err(Error::NotFound)));
}
