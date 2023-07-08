use cdcfs::{
    meta::{Error, Meta, MetaStore},
    MemoryMetaStore,
};

#[tokio::test]
async fn it_can_read_and_write() {
    let mut store = MemoryMetaStore::new();
    let key = 42;

    assert!(matches!(store.get(&key).await, Err(Error::NotFound)));

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

    assert!(matches!(store.get(&key).await, Err(Error::NotFound)));
    store.remove(&key).await.unwrap();

    let meta = Meta {
        hashes: [10; 20].into(),
        size: 1234,
    };
    store.upsert(key, meta.clone()).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), meta);

    store.remove(&key).await.unwrap();
    assert!(matches!(store.get(&key).await, Err(Error::NotFound)));

    store.remove(&key).await.unwrap();
}
