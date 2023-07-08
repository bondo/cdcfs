use with_postgres_ready::with_postgres_ready;

use cdcfs::{
    meta::{Error, Meta, MetaStore},
    PostgresMetaStore,
};

#[test]
fn it_can_read_and_write() {
    with_postgres_ready(|url| async move {
        let mut store = PostgresMetaStore::new(&url).await.unwrap();

        let key = 42;

        let value = store.get(&key).await;
        assert!(matches!(value, Err(Error::NotFound)));

        let initial_meta = Meta {
            hashes: b"Here's some stuff for hashes".map(Into::into).to_vec(),
            size: 1234,
        };
        store.upsert(key, initial_meta.clone()).await.unwrap();
        let value = store.get(&key).await.unwrap();
        assert_eq!(value, initial_meta);

        let updated_meta = Meta {
            hashes: b"Here's some stuff other stuff".map(Into::into).to_vec(),
            size: 4321,
        };
        store.upsert(key, updated_meta.clone()).await.unwrap();
        let value = store.get(&key).await.unwrap();
        assert_eq!(value, updated_meta);
    });
}

#[test]
fn it_can_remove_meta() {
    with_postgres_ready(|url| async move {
        let mut store = PostgresMetaStore::new(&url).await.unwrap();

        let key = 19;

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
    });
}
