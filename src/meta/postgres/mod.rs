use anyhow::Context;
use async_trait::async_trait;
use sqlx::{migrate, postgres::PgPoolOptions, query, query_as, PgPool};

use super::traits::{Meta, MetaStore, MetaStoreError};

#[derive(Debug)]
pub struct PostgresMetaStore(PgPool);

impl PostgresMetaStore {
    pub async fn new(url: &str) -> Result<PostgresMetaStore, sqlx::Error> {
        let pool = PgPoolOptions::new().connect(url).await?;
        migrate!("src/meta/postgres/migrations").run(&pool).await?;
        Ok(Self(pool))
    }
}

struct DbValue {
    hashes: Vec<i64>,
    size: i64,
}

impl From<DbValue> for Meta {
    fn from(value: DbValue) -> Self {
        Self {
            hashes: value.hashes.into_iter().map(|v| v as u64).collect(),
            size: value.size as usize,
        }
    }
}

impl From<Meta> for DbValue {
    fn from(value: Meta) -> Self {
        Self {
            hashes: value.hashes.into_iter().map(|v| v as i64).collect(),
            size: value.size as i64,
        }
    }
}

#[async_trait]
impl MetaStore for PostgresMetaStore {
    type Key = i32;

    async fn get(&self, key: &Self::Key) -> Result<Meta, MetaStoreError> {
        let row = query_as!(
            DbValue,
            r#"
                SELECT
                    hashes,
                    size
                FROM
                    files f
                WHERE
                    f.id = $1
            "#,
            key
        )
        .fetch_optional(&self.0)
        .await
        .context("Database error")?;

        row.map(Into::into).ok_or(MetaStoreError::NotFound)
    }

    async fn upsert(&mut self, key: Self::Key, meta: Meta) -> Result<(), MetaStoreError> {
        let meta: DbValue = meta.into();

        query!(
            r#"
                INSERT INTO files (
                    id,
                    hashes,
                    size
                )
                VALUES (
                    $1,
                    $2,
                    $3
                )
                ON CONFLICT (id) DO UPDATE SET
                    hashes = EXCLUDED.hashes,
                    size = EXCLUDED.size
            "#,
            key,
            &meta.hashes,
            meta.size
        )
        .execute(&self.0)
        .await
        .context("Database error")?;

        Ok(())
    }

    async fn remove(&mut self, key: &Self::Key) -> Result<(), MetaStoreError> {
        query!(
            r#"
                DELETE FROM
                    files f
                WHERE
                    f.id = $1
            "#,
            key,
        )
        .execute(&self.0)
        .await
        .context("Database error")?;

        Ok(())
    }
}

// To run tests, first run `docker pull postgres:15.3-alpine3.18` locally

#[cfg(test)]
mod tests {
    use test_log::test;

    use crate::test::postgres::with_postgres_ready;

    use super::*;

    #[test]
    fn it_can_read_and_write() {
        with_postgres_ready(|url| async move {
            let mut store = PostgresMetaStore::new(&url).await.unwrap();

            let key = 42;

            let value = store.get(&key).await;
            assert!(matches!(value, Err(MetaStoreError::NotFound)));

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
        });
    }
}
