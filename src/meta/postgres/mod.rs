use std::num::TryFromIntError;

use async_trait::async_trait;
use sqlx::{migrate, postgres::PgPoolOptions, query, query_as, PgPool};

use super::traits::{Meta, MetaStore};

#[derive(Debug)]
pub struct PostgresMetaStore(PgPool);

impl PostgresMetaStore {
    pub async fn new(url: &str) -> Result<PostgresMetaStore, sqlx::Error> {
        let pool = PgPoolOptions::new().connect(url).await?;
        migrate!("src/meta/postgres/migrations").run(&pool).await?;
        Ok(Self(pool))
    }
}

#[derive(Debug, PartialEq)]
pub enum PostgresMetaStoreError {
    Sql(String),
    Convert(TryFromIntError),
}

struct DbValue {
    hashes: Vec<i64>,
    size: i64,
}

impl TryFrom<DbValue> for Meta {
    type Error = PostgresMetaStoreError;

    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        Ok(Self {
            hashes: value.hashes.iter().map(|&v| v as u64).collect(),
            size: value
                .size
                .try_into()
                .map_err(PostgresMetaStoreError::Convert)?,
        })
    }
}

impl TryFrom<Meta> for DbValue {
    type Error = PostgresMetaStoreError;

    fn try_from(value: Meta) -> Result<Self, Self::Error> {
        Ok(Self {
            hashes: value.hashes.iter().map(|&v| v as i64).collect(),
            size: value
                .size
                .try_into()
                .map_err(PostgresMetaStoreError::Convert)?,
        })
    }
}

#[async_trait]
impl MetaStore for PostgresMetaStore {
    type Key = i32;
    type Error = PostgresMetaStoreError;

    async fn get(&self, key: &Self::Key) -> Result<Option<Meta>, Self::Error> {
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
        .map_err(|e| PostgresMetaStoreError::Sql(format!("{:?}", e)))?;

        Ok(row.map(TryInto::try_into).transpose()?)
    }

    async fn upsert(&mut self, key: Self::Key, meta: Meta) -> Result<(), Self::Error> {
        let meta: DbValue = meta.try_into()?;

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
        .map_err(|e| PostgresMetaStoreError::Sql(format!("{:?}", e)))?;

        Ok(())
    }

    async fn remove(&mut self, key: &Self::Key) -> Result<(), Self::Error> {
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
        .map_err(|e| PostgresMetaStoreError::Sql(format!("{:?}", e)))?;

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
            assert_eq!(value, Ok(None));

            let initial_meta = Meta {
                hashes: b"Here's some stuff for hashes".map(Into::into).to_vec(),
                size: 1234,
            };
            assert_eq!(store.upsert(key, initial_meta.clone()).await, Ok(()));
            let value = store.get(&key).await;
            assert_eq!(value, Ok(Some(initial_meta)));

            let updated_meta = Meta {
                hashes: b"Here's some stuff other stuff".map(Into::into).to_vec(),
                size: 4321,
            };
            assert_eq!(store.upsert(key, updated_meta.clone()).await, Ok(()));
            let value = store.get(&key).await;
            assert_eq!(value, Ok(Some(updated_meta)));
        });
    }

    #[test]
    fn it_can_remove_meta() {
        with_postgres_ready(|url| async move {
            let mut store = PostgresMetaStore::new(&url).await.unwrap();

            let key = 19;

            assert_eq!(store.get(&key).await, Ok(None));
            assert_eq!(store.remove(&key).await, Ok(()));

            let meta = Meta {
                hashes: [10; 20].into(),
                size: 1234,
            };
            assert_eq!(store.upsert(key, meta.clone()).await, Ok(()));
            assert_eq!(store.get(&key).await, Ok(Some(meta)));

            assert_eq!(store.remove(&key).await, Ok(()));
            assert_eq!(store.get(&key).await, Ok(None));

            assert_eq!(store.remove(&key).await, Ok(()));
        });
    }
}
