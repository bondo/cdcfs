use anyhow::Context;
use async_trait::async_trait;
use sqlx::{migrate, postgres::PgPoolOptions, query, query_as, PgPool};

use super::{
    error::{Error, Result},
    traits::{Meta, MetaStore},
};

#[derive(Debug)]
pub struct PostgresMetaStore(PgPool);

impl PostgresMetaStore {
    pub async fn new(url: &str) -> Result<PostgresMetaStore> {
        let pool = PgPoolOptions::new()
            .connect(url)
            .await
            .context("Database error")?;
        migrate!("src/meta/postgres/migrations")
            .run(&pool)
            .await
            .context("Database error")?;
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

    async fn get(&self, key: &Self::Key) -> Result<Meta> {
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

        row.map(Into::into).ok_or(Error::NotFound)
    }

    async fn upsert(&mut self, key: Self::Key, meta: Meta) -> Result<()> {
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

    async fn remove(&mut self, key: &Self::Key) -> Result<()> {
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
