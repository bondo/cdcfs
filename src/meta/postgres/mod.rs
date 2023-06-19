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
    use std::{future::Future, net::Ipv4Addr, time::Duration};

    use bollard::{exec::CreateExecOptions, Docker};
    use dockertest::{waitfor::RunningWait, Composition, DockerTest, Image};
    use test_log::test;
    use tokio::time::{sleep, Instant};

    use super::*;

    fn with_postgres_running<T, Fut>(f: T)
    where
        T: FnOnce((Ipv4Addr, u32), String) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut test = DockerTest::new();

        let image = Image::with_repository("postgres").tag("15.3-alpine3.18");
        let mut composition = Composition::with_image(image)
            .with_env([("POSTGRES_PASSWORD".to_string(), "postgres".to_string())].into())
            .with_wait_for(Box::new(RunningWait {
                check_interval: 1,
                max_checks: 10,
            }));
        composition.publish_all_ports();
        test.add_composition(composition);

        test.run(|ops| {
            let handle = ops.handle("postgres");
            let ip_and_port = handle.host_port(5432);
            assert!(ip_and_port.is_some());

            f(ip_and_port.unwrap().to_owned(), handle.name().to_owned())
        });
    }

    async fn wait_for_postgres_ready(
        container_name: &str,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let start = Instant::now();
        let docker = Docker::connect_with_local_defaults()?;

        loop {
            let check = docker
                .create_exec(
                    container_name,
                    CreateExecOptions {
                        cmd: Some(vec!["pg_isready"]),
                        ..Default::default()
                    },
                )
                .await?;
            docker.start_exec(&check.id, None).await?;
            loop {
                let status = docker.inspect_exec(&check.id).await?;
                let Some(true) = status.running else {
                    break;
                };
                assert!(start.elapsed() < timeout);
                sleep(Duration::from_millis(10)).await;
            }

            let status = docker.inspect_exec(&check.id).await?;
            if let Some(0) = status.exit_code {
                break;
            };
            assert!(start.elapsed() < timeout);
            sleep(Duration::from_millis(100)).await;
        }

        Ok(())
    }

    #[test]
    fn it_can_read_and_write() {
        with_postgres_running(|(ip, port), container_name| async move {
            assert!(
                wait_for_postgres_ready(&container_name, Duration::from_millis(2000))
                    .await
                    .is_ok()
            );

            let mut store = PostgresMetaStore::new(
                format!("postgresql://postgres:postgres@{ip}:{port}/postgres").as_str(),
            )
            .await
            .expect("Can connect to database");

            let key = 42;

            let value = store.get(&key).await;
            assert_eq!(value, Ok(None));

            let initial_meta = Meta {
                hashes: "Here's some stuff for hashes"
                    .as_bytes()
                    .iter()
                    .map(|v| *v as u64)
                    .collect(),
                size: 1234,
            };
            assert_eq!(store.upsert(key, initial_meta.clone()).await, Ok(()));
            let value = store.get(&key).await;
            assert_eq!(value, Ok(Some(initial_meta)));

            let updated_meta = Meta {
                hashes: "Here's some stuff other stuff"
                    .as_bytes()
                    .iter()
                    .map(|v| *v as u64)
                    .collect(),
                size: 4321,
            };
            assert_eq!(store.upsert(key, updated_meta.clone()).await, Ok(()));
            let value = store.get(&key).await;
            assert_eq!(value, Ok(Some(updated_meta)));
        });
    }

    #[test]
    fn it_can_remove_meta() {
        with_postgres_running(|(ip, port), container_name| async move {
            assert!(
                wait_for_postgres_ready(&container_name, Duration::from_millis(2000))
                    .await
                    .is_ok()
            );

            let mut store = PostgresMetaStore::new(
                format!("postgresql://postgres:postgres@{ip}:{port}/postgres").as_str(),
            )
            .await
            .expect("Can connect to database");

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
