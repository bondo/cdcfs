use std::{future::Future, time::Duration};

use async_trait::async_trait;
use bollard::{exec::CreateExecOptions, Docker};
use dockertest::{
    waitfor::{RunningWait, WaitFor},
    Composition, DockerTest, DockerTestError, Image, PendingContainer, RunningContainer,
};
use thiserror::Error;
use tokio::time::sleep;

pub fn with_postgres_ready<T, Fut>(f: T)
where
    T: FnOnce(String) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut test = DockerTest::new();

    let image = Image::with_repository("postgres").tag("15.3-alpine3.18");
    let mut composition = Composition::with_image(image)
        .with_env([("POSTGRES_PASSWORD".to_string(), "postgres".to_string())].into())
        .with_wait_for(Box::new(PostgresReadyWait {
            timeout: Duration::from_millis(5000),
        }));
    composition.publish_all_ports();
    test.add_composition(composition);

    test.run(|ops| {
        let handle = ops.handle("postgres");
        let ip_and_port = handle.host_port(5432);
        assert!(ip_and_port.is_some());

        let (ip, port) = ip_and_port.unwrap();
        let url = format!("postgresql://postgres:postgres@{ip}:{port}/postgres");
        f(url)
    });
}

#[derive(Clone)]
struct PostgresReadyWait {
    timeout: Duration,
}

#[async_trait]
impl WaitFor for PostgresReadyWait {
    async fn wait_for_ready(
        &self,
        container: PendingContainer,
    ) -> Result<RunningContainer, DockerTestError> {
        tokio::select! {
            res = self.wait_for_postgres_ready(container) => res,
            _ = tokio::time::sleep(self.timeout) => Err(DockerTestError::Processing("Timeout".to_string())),
        }
    }
}

#[derive(Debug, Error)]
#[error("Timeout")]
struct TimeoutError;

impl PostgresReadyWait {
    async fn wait_for_postgres_ready(
        &self,
        container: PendingContainer,
    ) -> Result<RunningContainer, DockerTestError> {
        let container = RunningWait {
            check_interval: 1,
            max_checks: 60,
        }
        .wait_for_ready(container)
        .await?;

        let docker = Docker::connect_with_local_defaults().map_err(|e| {
            DockerTestError::Daemon(format!("connection with local defaults: {:?}", e))
        })?;

        loop {
            let check = docker
                .create_exec(
                    container.name(),
                    CreateExecOptions {
                        cmd: Some(vec!["pg_isready"]),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| DockerTestError::Daemon(format!("create exec: {:?}", e)))?;

            docker
                .start_exec(&check.id, None)
                .await
                .map_err(|e| DockerTestError::Daemon(format!("start exec: {:?}", e)))?;

            let exit_code = loop {
                sleep(Duration::from_millis(100)).await;

                let status = docker
                    .inspect_exec(&check.id)
                    .await
                    .map_err(|e| DockerTestError::Daemon(format!("inspect exec: {:?}", e)))?;

                if let Some(exit_code) = status.exit_code {
                    break exit_code;
                };
            };

            if exit_code == 0 {
                // TODO: Figure out why tests fail without this
                sleep(Duration::from_millis(200)).await;

                return Ok(container);
            };
        }
    }
}
