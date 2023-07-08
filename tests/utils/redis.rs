use std::{
    future::Future,
    time::{Duration, Instant},
};

use dockertest::{waitfor::RunningWait, Composition, DockerTest, Image};
use redis::Client;
use tokio::time::sleep;

pub fn with_redis_ready<T, Fut>(f: T)
where
    T: FnOnce(String) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let timeout = Duration::from_secs(5);
    let start = Instant::now();

    let mut test = DockerTest::new();

    let image = Image::with_repository("redis").tag("6.0.19-alpine3.18");
    let composition = Composition::with_image(image).with_wait_for(Box::new(RunningWait {
        check_interval: 1,
        max_checks: 10,
    }));
    test.add_composition(composition);

    test.run(|ops| {
        let url = format!("redis://{}", ops.handle("redis").ip());

        let fut = f(url.clone());
        async move {
            tokio::select! {
                _ = wait_for_connection(&url) => (),
                _ = sleep(timeout - start.elapsed()) => panic!("Connection timeout after {:?}", start.elapsed()),
            }

            fut.await;
        }
    });
}

async fn wait_for_connection(url: &str) {
    let Ok( client) = Client::open(url) else {
        panic!("Invalid redis url: {}", url);
    };
    while client.get_connection().is_err() {
        sleep(Duration::from_millis(10)).await;
    }
}
