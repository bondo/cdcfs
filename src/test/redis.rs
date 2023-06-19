use std::future::Future;

use dockertest::{waitfor::RunningWait, Composition, DockerTest, Image};

pub fn with_redis_ready<T, Fut>(f: T)
where
    T: FnOnce(String) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut test = DockerTest::new();

    let image = Image::with_repository("redis").tag("6.0.19-alpine3.18");
    let composition = Composition::with_image(image).with_wait_for(Box::new(RunningWait {
        check_interval: 1,
        max_checks: 10,
    }));
    test.add_composition(composition);

    test.run(|ops| f(format!("redis://{}", ops.handle("redis").ip())));
}
