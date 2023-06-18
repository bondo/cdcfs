use std::{fmt::Debug, hash::Hash};

use fastcdc::v2020::FastCDC;
use wyhash::wyhash;

use crate::{
    chunks::traits::ChunkStore,
    meta::traits::{Meta, MetaStore},
};

#[derive(Debug)]
pub struct System<C: ChunkStore, M: MetaStore> {
    chunk_store: C,
    meta_store: M,
}

static AVG_SIZE: u32 = u32::pow(2, 14);
static MIN_SIZE: u32 = AVG_SIZE / 4;
static MAX_SIZE: u32 = AVG_SIZE * 4;

impl<K, C, M> System<C, M>
where
    K: Debug + Eq + Hash,
    M: MetaStore<Key = K>,
    C: ChunkStore,
{
    pub fn new(chunk_store: C, meta_store: M) -> Self {
        Self {
            chunk_store,
            meta_store,
        }
    }

    pub async fn upsert(&mut self, key: K, source: &[u8]) {
        let chunker = FastCDC::new(source, MIN_SIZE, AVG_SIZE, MAX_SIZE);
        let mut hashes = vec![];
        for chunk in chunker {
            let bytes = &source[chunk.offset..chunk.offset + chunk.length];
            let hash = wyhash(bytes, 42);
            self.chunk_store
                .insert(hash, bytes.to_owned())
                .expect("Should be able to insert chunk");
            hashes.push(hash);
        }
        self.meta_store
            .upsert(
                key,
                Meta {
                    hashes,
                    size: source.len(),
                },
            )
            .await
            .expect("Should be able to upsert meta");
    }

    pub async fn read(&self, key: K) -> Option<Vec<u8>> {
        let meta = self
            .meta_store
            .get(&key)
            .await
            .expect("Should be able to request meta read")?;
        let mut result = Vec::with_capacity(meta.size);
        for hash in meta.hashes.iter() {
            let chunk = self
                .chunk_store
                .get(hash)
                .expect("Hash should exist in map");
            result.extend_from_slice(&chunk);
        }
        Some(result)
    }

    pub async fn delete(&mut self, key: K) {
        self.meta_store
            .remove(&key)
            .await
            .expect("Should be able to remove meta");
    }
}

// To run tests, first run `docker pull redis:6.0.19-alpine3.18` locally

#[cfg(test)]
mod tests {
    use std::{fs, future::Future, net::Ipv4Addr};

    use dockertest::{waitfor::RunningWait, Composition, DockerTest, Image};

    use crate::{
        chunks::{memory::MemoryChunkStore, redis::RedisChunkStore},
        meta::memory::MemoryMetaStore,
    };

    use super::*;

    #[tokio::test]
    async fn it_can_read_and_write() {
        let source = "Hello World!".repeat(10_000);
        let mut fs = System::new(MemoryChunkStore::new(), MemoryMetaStore::new());
        fs.upsert(42, source.as_bytes()).await;
        assert_eq!(fs.read(42).await.map(String::from_utf8), Some(Ok(source)));
    }

    #[tokio::test]
    async fn it_can_update() {
        let mut fs = System::new(MemoryChunkStore::new(), MemoryMetaStore::new());

        let initial_source = "Initial contents";
        fs.upsert(42, initial_source.as_bytes()).await;

        let updated_source = "Updated contents";
        fs.upsert(42, updated_source.as_bytes()).await;

        assert_eq!(
            fs.read(42).await.map(String::from_utf8),
            Some(Ok(updated_source.to_string()))
        );
    }

    #[tokio::test]
    async fn can_restore_samples() {
        let mut fs = System::new(MemoryChunkStore::new(), MemoryMetaStore::new());

        let samples = vec![
            "file_example_JPG_2500kB.jpg",
            "file_example_OOG_5MG.ogg",
            "file-example_PDF_1MB.pdf",
            "file-sample_1MB.docx",
        ];

        let meta: Vec<(&str, Vec<u8>)> = samples
            .iter()
            .map(|sample| {
                let file = fs::read(format!("test/fixtures/{sample}"));
                assert!(file.is_ok());
                (*sample, file.unwrap())
            })
            .collect();

        for (name, file) in meta.iter() {
            fs.upsert(*name, file.as_slice()).await;
        }

        for (name, file) in meta.iter() {
            let result = fs.read(*name).await;
            assert!(result.is_some());
            let result = result.unwrap();
            assert_eq!(&result, file);
        }
    }

    fn with_redis_running<T, Fut>(f: T)
    where
        T: FnOnce(Ipv4Addr) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut test = DockerTest::new();

        let image = Image::with_repository("redis").tag("6.0.19-alpine3.18");
        let composition = Composition::with_image(image).with_wait_for(Box::new(RunningWait {
            check_interval: 1,
            max_checks: 10,
        }));
        test.add_composition(composition);

        test.run(|ops| f(ops.handle("redis").ip().to_owned()));
    }

    fn get_redis_chunk_store(ip: &Ipv4Addr) -> RedisChunkStore {
        RedisChunkStore::new(format!("redis://{}", ip))
            .expect("Should be able to create redis chunk store")
    }

    #[test_log::test]
    fn it_can_read_and_write_with_redis() {
        with_redis_running(|ip| async move {
            let mut fs = System::new(get_redis_chunk_store(&ip), MemoryMetaStore::new());

            let source = "Hello World!".repeat(10_000);
            fs.upsert(42, source.as_bytes()).await;
            assert_eq!(fs.read(42).await.map(String::from_utf8), Some(Ok(source)));
        });
    }

    #[test_log::test]
    fn it_can_update_with_redis() {
        with_redis_running(|ip| async move {
            let mut fs = System::new(get_redis_chunk_store(&ip), MemoryMetaStore::new());

            let initial_source = "Initial contents";
            fs.upsert(42, initial_source.as_bytes()).await;

            let updated_source = "Updated contents";
            fs.upsert(42, updated_source.as_bytes()).await;

            assert_eq!(
                fs.read(42).await.map(String::from_utf8),
                Some(Ok(updated_source.to_string()))
            );
        });
    }

    #[test_log::test]
    fn can_restore_samples_with_redis() {
        with_redis_running(|ip| async move {
            let mut fs = System::new(get_redis_chunk_store(&ip), MemoryMetaStore::new());

            let samples = vec![
                "file_example_JPG_2500kB.jpg",
                "file_example_OOG_5MG.ogg",
                "file-example_PDF_1MB.pdf",
                "file-sample_1MB.docx",
            ];

            let meta: Vec<(&str, Vec<u8>)> = samples
                .iter()
                .map(|sample| {
                    let file = fs::read(format!("test/fixtures/{sample}"));
                    assert!(file.is_ok());
                    (*sample, file.unwrap())
                })
                .collect();

            for (name, file) in meta.iter() {
                fs.upsert(*name, file.as_slice()).await;
            }

            for (name, file) in meta.iter() {
                let result = fs.read(*name).await;
                assert!(result.is_some());
                let result = result.unwrap();
                assert_eq!(&result, file);
            }
        });
    }
}