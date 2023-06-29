use std::{fmt::Debug, hash::Hash, io::Read};

use fastcdc::v2020::{FastCDC, StreamCDC};
use wyhash::wyhash;

use crate::{
    chunks::traits::ChunkStore,
    meta::traits::{Meta, MetaStore},
};

use self::{error::Result, reader::Reader};

mod error;
mod reader;

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

    pub async fn upsert(&mut self, key: K, source: &[u8]) -> Result<()> {
        let chunker = FastCDC::new(source, MIN_SIZE, AVG_SIZE, MAX_SIZE);
        let mut hashes = vec![];
        for chunk in chunker {
            let bytes = source[chunk.offset..chunk.offset + chunk.length].to_vec();
            hashes.push(self.write_chunk(bytes)?);
        }
        self.write_meta(key, hashes, source.len()).await
    }

    pub async fn upsert_stream<R: Read>(&mut self, key: K, source: R) -> Result<()> {
        let chunker = StreamCDC::new(source, MIN_SIZE, AVG_SIZE, MAX_SIZE);
        let mut hashes = vec![];
        let mut size: usize = 0;
        for chunk in chunker {
            let chunk = chunk?;
            hashes.push(self.write_chunk(chunk.data)?);
            size += chunk.length;
        }
        self.write_meta(key, hashes, size).await
    }

    fn write_chunk(&mut self, bytes: Vec<u8>) -> Result<u64> {
        let hash = wyhash(&bytes, 42);

        self.chunk_store.insert(hash, bytes)?;

        Ok(hash)
    }

    async fn write_meta(&mut self, key: K, hashes: Vec<u64>, size: usize) -> Result<()> {
        self.meta_store.upsert(key, Meta { hashes, size }).await?;
        Ok(())
    }

    pub async fn read(&self, key: K) -> Result<Vec<u8>> {
        let meta = self.meta_store.get(&key).await?;
        let mut result = Vec::with_capacity(meta.size);
        for hash in &meta.hashes {
            let chunk = self.chunk_store.get(hash)?;
            result.extend_from_slice(&chunk);
        }
        Ok(result)
    }

    pub async fn read_stream(&self, key: K) -> Result<Reader<C>> {
        let meta = self.meta_store.get(&key).await?;

        Ok(Reader::new(meta.hashes.into(), &self.chunk_store))
    }

    pub async fn read_into(&self, key: K, writer: &mut impl std::io::Write) -> Result<()> {
        let meta = self.meta_store.get(&key).await?;

        for hash in &meta.hashes {
            let chunk = self.chunk_store.get(hash)?;
            writer.write_all(&chunk)?;
        }

        Ok(())
    }

    pub async fn delete(&mut self, key: K) -> Result<()> {
        self.meta_store.remove(&key).await?;
        Ok(())
    }
}

// To run tests, first run `docker pull redis:6.0.19-alpine3.18` locally

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{
        chunks::{memory::MemoryChunkStore, redis::RedisChunkStore},
        meta::{memory::MemoryMetaStore, postgres::PostgresMetaStore},
        test::{postgres::with_postgres_ready, redis::with_redis_ready},
    };

    use super::*;

    #[tokio::test]
    async fn it_can_read_and_write() {
        let source = b"Hello World!".repeat(10_000);
        let mut fs = System::new(MemoryChunkStore::new(), MemoryMetaStore::new());
        fs.upsert(42, &source).await.unwrap();
        assert_eq!(fs.read(42).await.unwrap(), source);
    }

    #[tokio::test]
    async fn it_can_update() {
        let mut fs = System::new(MemoryChunkStore::new(), MemoryMetaStore::new());

        let initial_source = b"Initial contents";
        fs.upsert(42, initial_source).await.unwrap();

        let updated_source = b"Updated contents";
        fs.upsert(42, updated_source).await.unwrap();

        assert_eq!(fs.read(42).await.unwrap(), updated_source);
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
            .into_iter()
            .map(|sample| {
                let file = fs::read(format!("test/fixtures/{sample}"))
                    .expect("Should be able to read fixture");
                (sample, file)
            })
            .collect();

        for (name, file) in &meta {
            fs.upsert(*name, file.as_slice()).await.unwrap();
        }

        for (name, file) in meta {
            let result = fs.read(name).await.unwrap();
            assert_eq!(result, file);
        }
    }

    #[test_log::test]
    fn it_can_read_and_write_with_redis() {
        with_redis_ready(|url| async move {
            let mut fs = System::new(RedisChunkStore::new(url).unwrap(), MemoryMetaStore::new());

            let source = b"Hello World!".repeat(10_000);
            fs.upsert(42, &source).await.unwrap();
            assert_eq!(fs.read(42).await.unwrap(), source);
        });
    }

    #[test_log::test]
    fn it_can_update_with_redis() {
        with_redis_ready(|url| async move {
            let mut fs = System::new(RedisChunkStore::new(url).unwrap(), MemoryMetaStore::new());

            let initial_source = b"Initial contents";
            fs.upsert(42, initial_source).await.unwrap();

            let updated_source = b"Updated contents";
            fs.upsert(42, updated_source).await.unwrap();

            assert_eq!(fs.read(42).await.unwrap(), updated_source);
        });
    }

    #[test_log::test]
    fn can_restore_samples_with_redis() {
        with_redis_ready(|url| async move {
            let mut fs = System::new(RedisChunkStore::new(url).unwrap(), MemoryMetaStore::new());

            let samples = vec![
                "file_example_JPG_2500kB.jpg",
                "file_example_OOG_5MG.ogg",
                "file-example_PDF_1MB.pdf",
                "file-sample_1MB.docx",
            ];

            let meta: Vec<(&str, Vec<u8>)> = samples
                .into_iter()
                .map(|sample| {
                    let file = fs::read(format!("test/fixtures/{sample}"))
                        .expect("Should be able to read fixture");
                    (sample, file)
                })
                .collect();

            for (name, file) in &meta {
                fs.upsert(*name, file.as_slice()).await.unwrap();
            }

            for (name, file) in meta {
                let result = fs.read(name).await.unwrap();
                assert_eq!(result, file);
            }
        });
    }

    #[test_log::test]
    fn it_can_read_and_write_with_postgres() {
        with_postgres_ready(|url| async move {
            let source = b"Hello World!".repeat(10_000);
            let mut fs = System::new(
                MemoryChunkStore::new(),
                PostgresMetaStore::new(&url).await.unwrap(),
            );
            fs.upsert(42, &source).await.unwrap();
            assert_eq!(fs.read(42).await.unwrap(), source);
        });
    }

    #[test_log::test]
    fn it_can_update_with_postgres() {
        with_postgres_ready(|url| async move {
            let mut fs = System::new(
                MemoryChunkStore::new(),
                PostgresMetaStore::new(&url).await.unwrap(),
            );

            let initial_source = b"Initial contents";
            fs.upsert(42, initial_source).await.unwrap();

            let updated_source = b"Updated contents";
            fs.upsert(42, updated_source).await.unwrap();

            assert_eq!(fs.read(42).await.unwrap(), updated_source);
        });
    }

    #[test_log::test]
    fn can_restore_samples_with_postgres() {
        with_postgres_ready(|url| async move {
            let mut fs = System::new(
                MemoryChunkStore::new(),
                PostgresMetaStore::new(&url).await.unwrap(),
            );

            let samples = vec![
                "file_example_JPG_2500kB.jpg",
                "file_example_OOG_5MG.ogg",
                "file-example_PDF_1MB.pdf",
                "file-sample_1MB.docx",
            ];

            let meta: Vec<(i32, Vec<u8>)> = samples
                .into_iter()
                .enumerate()
                .map(|(idx, sample)| {
                    let file = fs::read(format!("test/fixtures/{sample}"))
                        .expect("Should be able to read fixture");
                    (idx as i32, file)
                })
                .collect();

            for (id, file) in &meta {
                fs.upsert(*id, file.as_slice()).await.unwrap();
            }

            for (id, file) in meta {
                let result = fs.read(id).await.unwrap();
                assert_eq!(result, file);
            }
        });
    }

    #[tokio::test]
    async fn can_stream_write_samples() {
        let mut fs = System::new(MemoryChunkStore::new(), MemoryMetaStore::new());

        let samples = vec![
            "file_example_JPG_2500kB.jpg",
            "file_example_OOG_5MG.ogg",
            "file-example_PDF_1MB.pdf",
            "file-sample_1MB.docx",
        ];

        let meta: Vec<(&str, fs::File, Vec<u8>)> = samples
            .into_iter()
            .map(|sample| {
                let file_bytes = fs::read(format!("test/fixtures/{sample}"))
                    .expect("Should be able to read fixture");
                let file_stream = fs::File::open(format!("test/fixtures/{sample}"))
                    .expect("Should be able to read fixture");
                (sample, file_stream, file_bytes)
            })
            .collect();

        for (name, file, _) in &meta {
            fs.upsert_stream(*name, file).await.unwrap();
        }

        for (name, _, bytes) in meta {
            let result = fs.read(name).await.unwrap();
            assert_eq!(result, bytes);
        }
    }

    #[tokio::test]
    async fn can_stream_read_samples() {
        let mut fs = System::new(MemoryChunkStore::new(), MemoryMetaStore::new());

        let samples = vec![
            "file_example_JPG_2500kB.jpg",
            "file_example_OOG_5MG.ogg",
            "file-example_PDF_1MB.pdf",
            "file-sample_1MB.docx",
        ];

        let meta: Vec<(&str, Vec<u8>)> = samples
            .into_iter()
            .map(|sample| {
                let file = fs::read(format!("test/fixtures/{sample}"))
                    .expect("Should be able to read fixture");
                (sample, file)
            })
            .collect();

        for (name, file) in &meta {
            fs.upsert(*name, file.as_slice()).await.unwrap();
        }

        for (name, file) in meta {
            let mut reader = fs.read_stream(name).await.expect("Should return stream");
            let mut buf = vec![];
            reader.read_to_end(&mut buf).unwrap();
            assert_eq!(buf, file);
        }
    }

    #[tokio::test]
    async fn can_read_into_with_samples() {
        let mut fs = System::new(MemoryChunkStore::new(), MemoryMetaStore::new());

        let samples = vec![
            "file_example_JPG_2500kB.jpg",
            "file_example_OOG_5MG.ogg",
            "file-example_PDF_1MB.pdf",
            "file-sample_1MB.docx",
        ];

        let meta: Vec<(&str, Vec<u8>)> = samples
            .into_iter()
            .map(|sample| {
                let file = fs::read(format!("test/fixtures/{sample}"))
                    .expect("Should be able to read fixture");
                (sample, file)
            })
            .collect();

        for (name, file) in &meta {
            fs.upsert(*name, file.as_slice()).await.unwrap();
        }

        for (name, file) in meta {
            let mut buf = vec![];
            fs.read_into(name, &mut buf).await.unwrap();
            assert_eq!(buf, file);
        }
    }
}
