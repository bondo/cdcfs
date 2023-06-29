use std::{fmt::Debug, hash::Hash, io::Read};

use fastcdc::v2020::{FastCDC, StreamCDC};
use wyhash::wyhash;

use crate::{
    chunks::ChunkStore,
    meta::{Meta, MetaStore},
};

use super::{error::Result, reader::Reader};

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
