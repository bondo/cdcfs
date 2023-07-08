use anyhow::Context;
use redis::{Client, Commands, IntoConnectionInfo};

use super::{error::Result, traits::ChunkStore, Error};

#[derive(Debug)]
pub struct RedisChunkStore(Client);

impl RedisChunkStore {
    pub fn new<T: IntoConnectionInfo>(params: T) -> Result<RedisChunkStore> {
        let client = Client::open(params).context("Redis error")?;
        Ok(Self(client))
    }
}

impl ChunkStore for RedisChunkStore {
    fn get(&self, hash: &u64) -> Result<Vec<u8>> {
        let mut conn = self.0.get_connection().context("Redis error")?;
        if !conn.exists(hash).context("Redis error")? {
            return Err(Error::NotFound);
        }
        let val: Vec<u8> = conn.get(hash).context("Redis error")?;
        Ok(val)
    }

    fn insert(&mut self, hash: u64, chunk: Vec<u8>) -> Result<()> {
        let mut conn = self.0.get_connection().context("Redis error")?;
        if conn.exists(hash).context("Redis error")? {
            return Err(Error::AlreadyExists);
        }
        conn.set(hash, chunk).context("Redis error")?;
        Ok(())
    }

    fn remove(&mut self, hash: &u64) -> Result<()> {
        let mut conn = self.0.get_connection().context("Redis error")?;
        if !conn.exists(hash).context("Redis error")? {
            return Err(Error::NotFound);
        }
        conn.del(hash).context("Redis error")?;
        Ok(())
    }
}
