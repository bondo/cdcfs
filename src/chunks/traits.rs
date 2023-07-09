use core::fmt::Debug;

use super::error::Result;

pub trait ChunkStore: Debug {
    fn get(&self, hash: &u64) -> Result<Vec<u8>>;

    fn upsert(&mut self, hash: u64, chunk: Vec<u8>) -> Result<()>;

    fn remove(&mut self, hash: &u64) -> Result<()>;
}
