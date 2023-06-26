use core::fmt::Debug;
use std::fmt::Display;

pub trait ChunkStore: Debug {
    type Error: Debug + Display;

    fn get(&self, hash: &u64) -> Result<Vec<u8>, Self::Error>;

    fn insert(&mut self, hash: u64, chunk: Vec<u8>) -> Result<(), Self::Error>;

    fn remove(&mut self, hash: &u64) -> Result<(), Self::Error>;
}
