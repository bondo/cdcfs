use std::hash::BuildHasher;

use wyhash::WyHash;

pub struct BuildWyHasher {
    seed: u64,
}

impl BuildWyHasher {
    pub fn new() -> Self {
        Self { seed: 0 }
    }

    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }
}

impl Default for BuildWyHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl BuildHasher for BuildWyHasher {
    type Hasher = WyHash;

    fn build_hasher(&self) -> Self::Hasher {
        WyHash::with_seed(self.seed)
    }
}
