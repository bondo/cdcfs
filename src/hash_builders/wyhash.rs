use std::hash::BuildHasher;

use wyhash::WyHash;

pub struct BuildWyHasher;

impl BuildHasher for BuildWyHasher {
    type Hasher = WyHash;

    fn build_hasher(&self) -> Self::Hasher {
        WyHash::default()
    }
}
