use std::hash::BuildHasher;

use highway::HighwayHasher;

pub struct BuildHighwayHasher;

impl BuildHasher for BuildHighwayHasher {
    type Hasher = HighwayHasher;

    fn build_hasher(&self) -> Self::Hasher {
        HighwayHasher::default()
    }
}
