pub mod chunks;
pub mod meta;
pub mod system;

use std::hash::BuildHasherDefault;

use highway::HighwayHasher;
use twox_hash::Xxh3Hash64;
use wyhash::WyHash;

pub use self::chunks::{MemoryChunkStore, RedisChunkStore};
pub use self::meta::{MemoryMetaStore, PostgresMetaStore};
pub use self::system::System;

pub type BuildWyHasher = BuildHasherDefault<WyHash>;
pub type BuildXxh3Hasher = BuildHasherDefault<Xxh3Hash64>;
pub type BuildHighwayHasher = BuildHasherDefault<HighwayHasher>;
