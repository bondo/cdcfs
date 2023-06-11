use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasherDefault,
};

use fastcdc::v2020::FastCDC;
use nohash_hasher::NoHashHasher;
use wyhash::wyhash;

#[derive(Debug)]
struct File {
    hashes: Vec<u64>,
    size: usize,
}

#[derive(Debug)]
pub struct CDCFS {
    chunks: HashMap<u64, Vec<u8>, BuildHasherDefault<NoHashHasher<u64>>>, // Should exist in key-value store
    files: HashMap<u32, File>, // Should exist in database
}

static AVG_SIZE: u32 = u32::pow(2, 14);
static MIN_SIZE: u32 = AVG_SIZE / 4;
static MAX_SIZE: u32 = AVG_SIZE * 4;

impl CDCFS {
    pub fn new() -> Self {
        Self {
            chunks: HashMap::with_hasher(BuildHasherDefault::default()),
            files: HashMap::new(),
        }
    }

    pub fn upsert(&mut self, id: u32, source: &[u8]) {
        let chunker = FastCDC::new(source, MIN_SIZE, AVG_SIZE, MAX_SIZE);
        let mut hashes = vec![];
        for chunk in chunker {
            let bytes = &source[chunk.offset..chunk.offset + chunk.length];
            let hash = wyhash(bytes, 42);
            self.chunks.insert(hash, bytes.to_owned());
            hashes.push(hash);
        }
        self.files.insert(
            id,
            File {
                hashes,
                size: source.len(),
            },
        );
    }

    pub fn read(&self, id: u32) -> Option<Vec<u8>> {
        let file = self.files.get(&id)?;
        let mut result = Vec::with_capacity(file.size);
        for hash in file.hashes.iter() {
            let chunk = self.chunks.get(&hash).expect("Hash should exist in map");
            result.extend_from_slice(chunk);
        }
        Some(result)
    }

    pub fn delete(&mut self, id: u32) {
        self.files.remove(&id);
    }

    pub fn gc(&mut self) {
        let mut hashes: HashSet<u64, BuildHasherDefault<NoHashHasher<u64>>> =
            HashSet::with_capacity_and_hasher(self.chunks.len(), BuildHasherDefault::default());
        hashes.extend(self.chunks.keys());
        for file in self.files.values() {
            for hash in file.hashes.iter() {
                hashes.remove(hash);
            }
        }
        for hash in hashes {
            self.chunks.remove(&hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_can_read_and_write() {
        let source = "Hello World!".repeat(10_000);
        let mut fs = CDCFS::new();
        fs.upsert(42, source.as_bytes());
        assert_eq!(fs.read(42).map(|v| String::from_utf8(v)), Some(Ok(source)));
    }

    #[test]
    fn it_can_update() {
        let mut fs = CDCFS::new();

        let initial_source = "Initial contents";
        fs.upsert(42, initial_source.as_bytes());

        let updated_source = "Updated contents";
        fs.upsert(42, updated_source.as_bytes());

        assert_eq!(
            fs.read(42).map(|v| String::from_utf8(v)),
            Some(Ok(updated_source.to_string()))
        );
    }

    #[test]
    fn can_gc() {
        let mut fs = CDCFS::new();

        fs.upsert(10, "Wow, such nice".as_bytes());
        assert!(fs.read(10).is_some());
        assert_eq!(fs.files.len(), 1);
        assert_eq!(fs.chunks.len(), 1);

        fs.gc();
        assert!(fs.read(10).is_some());
        assert_eq!(fs.files.len(), 1);
        assert_eq!(fs.chunks.len(), 1);

        fs.upsert(10, "New contents :D".as_bytes());
        assert!(fs.read(10).is_some());
        assert_eq!(fs.files.len(), 1);
        assert_eq!(fs.chunks.len(), 2);

        fs.gc();
        assert!(fs.read(10).is_some());
        assert_eq!(fs.files.len(), 1);
        assert_eq!(fs.chunks.len(), 1);

        fs.delete(10);
        assert!(fs.read(10).is_none());
        assert_eq!(fs.files.len(), 0);
        assert_eq!(fs.chunks.len(), 1);

        fs.gc();
        assert!(fs.read(10).is_none());
        assert_eq!(fs.files.len(), 0);
        assert_eq!(fs.chunks.len(), 0);
    }
}
