use std::{collections::HashMap, fmt::Debug, hash::Hash};

use chunks::traits::ChunkStore;
use fastcdc::v2020::FastCDC;
use wyhash::wyhash;

pub mod chunks;

#[derive(Debug)]
struct File {
    hashes: Vec<u64>,
    size: usize,
}

#[derive(Debug)]
pub struct CDCFS<K, ChunkStoreError> {
    chunks: Box<dyn ChunkStore<Error = ChunkStoreError>>, // Should exist in key-value store
    files: HashMap<K, File>,                              // Should exist in database
}

static AVG_SIZE: u32 = u32::pow(2, 14);
static MIN_SIZE: u32 = AVG_SIZE / 4;
static MAX_SIZE: u32 = AVG_SIZE * 4;

impl<K, ChunkStoreError: Debug> CDCFS<K, ChunkStoreError>
where
    K: Eq + Hash,
{
    pub fn new(chunks: Box<dyn ChunkStore<Error = ChunkStoreError>>) -> Self {
        Self {
            chunks,
            files: HashMap::new(),
        }
    }

    pub async fn upsert(&mut self, id: K, source: &[u8]) {
        let chunker = FastCDC::new(source, MIN_SIZE, AVG_SIZE, MAX_SIZE);
        let mut hashes = vec![];
        for chunk in chunker {
            let bytes = &source[chunk.offset..chunk.offset + chunk.length];
            let hash = wyhash(bytes, 42);
            self.chunks
                .insert(hash, bytes.to_owned())
                .await
                .expect("Should be able to insert chunk");
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

    pub async fn read(&self, id: K) -> Option<Vec<u8>> {
        let file = self.files.get(&id)?;
        let mut result = Vec::with_capacity(file.size);
        for hash in file.hashes.iter() {
            let chunk = self
                .chunks
                .get(hash)
                .await
                .expect("Hash should exist in map");
            result.extend_from_slice(chunk);
        }
        Some(result)
    }

    pub fn delete(&mut self, id: K) {
        self.files.remove(&id);
    }

    // pub fn gc(&mut self) {
    //     let mut hashes: HashSet<u64, BuildHasherDefault<NoHashHasher<u64>>> =
    //         HashSet::with_capacity_and_hasher(self.chunks.len(), BuildHasherDefault::default());
    //     hashes.extend(self.chunks.keys());
    //     for file in self.files.values() {
    //         for hash in file.hashes.iter() {
    //             hashes.remove(hash);
    //         }
    //     }
    //     for hash in hashes {
    //         self.chunks.remove(&hash);
    //     }
    // }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::chunks::memory::MemoryChunkStore;

    use super::*;

    #[tokio::test]
    async fn it_can_read_and_write() {
        let source = "Hello World!".repeat(10_000);
        let mut fs = CDCFS::new(Box::new(MemoryChunkStore::new()));
        fs.upsert(42, source.as_bytes()).await;
        assert_eq!(fs.read(42).await.map(String::from_utf8), Some(Ok(source)));
    }

    #[tokio::test]
    async fn it_can_update() {
        let mut fs = CDCFS::new(Box::new(MemoryChunkStore::new()));

        let initial_source = "Initial contents";
        fs.upsert(42, initial_source.as_bytes()).await;

        let updated_source = "Updated contents";
        fs.upsert(42, updated_source.as_bytes()).await;

        assert_eq!(
            fs.read(42).await.map(String::from_utf8),
            Some(Ok(updated_source.to_string()))
        );
    }

    // #[test]
    // fn can_gc() {
    //     let mut fs = CDCFS::new(Box::new(MemoryChunkStore::new()));

    //     fs.upsert(10, "Wow, such nice".as_bytes());
    //     assert!(fs.read(10).await.is_some());
    //     assert_eq!(fs.files.len(), 1);
    //     assert_eq!(fs.chunks.len(), 1);

    //     fs.gc();
    //     assert!(fs.read(10).await.is_some());
    //     assert_eq!(fs.files.len(), 1);
    //     assert_eq!(fs.chunks.len(), 1);

    //     fs.upsert(10, "New contents :D".as_bytes());
    //     assert!(fs.read(10).await.is_some());
    //     assert_eq!(fs.files.len(), 1);
    //     assert_eq!(fs.chunks.len(), 2);

    //     fs.gc();
    //     assert!(fs.read(10).await.is_some());
    //     assert_eq!(fs.files.len(), 1);
    //     assert_eq!(fs.chunks.len(), 1);

    //     fs.delete(10);
    //     assert!(fs.read(10).await.is_none());
    //     assert_eq!(fs.files.len(), 0);
    //     assert_eq!(fs.chunks.len(), 1);

    //     fs.gc();
    //     assert!(fs.read(10).await.is_none());
    //     assert_eq!(fs.files.len(), 0);
    //     assert_eq!(fs.chunks.len(), 0);
    // }

    #[tokio::test]
    async fn can_restore_samples() {
        let mut fs = CDCFS::new(Box::new(MemoryChunkStore::new()));

        let samples = vec![
            "file_example_JPG_2500kB.jpg",
            "file_example_OOG_5MG.ogg",
            "file-example_PDF_1MB.pdf",
            "file-sample_1MB.docx",
        ];

        let files: Vec<(&str, Vec<u8>)> = samples
            .iter()
            .map(|sample| {
                let file = fs::read(format!("test/fixtures/{sample}"));
                assert!(file.is_ok());
                (*sample, file.unwrap())
            })
            .collect();

        for (name, file) in files.iter() {
            fs.upsert(*name, file.as_slice()).await;
        }

        assert_eq!(fs.files.len(), 4);

        for (name, file) in files.iter() {
            let result = fs.read(*name).await;
            assert!(result.is_some());
            let result = result.unwrap();
            assert_eq!(&result, file);
        }
    }
}
