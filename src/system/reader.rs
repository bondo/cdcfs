use std::{
    collections::VecDeque,
    io::{Cursor, Read},
};

use bytes::Buf;

use crate::chunks::ChunkStore;

pub struct Reader<'a, C: ChunkStore> {
    buf: Cursor<Vec<u8>>,
    chunk_store: &'a C,
    hashes: VecDeque<u64>,
}

impl<'a, C: ChunkStore> Reader<'a, C> {
    pub fn new(hashes: VecDeque<u64>, chunk_store: &'a C) -> Self {
        Self {
            buf: Cursor::new(vec![]),
            chunk_store,
            hashes,
        }
    }
}

impl<'a, C: ChunkStore> Read for Reader<'a, C> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if !self.buf.has_remaining() {
            let Some(hash) = self.hashes.pop_front() else {
                return Ok(0);
            };
            let chunk = self
                .chunk_store
                .get(&hash)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{e}")))?;
            self.buf = Cursor::new(chunk);
        }

        self.buf.read(buf)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use crate::chunks::{ChunkStore, MemoryChunkStore};

    use super::Reader;

    #[test]
    fn it_can_read_to_end() {
        let hashes: [u64; 3] = [42, 5, 1337];
        let mut chunk_store = MemoryChunkStore::new();

        chunk_store
            .insert(42, vec![9, 8, 7, 6, 5, 4, 3, 2])
            .unwrap();
        chunk_store.insert(5, vec![10, 20, 30]).unwrap();
        chunk_store.insert(1337, vec![100, 50, 75, 80]).unwrap();

        let mut reader = Reader::new(hashes.into(), &chunk_store);

        let mut buf = vec![];

        assert_eq!(reader.read_to_end(&mut buf).unwrap(), 15);
        assert_eq!(buf, [9, 8, 7, 6, 5, 4, 3, 2, 10, 20, 30, 100, 50, 75, 80]);
    }
}
