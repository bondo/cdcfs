use proptest::prelude::*;

use cdcfs::chunks::{ChunkStore, MemoryChunkStore, RedisChunkStore};

use crate::utils::with_redis_ready;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    #[test]
    fn ransom_set_of_operations_result_in_same_output(
        operations in Operations::arbitrary(),
    ) {
        with_redis_ready(|url| async move {
            let mut memory_chunk_store = MemoryChunkStore::new();
            let mut redis_chunk_store = RedisChunkStore::new(url).unwrap();

            for operation in operations.0.iter() {
                match operation {
                    Operation::Insert(chunk_id, chunk) => {
                        let mem = memory_chunk_store.upsert(*chunk_id, chunk.clone()).map_err(|e|format!("{e:?}"));
                        let red = redis_chunk_store.upsert(*chunk_id, chunk.clone()).map_err(|e|format!("{e:?}"));
                        assert_eq!(mem, red);
                    },
                    Operation::Get(chunk_id) => {
                        let memory_chunk = memory_chunk_store.get(chunk_id).map_err(|e|format!("{e:?}"));
                        let redis_chunk = redis_chunk_store.get(chunk_id).map_err(|e|format!("{e:?}"));
                        assert_eq!(memory_chunk, redis_chunk);
                    },
                    Operation::Remove(chunk_id) => {
                        let memory_chunk = memory_chunk_store.remove(chunk_id).map_err(|e|format!("{e:?}"));
                        let redis_chunk = redis_chunk_store.remove(chunk_id).map_err(|e|format!("{e:?}"));
                        assert_eq!(memory_chunk, redis_chunk);
                    },
                }
            }
        });
    }
}

#[derive(Debug, Clone)]
enum Operation {
    Insert(u64, Vec<u8>),
    Get(u64),
    Remove(u64),
}

#[derive(Debug, Clone)]
struct Operations(Vec<Operation>);

impl Arbitrary for Operations {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        let operations = vec![1, 2, 3];
        (
            prop::collection::vec(any::<u64>(), 10..50),
            prop::collection::vec(
                (
                    any::<prop::sample::Index>(),
                    any::<Vec<u8>>(),
                    prop::sample::select(operations),
                ),
                5_000,
            ),
        )
            .prop_map(|(ids, operations)| {
                Operations(
                    operations
                        .iter()
                        .map(|(idx, chunk, operation)| match operation {
                            1 => Operation::Insert(ids[idx.index(ids.len())], chunk.to_owned()),
                            2 => Operation::Get(ids[idx.index(ids.len())]),
                            3 => Operation::Remove(ids[idx.index(ids.len())]),
                            _ => unreachable!(),
                        })
                        .collect(),
                )
            })
            .boxed()
    }
}
