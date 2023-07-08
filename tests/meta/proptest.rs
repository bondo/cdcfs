use proptest::prelude::*;
use with_postgres_ready::with_postgres_ready;

use cdcfs::meta::{MemoryMetaStore, Meta, MetaStore, PostgresMetaStore};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    #[test]
    fn ransom_set_of_operations_result_in_same_output(
        operations in Operations::arbitrary(),
    ) {
        with_postgres_ready(|url| async move {
            let mut memory_meta_store = MemoryMetaStore::<i32>::new();
            let mut postgres_meta_store = PostgresMetaStore::new(&url).await.unwrap();

            for operation in operations.0.iter() {
                match operation {
                    Operation::Upsert(id, hashes) => {
                        let meta = Meta { hashes: hashes.clone(), size: 0 };
                        let mem = memory_meta_store.upsert(*id, meta.clone()).await.map_err(|e|format!("{e:?}"));
                        let red = postgres_meta_store.upsert(*id, meta.clone()).await.map_err(|e|format!("{e:?}"));
                        assert_eq!(mem, red);
                    },
                    Operation::Get(id) => {
                        let memory_meta = memory_meta_store.get(id).await.map_err(|e|format!("{e:?}"));
                        let postgres_meta = postgres_meta_store.get(id).await.map_err(|e|format!("{e:?}"));
                        assert_eq!(memory_meta, postgres_meta);
                    },
                    Operation::Remove(id) => {
                        let memory_meta = memory_meta_store.remove(id).await.map_err(|e|format!("{e:?}"));
                        let postgres_meta = postgres_meta_store.remove(id).await.map_err(|e|format!("{e:?}"));
                        assert_eq!(memory_meta, postgres_meta);
                    },
                }
            }
        });
    }
}

#[derive(Debug, Clone)]
enum Operation {
    Upsert(i32, Vec<u64>),
    Get(i32),
    Remove(i32),
}

#[derive(Debug, Clone)]
struct Operations(Vec<Operation>);

impl Arbitrary for Operations {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        let operations = vec![1, 2, 3];
        (
            prop::collection::vec(any::<i32>(), 10..50),
            prop::collection::vec(
                (
                    any::<prop::sample::Index>(),
                    any::<Vec<u64>>(),
                    prop::sample::select(operations),
                ),
                5_000,
            ),
        )
            .prop_map(|(ids, operations)| {
                Operations(
                    operations
                        .iter()
                        .map(|(idx, hashes, operation)| match operation {
                            1 => Operation::Upsert(ids[idx.index(ids.len())], hashes.to_owned()),
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
