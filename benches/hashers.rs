use std::{fs, hash::BuildHasher};

use cdcfs::{
    BuildHighwayHasher, BuildWyHasher, BuildXxh3Hasher, MemoryChunkStore, MemoryMetaStore, System,
};
use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use tokio::runtime::Runtime;

fn bench_hasher<H: BuildHasher + Default>(b: &mut Bencher<'_>) {
    b.to_async(Runtime::new().unwrap()).iter(|| async {
        let mut fs = System::new(
            MemoryChunkStore::new(),
            MemoryMetaStore::new(),
            H::default(),
        );

        let samples = vec![
            "file_example_JPG_2500kB.jpg",
            "file_example_OOG_5MG.ogg",
            "file-example_PDF_1MB.pdf",
            "file-sample_1MB.docx",
        ];

        let meta: Vec<(&str, Vec<u8>)> = samples
            .into_iter()
            .map(|sample| {
                let file = fs::read(format!("tests/fixtures/{sample}"))
                    .expect("Should be able to read fixture");
                (sample, file)
            })
            .collect();

        for (name, file) in &meta {
            fs.upsert(*name, file.as_slice()).await.unwrap();
        }

        for (name, file) in meta {
            let result = fs.read(name).await.unwrap();
            assert_eq!(result, file);
        }
    })
}

fn bench_hashers(c: &mut Criterion) {
    let mut group = c.benchmark_group("Hashers");

    group.bench_function("wyhash", bench_hasher::<BuildWyHasher>);
    group.bench_function("highway", bench_hasher::<BuildHighwayHasher>);
    group.bench_function("xxh3", bench_hasher::<BuildXxh3Hasher>);
}

criterion_group!(benches, bench_hashers);
criterion_main!(benches);
