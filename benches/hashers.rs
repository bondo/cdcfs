use std::fs;

use cdcfs::{BuildHighwayHasher, BuildWyHasher, MemoryChunkStore, MemoryMetaStore, System};
use criterion::{criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;

fn bench_hashers(c: &mut Criterion) {
    let mut group = c.benchmark_group("Hashers");
    let runtime = Runtime::new().unwrap();

    group.bench_function("wyhash", |b| {
        b.to_async(&runtime).iter(|| async {
            let mut fs = System::new(
                MemoryChunkStore::new(),
                MemoryMetaStore::new(),
                BuildWyHasher,
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
    });

    group.bench_function("highway", |b| {
        b.to_async(&runtime).iter(|| async {
            let mut fs = System::new(
                MemoryChunkStore::new(),
                MemoryMetaStore::new(),
                BuildHighwayHasher,
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
    });
}

criterion_group!(benches, bench_hashers);
criterion_main!(benches);
