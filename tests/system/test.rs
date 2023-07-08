use std::{fs, io::Read};

use cdcfs::{MemoryChunkStore, MemoryMetaStore, PostgresMetaStore, RedisChunkStore, System};

use crate::utils::{with_postgres_ready, with_redis_ready};

#[tokio::test]
async fn it_can_read_and_write() {
    let source = b"Hello World!".repeat(10_000);
    let mut fs = System::new(MemoryChunkStore::new(), MemoryMetaStore::new());
    fs.upsert(42, &source).await.unwrap();
    assert_eq!(fs.read(42).await.unwrap(), source);
}

#[tokio::test]
async fn it_can_update() {
    let mut fs = System::new(MemoryChunkStore::new(), MemoryMetaStore::new());

    let initial_source = b"Initial contents";
    fs.upsert(42, initial_source).await.unwrap();

    let updated_source = b"Updated contents";
    fs.upsert(42, updated_source).await.unwrap();

    assert_eq!(fs.read(42).await.unwrap(), updated_source);

    fs.delete(42).await.unwrap();
    assert!(matches!(
        fs.read(42).await,
        Err(cdcfs::system::Error::MetaStore(
            cdcfs::meta::Error::NotFound
        ))
    ));
}

#[tokio::test]
async fn can_restore_samples() {
    let mut fs = System::new(MemoryChunkStore::new(), MemoryMetaStore::new());

    let samples = vec![
        "file_example_JPG_2500kB.jpg",
        "file_example_OOG_5MG.ogg",
        "file-example_PDF_1MB.pdf",
        "file-sample_1MB.docx",
    ];

    let meta: Vec<(&str, Vec<u8>)> = samples
        .into_iter()
        .map(|sample| {
            let file = fs::read(format!("test/fixtures/{sample}"))
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
}

#[test_log::test]
fn it_can_read_and_write_with_redis() {
    with_redis_ready(|url| async move {
        let mut fs = System::new(RedisChunkStore::new(url).unwrap(), MemoryMetaStore::new());

        let source = b"Hello World!".repeat(10_000);
        fs.upsert(42, &source).await.unwrap();
        assert_eq!(fs.read(42).await.unwrap(), source);
    });
}

#[test_log::test]
fn it_can_update_with_redis() {
    with_redis_ready(|url| async move {
        let mut fs = System::new(RedisChunkStore::new(url).unwrap(), MemoryMetaStore::new());

        let initial_source = b"Initial contents";
        fs.upsert(42, initial_source).await.unwrap();

        let updated_source = b"Updated contents";
        fs.upsert(42, updated_source).await.unwrap();

        assert_eq!(fs.read(42).await.unwrap(), updated_source);
    });
}

#[test_log::test]
fn can_restore_samples_with_redis() {
    with_redis_ready(|url| async move {
        let mut fs = System::new(RedisChunkStore::new(url).unwrap(), MemoryMetaStore::new());

        let samples = vec![
            "file_example_JPG_2500kB.jpg",
            "file_example_OOG_5MG.ogg",
            "file-example_PDF_1MB.pdf",
            "file-sample_1MB.docx",
        ];

        let meta: Vec<(&str, Vec<u8>)> = samples
            .into_iter()
            .map(|sample| {
                let file = fs::read(format!("test/fixtures/{sample}"))
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
    });
}

#[test_log::test]
fn it_can_read_and_write_with_postgres() {
    with_postgres_ready(|url| async move {
        let source = b"Hello World!".repeat(10_000);
        let mut fs = System::new(
            MemoryChunkStore::new(),
            PostgresMetaStore::new(&url).await.unwrap(),
        );
        fs.upsert(42, &source).await.unwrap();
        assert_eq!(fs.read(42).await.unwrap(), source);
    });
}

#[test_log::test]
fn it_can_update_with_postgres() {
    with_postgres_ready(|url| async move {
        let mut fs = System::new(
            MemoryChunkStore::new(),
            PostgresMetaStore::new(&url).await.unwrap(),
        );

        let initial_source = b"Initial contents";
        fs.upsert(42, initial_source).await.unwrap();

        let updated_source = b"Updated contents";
        fs.upsert(42, updated_source).await.unwrap();

        assert_eq!(fs.read(42).await.unwrap(), updated_source);
    });
}

#[test_log::test]
fn can_restore_samples_with_postgres() {
    with_postgres_ready(|url| async move {
        let mut fs = System::new(
            MemoryChunkStore::new(),
            PostgresMetaStore::new(&url).await.unwrap(),
        );

        let samples = vec![
            "file_example_JPG_2500kB.jpg",
            "file_example_OOG_5MG.ogg",
            "file-example_PDF_1MB.pdf",
            "file-sample_1MB.docx",
        ];

        let meta: Vec<(i32, Vec<u8>)> = samples
            .into_iter()
            .enumerate()
            .map(|(idx, sample)| {
                let file = fs::read(format!("test/fixtures/{sample}"))
                    .expect("Should be able to read fixture");
                (idx as i32, file)
            })
            .collect();

        for (id, file) in &meta {
            fs.upsert(*id, file.as_slice()).await.unwrap();
        }

        for (id, file) in meta {
            let result = fs.read(id).await.unwrap();
            assert_eq!(result, file);
        }
    });
}

#[tokio::test]
async fn can_stream_write_samples() {
    let mut fs = System::new(MemoryChunkStore::new(), MemoryMetaStore::new());

    let samples = vec![
        "file_example_JPG_2500kB.jpg",
        "file_example_OOG_5MG.ogg",
        "file-example_PDF_1MB.pdf",
        "file-sample_1MB.docx",
    ];

    let meta: Vec<(&str, fs::File, Vec<u8>)> = samples
        .into_iter()
        .map(|sample| {
            let file_bytes = fs::read(format!("test/fixtures/{sample}"))
                .expect("Should be able to read fixture");
            let file_stream = fs::File::open(format!("test/fixtures/{sample}"))
                .expect("Should be able to read fixture");
            (sample, file_stream, file_bytes)
        })
        .collect();

    for (name, file, _) in &meta {
        fs.upsert_stream(*name, file).await.unwrap();
    }

    for (name, _, bytes) in meta {
        let result = fs.read(name).await.unwrap();
        assert_eq!(result, bytes);
    }
}

#[tokio::test]
async fn can_stream_read_samples() {
    let mut fs = System::new(MemoryChunkStore::new(), MemoryMetaStore::new());

    let samples = vec![
        "file_example_JPG_2500kB.jpg",
        "file_example_OOG_5MG.ogg",
        "file-example_PDF_1MB.pdf",
        "file-sample_1MB.docx",
    ];

    let meta: Vec<(&str, Vec<u8>)> = samples
        .into_iter()
        .map(|sample| {
            let file = fs::read(format!("test/fixtures/{sample}"))
                .expect("Should be able to read fixture");
            (sample, file)
        })
        .collect();

    for (name, file) in &meta {
        fs.upsert(*name, file.as_slice()).await.unwrap();
    }

    for (name, file) in meta {
        let mut reader = fs.read_stream(name).await.expect("Should return stream");
        let mut buf = vec![];
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, file);
    }
}

#[tokio::test]
async fn can_read_into_with_samples() {
    let mut fs = System::new(MemoryChunkStore::new(), MemoryMetaStore::new());

    let samples = vec![
        "file_example_JPG_2500kB.jpg",
        "file_example_OOG_5MG.ogg",
        "file-example_PDF_1MB.pdf",
        "file-sample_1MB.docx",
    ];

    let meta: Vec<(&str, Vec<u8>)> = samples
        .into_iter()
        .map(|sample| {
            let file = fs::read(format!("test/fixtures/{sample}"))
                .expect("Should be able to read fixture");
            (sample, file)
        })
        .collect();

    for (name, file) in &meta {
        fs.upsert(*name, file.as_slice()).await.unwrap();
    }

    for (name, file) in meta {
        let mut buf = vec![];
        fs.read_into(name, &mut buf).await.unwrap();
        assert_eq!(buf, file);
    }
}
