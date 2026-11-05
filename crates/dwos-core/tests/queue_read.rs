use std::num::NonZeroU32;

use dwos_core::{Engine, EngineConfig};

#[macro_use]
mod common;
use common::*;

all_dbs!(queue_lookup_reads_persisted_limits, backend, {
    let db = TestDb::migrated(backend).await;
    let writer = Engine::connect(&db.url, EngineConfig::default())
        .await
        .unwrap();
    writer
        .ensure_queue("limited", NonZeroU32::new(3).unwrap())
        .await
        .unwrap();
    writer
        .ensure_queue("unlimited", NonZeroU32::MAX)
        .await
        .unwrap();
    let reader = Engine::connect(&db.url, EngineConfig::default())
        .await
        .unwrap();
    assert_eq!(
        reader.get_queue("limited").await.unwrap(),
        NonZeroU32::new(3)
    );
    assert_eq!(
        reader.get_queue("unlimited").await.unwrap(),
        Some(NonZeroU32::MAX)
    );
    assert_eq!(reader.get_queue("unknown").await.unwrap(), None);
    assert_eq!(writer.get_queue("unknown").await.unwrap(), None);
});

all_dbs!(queue_lookup_works_in_read_only_sessions, backend, {
    let db = TestDb::migrated(backend).await;
    let writer = Engine::connect(&db.url, EngineConfig::default())
        .await
        .unwrap();
    writer
        .ensure_queue("q", NonZeroU32::new(2).unwrap())
        .await
        .unwrap();
    let reader = Engine::connect(
        &db.url,
        EngineConfig {
            read_only: true,
            ..EngineConfig::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(reader.get_queue("q").await.unwrap(), NonZeroU32::new(2));
    assert_eq!(reader.get_queue("unknown").await.unwrap(), None);
    assert!(reader.ensure_queue("new", NonZeroU32::MIN).await.is_err());
    assert_eq!(writer.get_queue("new").await.unwrap(), None);
});

all_dbs!(queue_lookup_rejects_invalid_names, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = Engine::connect(&db.url, EngineConfig::default())
        .await
        .unwrap();
    for name in ["".to_owned(), "bad\0queue".to_owned(), "q".repeat(256)] {
        assert!(engine.get_queue(&name).await.is_err());
    }
});
