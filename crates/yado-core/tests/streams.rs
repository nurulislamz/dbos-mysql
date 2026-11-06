use std::time::Duration;

use yado_core::{
    Begin, Begun, Engine, EngineConfig, Outcome, PORTABLE_JSON, Run, SendOutcome, StoredValue,
    StreamRead,
};

#[macro_use]
mod common;
use common::*;

// Far longer than any test, so only a wake can end a wait.
const NO_POLL: Option<Duration> = Some(Duration::from_secs(3600));

async fn engine(db: &TestDb) -> Engine {
    let config = EngineConfig {
        lease: LONG,
        pickle: false,
        ..EngineConfig::default()
    };
    Engine::connect(&db.url, config).await.unwrap()
}

async fn begin_run(engine: &Engine, id: &str) -> Run {
    let begin = Begin {
        name: "wf",
        input: "{}",
        max_recovery_attempts: 100,
        workflow_id: Some(id),
        parent: None,
        timeout_ms: None,
        ignore_propagated_deadline: false,
    };
    match engine.begin(begin).await.unwrap() {
        Begun::Run(run) => run,
        other => panic!("expected to run, got {other:?}"),
    }
}

fn value(text: &str) -> StreamRead {
    StreamRead::Value(StoredValue {
        text: text.to_owned(),
        serialization: PORTABLE_JSON.to_owned(),
    })
}

async fn at(engine: &Engine, offset: i64) -> StreamRead {
    engine.store().read_stream("wf", "k", offset).await.unwrap()
}

all_dbs!(values_come_back_in_order_until_closed, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = begin_run(&engine, "wf").await;
    assert_eq!(at(&engine, 0).await, StreamRead::Waiting);
    engine
        .write_stream(&run, Some(1), "k", "\"a\"", PORTABLE_JSON)
        .await
        .unwrap();
    // A write from a step has no step ID and is not recorded.
    engine
        .write_stream(&run, None, "k", "\"b\"", PORTABLE_JSON)
        .await
        .unwrap();
    // Replaying the recorded write does not append it again.
    let replayed = engine
        .write_stream(&run, Some(1), "k", "\"a\"", PORTABLE_JSON)
        .await
        .unwrap();
    assert_eq!(replayed, SendOutcome::Replayed);
    assert_eq!(at(&engine, 0).await, value("\"a\""));
    assert_eq!(at(&engine, 1).await, value("\"b\""));
    assert_eq!(at(&engine, 2).await, StreamRead::Waiting);
    engine.close_stream(&run, Some(2), "k").await.unwrap();
    assert_eq!(at(&engine, 2).await, StreamRead::Closed);
    // Past the close marker the stream has ended, even while the workflow runs.
    assert_eq!(at(&engine, 5).await, StreamRead::Closed);
    // Other keys are separate streams.
    let other = engine.store().read_stream("wf", "other", 0).await.unwrap();
    assert_eq!(other, StreamRead::Waiting);
    assert_eq!(
        engine.store().read_stream("missing", "k", 0).await.unwrap(),
        StreamRead::Closed
    );
});

all_dbs!(a_finished_workflow_ends_its_open_streams, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = begin_run(&engine, "wf").await;
    engine
        .write_stream(&run, None, "k", "1", PORTABLE_JSON)
        .await
        .unwrap();
    engine
        .finish(&run, Outcome::Success("null".to_owned()))
        .await
        .unwrap();
    assert_eq!(at(&engine, 0).await, value("1"));
    assert_eq!(at(&engine, 1).await, StreamRead::Closed);
});

all_dbs!(a_local_write_wakes_a_waiting_reader, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = begin_run(&engine, "wf").await;
    let reader = engine.clone();
    let read = tokio::spawn(async move { reader.read_stream("wf", "k", 0, None, NO_POLL).await });
    let started = std::time::Instant::now();
    while engine.waiting_keys() == 0 {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the reader never waited"
        );
        tokio::task::yield_now().await;
    }
    engine
        .write_stream(&run, None, "k", "\"hi\"", PORTABLE_JSON)
        .await
        .unwrap();
    let woken = tokio::time::timeout(Duration::from_secs(30), read).await;
    assert_eq!(
        woken.expect("the wake was lost").unwrap().unwrap(),
        value("\"hi\"")
    );
    // A timed read with nothing to read gives up.
    let timed = engine
        .read_stream("wf", "k", 1, Some(Duration::from_millis(50)), None)
        .await;
    assert_eq!(timed.unwrap(), StreamRead::Waiting);
});
