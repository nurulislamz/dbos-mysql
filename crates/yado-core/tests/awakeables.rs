use std::sync::Arc;
use std::time::Duration;

use yado_core::{Begin, Begun, Engine, EngineConfig, Error, PORTABLE_JSON, Run, StoredValue};

#[macro_use]
mod common;
use common::*;

const LONG_WAIT_MS: i64 = 60_000;
const BOUND: Duration = Duration::from_secs(30);

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

fn json(text: &str) -> StoredValue {
    StoredValue {
        text: text.to_owned(),
        serialization: PORTABLE_JSON.to_owned(),
    }
}

async fn complete(engine: &Engine, id: &str, value: &str) -> bool {
    engine
        .complete_awakeable(id, value, PORTABLE_JSON, None, None)
        .await
        .unwrap()
}

async fn wait(engine: &Engine, run: &Run, id: &str, timeout_ms: i64) -> StoredValue {
    tokio::time::timeout(BOUND, engine.wait_awakeable(run, 2, 3, id, timeout_ms))
        .await
        .expect("the wait did not end in time")
        .unwrap()
}

async fn step_names(engine: &Engine, workflow_id: &str) -> Vec<String> {
    let steps = engine.list_steps(workflow_id).await.unwrap();
    steps.into_iter().map(|step| step.step_name).collect()
}

all_dbs!(
    a_completion_before_the_wait_is_kept_and_replayed,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let run = begin_run(&engine, "wf").await;
        let id = engine.create_awakeable(&run, 1).await.unwrap();
        assert_eq!(engine.create_awakeable(&run, 1).await.unwrap(), id);
        assert!(complete(&engine, &id, "{\"ok\":true}").await);
        // The first completion wins; a retried webhook learns it was too late.
        assert!(!complete(&engine, &id, "\"second\"").await);
        assert_eq!(wait(&engine, &run, &id, 0).await, json("{\"ok\":true}"));
        assert_eq!(wait(&engine, &run, &id, 0).await, json("{\"ok\":true}"));
        assert_eq!(
            step_names(&engine, "wf").await,
            ["DBOS.createAwakeable", "DBOS.awaitAwakeable", "DBOS.sleep"]
        );
        let missing = engine
            .complete_awakeable("nope", "1", PORTABLE_JSON, None, None)
            .await;
        assert!(matches!(missing, Err(Error::AwakeableNotFound(_))));
        let missing = engine.wait_awakeable(&run, 4, 5, "nope", 0).await;
        assert!(matches!(missing, Err(Error::AwakeableNotFound(_))));
    }
);

all_dbs!(a_timeout_settles_the_awakeable, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = begin_run(&engine, "wf").await;
    let id = engine.create_awakeable(&run, 1).await.unwrap();
    assert_eq!(wait(&engine, &run, &id, 50).await, json("null"));
    // The wait recorded its timeout, so a late completion is refused, not silently lost.
    assert!(!complete(&engine, &id, "\"late\"").await);
    assert_eq!(wait(&engine, &run, &id, LONG_WAIT_MS).await, json("null"));
});

all_dbs!(a_completion_from_another_process_ends_a_wait, backend, {
    let db = TestDb::migrated(backend).await;
    let (waiting, webhook) = (engine(&db).await, engine(&db).await);
    let run = Arc::new(begin_run(&waiting, "wf").await);
    let id = waiting.create_awakeable(&run, 1).await.unwrap();
    let waiter = {
        let (engine, run, id) = (waiting.clone(), run.clone(), id.clone());
        tokio::spawn(async move { wait(&engine, &run, &id, LONG_WAIT_MS).await })
    };
    while waiting.waiting_keys() == 0 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(complete(&webhook, &id, "42").await);
    assert_eq!(waiter.await.unwrap(), json("42"));
});

all_dbs!(recovery_returns_the_same_id_and_completion, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = begin_run(&engine, "wf").await;
    let id = engine.create_awakeable(&run, 1).await.unwrap();
    engine.release(&run).await.unwrap();
    // Completed while no process owns the workflow.
    assert!(complete(&engine, &id, "\"done\"").await);
    let claim = engine.recover_now(&names()).await.unwrap().remove(0);
    let run = Run::new(claim.lease, claim.serialization, claim.deadline_epoch_ms);
    assert_eq!(engine.create_awakeable(&run, 1).await.unwrap(), id);
    assert_eq!(
        wait(&engine, &run, &id, LONG_WAIT_MS).await,
        json("\"done\"")
    );
});

all_dbs!(a_workflow_completion_replays_its_answer, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let owner = begin_run(&engine, "owner").await;
    let other = begin_run(&engine, "other").await;
    let id = engine.create_awakeable(&owner, 1).await.unwrap();
    for _ in 0..2 {
        // Replay of the recorded step answers true again, not "already completed".
        let stored = engine
            .complete_awakeable(&id, "1", PORTABLE_JSON, Some(&other), Some(1))
            .await
            .unwrap();
        assert!(stored);
    }
    assert_eq!(
        step_names(&engine, "other").await,
        ["DBOS.completeAwakeable"]
    );
    assert_eq!(wait(&engine, &owner, &id, 0).await, json("1"));
});
