use std::num::NonZeroU32;

use yado_core::{
    Begin, Begun, Engine, EngineConfig, Error, ParentStep, Run, Status, WorkflowOptions,
};

#[macro_use]
mod common;
use common::*;

const HOUR_MS: i64 = 3_600_000;

fn call<'a>(parent: &'a Run, input: &'a str) -> Begin<'a> {
    let mut call = begin("wf", input, None);
    call.parent = Some(ParentStep {
        run: parent,
        step_id: 1,
    });
    call
}

fn begin<'a>(name: &'a str, input: &'a str, id: Option<&'a str>) -> Begin<'a> {
    Begin {
        name,
        input,
        max_recovery_attempts: 100,
        workflow_id: id,
        parent: None,
        timeout_ms: None,
        ignore_propagated_deadline: false,
    }
}

async fn engine(db: &TestDb) -> Engine {
    let engine = Engine::connect(&db.url, EngineConfig::default())
        .await
        .unwrap();
    engine.ensure_queue("q", NonZeroU32::MAX).await.unwrap();
    engine
}

async fn debounce(
    engine: &Engine,
    input: &str,
    period_ms: i64,
    timeout_ms: Option<i64>,
) -> Result<String, Error> {
    engine
        .debounce(
            "q",
            begin("wf", input, None),
            &WorkflowOptions::default(),
            "k",
            period_ms,
            timeout_ms,
        )
        .await
}

all_dbs!(
    calls_coalesce_into_one_workflow_with_the_last_inputs,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let first = debounce(&engine, "1", HOUR_MS, None).await.unwrap();
        let second = debounce(&engine, "2", HOUR_MS, None).await.unwrap();
        assert_eq!(first, second);
        let row = engine.get_status(&first).await.unwrap();
        assert_eq!(row.input, "2");
        assert_eq!(row.status, Status::Delayed);
        assert_eq!(row.deduplication_id.as_deref(), Some("wf-k"));
        assert_eq!(row.queue_name.as_deref(), Some("q"));
    }
);

all_dbs!(a_released_workflow_frees_the_key, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    // A zero period releases the workflow at once, so the next call starts a new one.
    let first = debounce(&engine, "1", 0, None).await.unwrap();
    let second = debounce(&engine, "2", HOUR_MS, None).await.unwrap();
    assert_ne!(first, second);
    let released = engine.get_status(&first).await.unwrap();
    assert_eq!(released.input, "1");
    assert_eq!(released.deduplication_id, None);
});

all_dbs!(the_timeout_caps_every_delay, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let id = debounce(&engine, "1", 100 * HOUR_MS, Some(HOUR_MS))
        .await
        .unwrap();
    let first = engine.get_status(&id).await.unwrap();
    let cap = first.created_at_ms + HOUR_MS;
    assert!(first.delay_until_ms.unwrap() <= cap);
    assert_eq!(
        debounce(&engine, "2", 100 * HOUR_MS, None).await.unwrap(),
        id
    );
    let bounced = engine.get_status(&id).await.unwrap();
    assert_eq!(bounced.input, "2");
    assert!(bounced.delay_until_ms.unwrap() <= cap);
});

all_dbs!(a_key_held_by_another_workflow_is_a_conflict, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let options = WorkflowOptions {
        deduplication_id: Some("wf-k".to_owned()),
        ..WorkflowOptions::default()
    };
    engine
        .enqueue_with("q", begin("wf", "user", None), &options)
        .await
        .unwrap();
    let error = debounce(&engine, "1", HOUR_MS, None).await.unwrap_err();
    assert!(
        matches!(error, Error::QueueDeduplicated { .. }),
        "{error:?}"
    );
    // The same key under another workflow's name is its own debounce.
    let other = engine
        .debounce(
            "q",
            begin("wf-other", "1", None),
            &WorkflowOptions::default(),
            "k",
            HOUR_MS,
            None,
        )
        .await
        .unwrap();
    let error = engine
        .debounce(
            "q",
            begin("wf", "1", None),
            &WorkflowOptions::default(),
            "other-k",
            HOUR_MS,
            None,
        )
        .await
        .unwrap_err();
    // "wf" + "other-k" and "wf-other" + "k" both make "wf-other-k", and must not share inputs.
    assert!(
        matches!(error, Error::QueueDeduplicated { .. }),
        "{error:?}"
    );
    assert_eq!(engine.get_status(&other).await.unwrap().input, "1");
});

all_dbs!(
    a_debounce_in_a_workflow_replays_its_saved_result,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let Begun::Run(parent) = engine
            .begin(begin("parent", "{}", Some("p")))
            .await
            .unwrap()
        else {
            panic!("expected to run the parent");
        };
        let id = engine
            .debounce(
                "q",
                call(&parent, "1"),
                &WorkflowOptions::default(),
                "k",
                HOUR_MS,
                None,
            )
            .await
            .unwrap();
        assert_eq!(id, "p-1");
        // The replay returns the saved ID and does not bounce the workflow again.
        let replay = engine
            .debounce(
                "q",
                call(&parent, "2"),
                &WorkflowOptions::default(),
                "k",
                HOUR_MS,
                None,
            )
            .await
            .unwrap();
        assert_eq!(replay, id);
        assert_eq!(engine.get_status(&id).await.unwrap().input, "1");
        let steps = engine.list_steps("p").await.unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].record.child_workflow_id.as_deref(), Some("p-1"));
    }
);
