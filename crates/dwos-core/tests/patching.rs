use dwos_core::{Begin, Begun, Engine, EngineConfig, Error, Lease, Run, StepDecision};

#[macro_use]
mod common;
use common::*;

fn begin(id: &str) -> Begin<'_> {
    Begin {
        name: "wf",
        input: "{}",
        max_recovery_attempts: 100,
        workflow_id: Some(id),
        parent: None,
        timeout_ms: None,
        ignore_propagated_deadline: false,
    }
}

async fn started(engine: &Engine, id: &str) -> Run {
    let Begun::Run(run) = engine.begin(begin(id)).await.unwrap() else {
        panic!("expected a new run")
    };
    run
}

/// The same owner from step 0, as after a crash and recovery.
fn replay(run: &Run) -> Run {
    let lease = Lease {
        workflow_id: run.workflow_id().to_owned(),
        token: run.token().to_owned(),
    };
    Run::new(lease, run.serialization().to_owned(), None)
}

async fn engine(db: &TestDb) -> Engine {
    Engine::connect(&db.url, EngineConfig::default())
        .await
        .unwrap()
}

all_dbs!(a_new_run_takes_the_patch_and_replays_it, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = started(&engine, "a").await;
    assert!(engine.patch(&run, "use-baz").await.unwrap());
    assert_eq!(run.last_step_id(), 1);
    engine
        .save_step_output(&run, 2, "baz", "1".into())
        .await
        .unwrap();

    let again = replay(&run);
    assert!(engine.patch(&again, "use-baz").await.unwrap());
    assert_eq!(
        engine.check_step(&again, 2, "baz").await.unwrap(),
        StepDecision::ReplayOutput("1".into())
    );
    let steps = engine.list_steps("a").await.unwrap();
    assert_eq!(steps[0].step_name, "DBOS.patch-use-baz");
});

all_dbs!(an_old_run_past_the_point_skips_the_patch, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = started(&engine, "a").await;
    engine
        .save_step_output(&run, 1, "foo", "1".into())
        .await
        .unwrap();

    let new_code = replay(&run);
    assert!(!engine.patch(&new_code, "use-baz").await.unwrap());
    assert_eq!(new_code.last_step_id(), 0, "foo keeps step 1");
    assert_eq!(
        engine.check_step(&new_code, 1, "foo").await.unwrap(),
        StepDecision::ReplayOutput("1".into())
    );
    assert_eq!(engine.list_steps("a").await.unwrap().len(), 1);
});

all_dbs!(deprecate_patch_steps_over_only_its_own_marker, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let patched = started(&engine, "patched").await;
    assert!(engine.patch(&patched, "use-baz").await.unwrap());
    engine
        .save_step_output(&patched, 2, "baz", "1".into())
        .await
        .unwrap();
    let deprecated = replay(&patched);
    engine
        .deprecate_patch(&deprecated, "use-baz")
        .await
        .unwrap();
    assert_eq!(deprecated.last_step_id(), 1);
    assert_eq!(
        engine.check_step(&deprecated, 2, "baz").await.unwrap(),
        StepDecision::ReplayOutput("1".into())
    );

    let fresh = started(&engine, "fresh").await;
    engine.deprecate_patch(&fresh, "use-baz").await.unwrap();
    assert_eq!(fresh.last_step_id(), 0);
    assert!(engine.list_steps("fresh").await.unwrap().is_empty());
});

all_dbs!(removing_a_patch_too_early_fails_at_the_marker, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = started(&engine, "a").await;
    assert!(engine.patch(&run, "use-baz").await.unwrap());
    let error = engine
        .check_step(&replay(&run), 1, "baz")
        .await
        .unwrap_err();
    assert!(matches!(error, Error::UnexpectedStep { .. }), "{error:?}");
});
