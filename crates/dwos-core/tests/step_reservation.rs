use dwos_core::{Begin, Begun, Engine, EngineConfig, StepDecision};

#[macro_use]
mod common;
use common::*;

all_dbs!(reserved_steps_replay_by_their_original_ids, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = Engine::connect(&db.url, EngineConfig::default())
        .await
        .unwrap();
    let Begun::Run(run) = engine
        .begin(Begin {
            name: "wf",
            input: "{}",
            max_recovery_attempts: 10,
            workflow_id: Some("wf"),
            parent: None,
            timeout_ms: None,
            ignore_propagated_deadline: false,
        })
        .await
        .unwrap()
    else {
        panic!("expected a new run")
    };
    let first = run.next_step_id();
    let second = run.next_step_id();
    assert_eq!(
        engine.check_step(&run, second, "second").await.unwrap(),
        StepDecision::Run
    );
    engine
        .save_step_output(&run, second, "second", "2".into())
        .await
        .unwrap();
    engine
        .save_step_output(&run, first, "first", "1".into())
        .await
        .unwrap();
    assert_eq!(
        engine.check_step(&run, first, "first").await.unwrap(),
        StepDecision::ReplayOutput("1".into())
    );
    assert_eq!(
        engine.check_step(&run, second, "second").await.unwrap(),
        StepDecision::ReplayOutput("2".into())
    );
});
