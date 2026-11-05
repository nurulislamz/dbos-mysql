//! A step attempt's timers against the real step path. The timer rules are unit tests in `lease.rs`.

use std::future::{Future, pending};
use std::time::Duration;

use dwos_core::{
    Begin, Begun, Engine, EngineConfig, Error, Finished, Outcome, Run, Status, StepDecision,
    StepExpiry, StepPolicy, StepWatch,
};

#[macro_use]
mod common;
use common::*;

async fn engine_with_lease(db: &TestDb, lease: Duration) -> Engine {
    let config = EngineConfig {
        lease,
        pickle: false,
        ..EngineConfig::default()
    };
    Engine::connect(&db.url, config).await.unwrap()
}

async fn begin_run(engine: &Engine, timeout_ms: Option<i64>) -> Run {
    let begin = Begin {
        name: "wf",
        input: "{}",
        max_recovery_attempts: 100,
        workflow_id: Some("a"),
        parent: None,
        timeout_ms,
        ignore_propagated_deadline: false,
    };
    match engine.begin(begin).await.unwrap() {
        Begun::Run(run) => run,
        other => panic!("expected to run, got {other:?}"),
    }
}

/// What an SDK does for one attempt: race the body against the timers, and let `finish` decide.
async fn attempt<F>(watch: &StepWatch, body: F) -> Result<String, StepExpiry>
where
    F: Future<Output = String>,
{
    tokio::select! {
        value = body => watch.finish().map(|()| value),
        why = watch.expired() => Err(why.expect("nothing finished this attempt")),
    }
}

all_dbs!(
    a_silent_step_fails_while_the_owner_lease_stays_valid,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine_with_lease(&db, Duration::from_secs(1)).await;
        let run = begin_run(&engine, None).await;
        let active = engine.keep_lease(&run);
        let policy = StepPolicy::new(false, 0.0, 1, 1.0)
            .unwrap()
            .with_timeouts(None, Some(1.5))
            .unwrap();
        let step_id = run.next_step_id();
        let watch = policy.watch().unwrap();
        assert_eq!(
            attempt(&watch, pending()).await,
            Err(StepExpiry::MissedHeartbeat)
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(1), active.lost())
                .await
                .is_err(),
            "the keeper does not hear about heartbeats"
        );
        assert!(
            engine.recover_now(&names()).await.unwrap().is_empty(),
            "the silence outlasted the lease, which the keeper kept renewing"
        );
        engine
            .save_step_error(&run, step_id, "silent", "expired".to_owned())
            .await
            .unwrap();
        assert_eq!(
            engine.get_status("a").await.unwrap().status,
            Status::Pending
        );
    }
);

all_dbs!(a_deadline_ends_one_attempt_and_the_retry_runs, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine_with_lease(&db, LONG).await;
    let run = begin_run(&engine, None).await;
    let policy = StepPolicy::new(true, 0.0, 3, 1.0)
        .unwrap()
        .with_timeouts(Some(0.2), None)
        .unwrap();
    let step_id = run.next_step_id();
    let mut attempts = policy.attempts();
    let watch = policy.watch().unwrap();
    assert_eq!(attempt(&watch, pending()).await, Err(StepExpiry::Deadline));
    assert_eq!(attempts.failed(true), Some(Duration::ZERO));
    assert_eq!(attempts.status(), (Some(1), Some(3)));
    assert!(engine.list_steps("a").await.unwrap().is_empty());

    let watch = policy.watch().unwrap();
    let value = attempt(&watch, async { "second".to_owned() })
        .await
        .unwrap();
    engine
        .save_step_output(&run, step_id, "extract", value)
        .await
        .unwrap();
    assert_eq!(
        engine.check_step(&run, step_id, "extract").await.unwrap(),
        StepDecision::ReplayOutput("second".to_owned())
    );
    assert_eq!(
        engine
            .finish(&run, Outcome::Success("1".to_owned()))
            .await
            .unwrap(),
        Finished::Accepted
    );
    assert_eq!(
        engine.get_status("a").await.unwrap().status,
        Status::Success
    );
});

all_dbs!(
    the_body_can_run_its_failure_step_after_the_last_expiry,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine_with_lease(&db, LONG).await;
        let run = begin_run(&engine, None).await;
        let active = engine.keep_lease(&run);
        let policy = StepPolicy::new(true, 0.0, 2, 1.0)
            .unwrap()
            .with_timeouts(Some(0.1), None)
            .unwrap();
        let step_id = run.next_step_id();
        let mut attempts = policy.attempts();
        loop {
            let watch = policy.watch().unwrap();
            assert_eq!(attempt(&watch, pending()).await, Err(StepExpiry::Deadline));
            if attempts.failed(true).is_none() {
                break;
            }
        }
        engine
            .save_step_error(&run, step_id, "evaluate", "expired".to_owned())
            .await
            .unwrap();
        assert_eq!(
            engine.check_step(&run, step_id, "evaluate").await.unwrap(),
            StepDecision::ReplayError("expired".to_owned())
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(1), active.lost())
                .await
                .is_err()
        );

        let failure_step = run.next_step_id();
        engine
            .save_step_output(&run, failure_step, "mark_evaluation_failed", "1".to_owned())
            .await
            .unwrap();
        assert_eq!(
            engine
                .finish(&run, Outcome::Success("failed".to_owned()))
                .await
                .unwrap(),
            Finished::Accepted
        );
        assert_eq!(
            engine.get_status("a").await.unwrap().status,
            Status::Success
        );
    }
);

all_dbs!(a_workflow_deadline_still_ends_the_workflow, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine_with_lease(&db, LONG).await;
    let run = begin_run(&engine, Some(300)).await;
    let policy = StepPolicy::new(false, 0.0, 1, 1.0)
        .unwrap()
        .with_timeouts(Some(3600.0), Some(3600.0))
        .unwrap();
    let step_id = run.next_step_id();
    let _watch = policy.watch().unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let saved = engine
        .save_step_output(&run, step_id, "slow", "1".to_owned())
        .await;
    assert!(
        matches!(saved, Err(Error::WorkflowCancelled(_))),
        "{saved:?}"
    );
});
