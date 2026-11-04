use std::time::Duration;

use dwos_core::{Engine, EngineConfig, Error, Outcome};

#[macro_use]
mod common;
use common::*;

async fn engine(backend: Backend) -> (TestDb, Engine) {
    let db = TestDb::create(backend).await;
    let engine = Engine::connect(
        &db.url,
        EngineConfig {
            lease: LONG,
            pickle: false,
            ..EngineConfig::default()
        },
    )
    .await
    .unwrap();
    (db, engine)
}

all_dbs!(
    sleep_records_a_wake_time_and_returns_the_time_left,
    backend,
    {
        let (_db, engine) = engine(backend).await;
        let run = match engine
            .begin(dwos_core::Begin {
                name: "wf",
                input: "{}",
                max_recovery_attempts: 100,
                workflow_id: Some("a"),
                parent: None,
                timeout_ms: None,
                ignore_propagated_deadline: false,
            })
            .await
            .unwrap()
        {
            dwos_core::Begun::Run(run) => run,
            other => panic!("expected to run, got {other:?}"),
        };
        let step_id = run.next_step_id();
        let left = engine.sleep(&run, step_id, 1.5).await.unwrap();
        assert!(
            left >= Duration::from_millis(1400) && left <= Duration::from_millis(1500),
            "{left:?}"
        );
        let steps = engine.list_steps("a").await.unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].step_name, "DBOS.sleep");
        assert!(
            steps[0]
                .record
                .output
                .as_ref()
                .unwrap()
                .parse::<i64>()
                .is_ok()
        );
    }
);

all_dbs!(sleep_replay_waits_only_the_time_left, backend, {
    let (_db, engine) = engine(backend).await;
    let run = match engine
        .begin(dwos_core::Begin {
            name: "wf",
            input: "{}",
            max_recovery_attempts: 100,
            workflow_id: Some("a"),
            parent: None,
            timeout_ms: None,
            ignore_propagated_deadline: false,
        })
        .await
        .unwrap()
    {
        dwos_core::Begun::Run(run) => run,
        other => panic!("expected to run, got {other:?}"),
    };
    let step_id = run.next_step_id();
    let first = engine.sleep(&run, step_id, 2.0).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let second = engine.sleep(&run, step_id, 2.0).await.unwrap();
    assert!(second < first, "first {first:?} second {second:?}");
    assert!(second <= Duration::from_millis(1800), "{second:?}");
    assert_eq!(engine.list_steps("a").await.unwrap().len(), 1);
});

all_dbs!(a_zero_or_negative_sleep_does_not_record_a_step, backend, {
    let (_db, engine) = engine(backend).await;
    let run = match engine
        .begin(dwos_core::Begin {
            name: "wf",
            input: "{}",
            max_recovery_attempts: 100,
            workflow_id: Some("a"),
            parent: None,
            timeout_ms: None,
            ignore_propagated_deadline: false,
        })
        .await
        .unwrap()
    {
        dwos_core::Begun::Run(run) => run,
        other => panic!("expected to run, got {other:?}"),
    };
    let step_id = run.next_step_id();
    assert_eq!(
        engine.sleep(&run, step_id, 0.0).await.unwrap(),
        Duration::ZERO
    );
    assert_eq!(
        engine.sleep(&run, step_id, -1.0).await.unwrap(),
        Duration::ZERO
    );
    assert!(engine.list_steps("a").await.unwrap().is_empty());
});

all_dbs!(sleep_on_a_cancelled_workflow_is_cancelled, backend, {
    let (_db, engine) = engine(backend).await;
    let run = match engine
        .begin(dwos_core::Begin {
            name: "wf",
            input: "{}",
            max_recovery_attempts: 100,
            workflow_id: Some("a"),
            parent: None,
            timeout_ms: Some(1),
            ignore_propagated_deadline: false,
        })
        .await
        .unwrap()
    {
        dwos_core::Begun::Run(run) => run,
        other => panic!("expected to run, got {other:?}"),
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    let step_id = run.next_step_id();
    let error = engine.sleep(&run, step_id, 5.0).await.unwrap_err();
    assert!(matches!(error, Error::WorkflowCancelled(_)), "{error:?}");
    engine
        .finish(&run, Outcome::Success("1".to_owned()))
        .await
        .unwrap();
    let state = engine.get_status("a").await.unwrap();
    assert_eq!(state.status, dwos_core::Status::Cancelled);
});

all_dbs!(sleep_keeps_its_wake_time_after_recovery, backend, {
    let (_db, engine) = engine(backend).await;
    let run = match engine
        .begin(dwos_core::Begin {
            name: "wf",
            input: "{}",
            max_recovery_attempts: 100,
            workflow_id: Some("a"),
            parent: None,
            timeout_ms: None,
            ignore_propagated_deadline: false,
        })
        .await
        .unwrap()
    {
        dwos_core::Begun::Run(run) => run,
        other => panic!("expected to run, got {other:?}"),
    };
    let first = engine.sleep(&run, 1, 2.0).await.unwrap();
    engine.release(&run).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let claimed = engine.recover_now(&names()).await.unwrap().remove(0);
    let recovered = dwos_core::Run::new(
        claimed.lease,
        claimed.serialization,
        claimed.deadline_epoch_ms,
    );
    let left = engine.sleep(&recovered, 1, 10.0).await.unwrap();
    assert!(left < first, "first {first:?}, recovered {left:?}");
    assert_eq!(engine.list_steps("a").await.unwrap().len(), 1);
});
