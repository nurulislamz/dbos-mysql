use std::sync::Arc;
use std::time::{Duration, Instant};

use dwos_core::{ActiveRun, Engine, EngineConfig, Error, LeaseLoss, Outcome, Run, Status};
use tokio::task::JoinHandle;

#[macro_use]
mod common;
use common::*;

async fn engine(backend: Backend) -> (TestDb, Engine) {
    let db = TestDb::migrated(backend).await;
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
    tokio::time::sleep(Duration::from_millis(80)).await;
    let second = engine.sleep(&run, step_id, 2.0).await.unwrap();
    assert!(second < first, "first {first:?} second {second:?}");
    assert!(second <= Duration::from_millis(1950), "{second:?}");
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
    tokio::time::sleep(Duration::from_millis(40)).await;
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

// Keepers on `LONG` renew every 200 seconds, so inside this bound only a poke or the poll can end a wait.
const BOUND: Duration = Duration::from_secs(30);
const FOREVER: Duration = Duration::from_secs(3600);

async fn begin_run(engine: &Engine, id: &str) -> Arc<Run> {
    match engine
        .begin(dwos_core::Begin {
            name: "wf",
            input: "{}",
            max_recovery_attempts: 100,
            workflow_id: Some(id),
            parent: None,
            timeout_ms: None,
            ignore_propagated_deadline: false,
        })
        .await
        .unwrap()
    {
        dwos_core::Begun::Run(run) => Arc::new(run),
        other => panic!("expected to run, got {other:?}"),
    }
}

fn sleeper(
    engine: &Engine,
    run: &Arc<Run>,
    active: &Arc<ActiveRun>,
    left: Duration,
) -> JoinHandle<Option<LeaseLoss>> {
    let (engine, run, active) = (engine.clone(), run.clone(), active.clone());
    tokio::spawn(async move { engine.wait_sleep(&run, &active, left).await })
}

async fn woken(sleeper: JoinHandle<Option<LeaseLoss>>) -> Option<LeaseLoss> {
    tokio::time::timeout(BOUND, sleeper)
        .await
        .expect("the sleeper was not woken")
        .unwrap()
}

all_dbs!(a_cancel_from_this_engine_pokes_the_keeper, backend, {
    let (_db, engine) = engine(backend).await;
    let run = begin_run(&engine, "a").await;
    let active = engine.keep_lease(&run);
    engine.cancel_workflow("a").await.unwrap();
    let loss = tokio::time::timeout(BOUND, active.lost())
        .await
        .expect("no poll runs here, so only the poke can report this");
    assert_eq!(loss, LeaseLoss::TakenOver);
});

all_dbs!(a_cancel_from_this_engine_wakes_a_sleeper, backend, {
    let (_db, engine) = engine(backend).await;
    let run = begin_run(&engine, "a").await;
    let active = Arc::new(engine.keep_lease(&run));
    let left = engine
        .sleep(&run, run.next_step_id(), 3600.0)
        .await
        .unwrap();
    let sleeping = sleeper(&engine, &run, &active, left);
    engine.cancel_workflow("a").await.unwrap();
    assert_eq!(woken(sleeping).await, Some(LeaseLoss::TakenOver));
});

all_dbs!(a_cancel_from_a_second_engine_wakes_a_sleeper, backend, {
    let (db, engine) = engine(backend).await;
    let other = Engine::connect(&db.url, EngineConfig::default())
        .await
        .unwrap();
    let run = begin_run(&engine, "a").await;
    let active = Arc::new(engine.keep_lease(&run));
    other.cancel_workflow("a").await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(600), active.lost())
            .await
            .is_err(),
        "the other engine cannot poke this keeper, so nothing has noticed yet"
    );
    let started = Instant::now();
    let sleeping = sleeper(&engine, &run, &active, FOREVER);
    assert_eq!(woken(sleeping).await, Some(LeaseLoss::TakenOver));
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
});

all_dbs!(a_sleep_ends_on_time_and_its_poll_writes_nothing, backend, {
    let (_db, engine) = engine(backend).await;
    let run = begin_run(&engine, "a").await;
    let active = Arc::new(engine.keep_lease(&run));
    let before = engine.get_status("a").await.unwrap().updated_at_ms;
    let started = Instant::now();
    let left = Duration::from_millis(700);
    assert_eq!(woken(sleeper(&engine, &run, &active, left)).await, None);
    assert!(started.elapsed() >= left);
    assert_eq!(engine.get_status("a").await.unwrap().updated_at_ms, before);
});

all_dbs!(nothing_left_to_sleep_returns_at_once, backend, {
    let (_db, engine) = engine(backend).await;
    let run = begin_run(&engine, "a").await;
    let active = Arc::new(engine.keep_lease(&run));
    assert_eq!(
        woken(sleeper(&engine, &run, &active, Duration::ZERO)).await,
        None
    );
});

all_dbs!(
    a_resumed_workflow_sleeps_on_and_only_the_old_run_stops,
    backend,
    {
        let (_db, engine) = engine(backend).await;
        let run = begin_run(&engine, "a").await;
        let active = Arc::new(engine.keep_lease(&run));
        let step_id = run.next_step_id();
        let first = engine.sleep(&run, step_id, 1.5).await.unwrap();
        let old = sleeper(&engine, &run, &active, FOREVER);
        engine.cancel_workflow("a").await.unwrap();
        assert_eq!(woken(old).await, Some(LeaseLoss::TakenOver));

        let claim = engine
            .resume_workflow("a", &names())
            .await
            .unwrap()
            .unwrap();
        let resumed = Arc::new(Run::new(
            claim.lease,
            claim.serialization,
            claim.deadline_epoch_ms,
        ));
        let keeping = Arc::new(engine.keep_lease(&resumed));
        let left = engine.sleep(&resumed, step_id, 60.0).await.unwrap();
        assert!(left <= first, "the wake time is kept: {first:?}, {left:?}");
        assert_eq!(
            woken(sleeper(&engine, &resumed, &keeping, left)).await,
            None
        );
        assert_eq!(engine.list_steps("a").await.unwrap().len(), 1);
    }
);

all_dbs!(
    a_cancel_and_resume_elsewhere_stops_the_old_sleeper_only,
    backend,
    {
        let (db, engine) = engine(backend).await;
        let other = Engine::connect(
            &db.url,
            EngineConfig {
                lease: LONG,
                pickle: false,
                ..EngineConfig::default()
            },
        )
        .await
        .unwrap();
        let run = begin_run(&engine, "a").await;
        let active = Arc::new(engine.keep_lease(&run));
        let old = sleeper(&engine, &run, &active, FOREVER);
        other.cancel_workflow("a").await.unwrap();
        let claim = other.resume_workflow("a", &names()).await.unwrap().unwrap();
        let resumed = Run::new(claim.lease, claim.serialization, claim.deadline_epoch_ms);
        assert_eq!(woken(old).await, Some(LeaseLoss::TakenOver));
        let step_id = resumed.next_step_id();
        other
            .save_step_output(&resumed, step_id, "step", "1".to_owned())
            .await
            .unwrap();
        assert!(engine.recover_now(&names()).await.unwrap().is_empty());
        assert_eq!(
            engine.get_status("a").await.unwrap().status,
            Status::Pending
        );
    }
);

all_dbs!(a_cancel_reaches_every_local_run_of_a_workflow, backend, {
    let (db, engine) = engine(backend).await;
    let run = begin_run(&engine, "a").await;
    let active = Arc::new(engine.keep_lease(&run));
    let store = db.open().await;
    sqlx::query("UPDATE workflow_status SET claim_after = 0 WHERE workflow_uuid = 'a'")
        .execute(store.pool())
        .await
        .unwrap();
    let claim = engine.recover_now(&names()).await.unwrap().remove(0);
    let newer = Arc::new(Run::new(
        claim.lease,
        claim.serialization,
        claim.deadline_epoch_ms,
    ));
    let replacing = Arc::new(engine.keep_lease(&newer));
    let (old, new) = (
        sleeper(&engine, &run, &active, FOREVER),
        sleeper(&engine, &newer, &replacing, FOREVER),
    );
    engine.cancel_workflow("a").await.unwrap();
    assert_eq!(woken(new).await, Some(LeaseLoss::TakenOver));
    assert_eq!(woken(old).await, Some(LeaseLoss::TakenOver));
});

all_dbs!(stopping_one_run_leaves_the_other_reachable, backend, {
    let (db, engine) = engine(backend).await;
    let run = begin_run(&engine, "a").await;
    let stale = engine.keep_lease(&run);
    let store = db.open().await;
    sqlx::query("UPDATE workflow_status SET claim_after = 0 WHERE workflow_uuid = 'a'")
        .execute(store.pool())
        .await
        .unwrap();
    let claim = engine.recover_now(&names()).await.unwrap().remove(0);
    let newer = Run::new(claim.lease, claim.serialization, claim.deadline_epoch_ms);
    let active = engine.keep_lease(&newer);
    stale.stop();
    engine.cancel_workflow("a").await.unwrap();
    let loss = tokio::time::timeout(BOUND, active.lost()).await.unwrap();
    assert_eq!(loss, LeaseLoss::TakenOver);
});
