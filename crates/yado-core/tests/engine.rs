use std::sync::Arc;
use std::time::Duration;

use yado_core::{
    Begin, Begun, Ended, Engine, EngineConfig, Error, FinalOutcome, Finished, Outcome, ParentStep,
    ReusePolicy, Run, Status, StepDecision, StepPolicy, StepRecord, WorkflowOptions,
    recovery_limit, recovery_limit_of,
};

#[macro_use]
mod common;
use common::*;

fn config() -> EngineConfig {
    EngineConfig {
        lease: LONG,
        recovery_interval: Duration::from_millis(20),
        ..EngineConfig::default()
    }
}

async fn engine(db: &TestDb, config: EngineConfig) -> Engine {
    Engine::connect(&db.url, config).await.unwrap()
}

fn begin<'a>(id: Option<&'a str>) -> Begin<'a> {
    Begin {
        name: "wf",
        input: "{}",
        max_recovery_attempts: 100,
        workflow_id: id,
        parent: None,
        timeout_ms: None,
        ignore_propagated_deadline: false,
    }
}

async fn begin_run(engine: &Engine, id: &str) -> Run {
    match engine.begin(begin(Some(id))).await.unwrap() {
        Begun::Run(run) => run,
        other => panic!("expected to run, got {other:?}"),
    }
}

fn expired() -> EngineConfig {
    EngineConfig {
        lease: EXPIRED,
        ..config()
    }
}

all_dbs!(begin_runs_a_new_workflow_with_the_given_id, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let run = begin_run(&engine, "a").await;
    assert_eq!(run.workflow_id(), "a");
});

all_dbs!(begin_generates_an_id_when_none_is_given, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    for id in [None, Some("")] {
        let Begun::Run(run) = engine.begin(begin(id)).await.unwrap() else {
            panic!("expected to run");
        };
        assert_eq!(run.workflow_id().len(), 36);
    }
});

all_dbs!(begin_attaches_to_a_running_workflow, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    begin_run(&engine, "a").await;
    let begun = engine.begin(begin(Some("a"))).await.unwrap();
    assert!(matches!(begun, Begun::Attach(id) if id == "a"));
});

all_dbs!(begin_returns_a_stored_success, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let run = begin_run(&engine, "a").await;
    engine
        .finish(&run, Outcome::Success("1".to_owned()))
        .await
        .unwrap();
    let Begun::Stored(result) = engine.begin(begin(Some("a"))).await.unwrap() else {
        panic!("expected the stored result");
    };
    assert_eq!(result.outcome, FinalOutcome::Success("1".to_owned()));
    assert_eq!(result.name, "wf");
});

all_dbs!(begin_returns_a_stored_error, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let run = begin_run(&engine, "a").await;
    engine
        .finish(&run, Outcome::Error("boom".to_owned()))
        .await
        .unwrap();
    let Begun::Stored(result) = engine.begin(begin(Some("a"))).await.unwrap() else {
        panic!("expected the stored result");
    };
    assert_eq!(result.outcome, FinalOutcome::Error("boom".to_owned()));
});

all_dbs!(begin_with_another_name_conflicts, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    begin_run(&engine, "a").await;
    let mut other = begin(Some("a"));
    other.name = "other";
    let error = engine.begin(other).await.unwrap_err();
    assert!(
        matches!(error, Error::ConflictingWorkflow { .. }),
        "{error:?}"
    );
});

all_dbs!(child_id_comes_from_the_parent_and_step, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let parent = begin_run(&engine, "p").await;
    let step_id = parent.next_step_id();
    let mut child = begin(None);
    child.parent = Some(ParentStep {
        run: &parent,
        step_id,
    });
    let Begun::Run(run) = engine.begin(child).await.unwrap() else {
        panic!("expected to run the child");
    };
    assert_eq!(run.workflow_id(), "p-1");

    let steps = engine.list_steps("p").await.unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].record.child_workflow_id.as_deref(), Some("p-1"));
    assert!(steps[0].completed_at_epoch_ms > 0);
    let child_row = engine.get_status("p-1").await.unwrap();
    assert_eq!(child_row.parent_workflow_id.as_deref(), Some("p"));
});

all_dbs!(
    child_uses_an_explicit_id_and_replays_the_recorded_one,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db, config()).await;
        let parent = begin_run(&engine, "p").await;
        let parent_step = ParentStep {
            run: &parent,
            step_id: 1,
        };
        let mut child = begin(Some("chosen"));
        child.parent = Some(parent_step);
        let Begun::Run(run) = engine.begin(child).await.unwrap() else {
            panic!("expected to run the child");
        };
        assert_eq!(run.workflow_id(), "chosen");

        let mut replay = begin(Some("different"));
        replay.parent = Some(parent_step);
        let begun = engine.begin(replay).await.unwrap();
        assert!(matches!(begun, Begun::Attach(id) if id == "chosen"));
    }
);

all_dbs!(a_replayed_child_start_ignores_the_reject_policy, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let parent = begin_run(&engine, "p").await;
    let reject = WorkflowOptions {
        reuse: ReusePolicy::Reject,
        ..WorkflowOptions::default()
    };
    let mut child = begin(Some("chosen"));
    child.parent = Some(ParentStep {
        run: &parent,
        step_id: 1,
    });
    assert!(matches!(
        engine.begin_with(child, &reject).await.unwrap(),
        Begun::Run(_)
    ));
    // The parent replays the same call: its saved step already started this child.
    let begun = engine.begin_with(child, &reject).await.unwrap();
    assert!(matches!(begun, Begun::Attach(id) if id == "chosen"));
});

all_dbs!(stale_parent_cannot_start_a_child, backend, {
    let db = TestDb::migrated(backend).await;
    let stale_engine = engine(&db, expired()).await;
    let parent = begin_run(&stale_engine, "p").await;
    stale_engine.recover_now(&names()).await.unwrap();

    let mut child = begin(None);
    child.parent = Some(ParentStep {
        run: &parent,
        step_id: 1,
    });
    let error = stale_engine.begin(child).await.unwrap_err();
    assert!(matches!(error, Error::LeaseLost(_)), "{error:?}");
    assert!(
        stale_engine
            .store()
            .get_workflow("p-1")
            .await
            .unwrap()
            .is_none()
    );
});

all_dbs!(child_slot_holding_a_step_is_a_mismatch, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let parent = begin_run(&engine, "p").await;
    engine
        .save_step_output(&parent, 1, "wf", "1".to_owned())
        .await
        .unwrap();
    let mut child = begin(None);
    child.parent = Some(ParentStep {
        run: &parent,
        step_id: 1,
    });
    let error = engine.begin(child).await.unwrap_err();
    assert!(matches!(error, Error::StepKindMismatch { .. }), "{error:?}");
});

all_dbs!(check_step_says_run_when_nothing_is_saved, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let run = begin_run(&engine, "a").await;
    assert_eq!(
        engine.check_step(&run, 1, "step").await.unwrap(),
        StepDecision::Run
    );
});

all_dbs!(check_step_replays_output_and_errors, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let run = begin_run(&engine, "a").await;
    engine
        .save_step_output(&run, 1, "good", "1".to_owned())
        .await
        .unwrap();
    engine
        .save_step_error(&run, 2, "bad", "boom".to_owned())
        .await
        .unwrap();
    assert_eq!(
        engine.check_step(&run, 1, "good").await.unwrap(),
        StepDecision::ReplayOutput("1".to_owned())
    );
    assert_eq!(
        engine.check_step(&run, 2, "bad").await.unwrap(),
        StepDecision::ReplayError("boom".to_owned())
    );
});

all_dbs!(check_step_rejects_a_changed_name, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let run = begin_run(&engine, "a").await;
    engine
        .save_step_output(&run, 1, "step", "1".to_owned())
        .await
        .unwrap();
    let error = engine.check_step(&run, 1, "renamed").await.unwrap_err();
    assert!(matches!(error, Error::UnexpectedStep { .. }), "{error:?}");
});

all_dbs!(check_step_rejects_a_child_pointer, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let parent = begin_run(&engine, "p").await;
    let mut child = begin(None);
    child.parent = Some(ParentStep {
        run: &parent,
        step_id: 1,
    });
    engine.begin(child).await.unwrap();
    let error = engine.check_step(&parent, 1, "wf").await.unwrap_err();
    assert!(matches!(error, Error::StepKindMismatch { .. }), "{error:?}");
});

all_dbs!(saving_a_step_as_a_stale_owner_fails, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, expired()).await;
    let run = begin_run(&engine, "a").await;
    engine.recover_now(&names()).await.unwrap();
    let error = engine
        .save_step_output(&run, 1, "step", "1".to_owned())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::LeaseLost(_)), "{error:?}");
    let record: Option<StepRecord> = engine.store().load_step("a", 1, "step").await.unwrap();
    assert_eq!(record, None);
});

all_dbs!(await_result_waits_for_the_owner, backend, {
    let db = Arc::new(TestDb::migrated(backend).await);
    let engine = engine(&db, config()).await;
    let run = begin_run(&engine, "a").await;

    let waiter = engine.clone();
    let waiting = tokio::spawn(async move { waiter.await_result("a").await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!waiting.is_finished());

    engine
        .finish(&run, Outcome::Success("1".to_owned()))
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(10), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.outcome, FinalOutcome::Success("1".to_owned()));
});

all_dbs!(await_result_on_a_missing_workflow_fails, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let error = engine.await_result("nope").await.unwrap_err();
    assert!(matches!(error, Error::WorkflowNotFound(_)), "{error:?}");
});

all_dbs!(await_result_reports_too_many_recoveries, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, expired()).await;
    let mut new = begin(Some("a"));
    new.max_recovery_attempts = 0;
    engine.begin(new).await.unwrap();
    assert!(engine.recover_now(&names()).await.unwrap().is_empty());
    let result = engine.await_result("a").await.unwrap();
    assert_eq!(result.outcome, FinalOutcome::MaxRecoveryAttemptsExceeded(0));
});

all_dbs!(finish_is_accepted_for_the_owner, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let run = begin_run(&engine, "a").await;
    let finished = engine
        .finish(&run, Outcome::Success("1".to_owned()))
        .await
        .unwrap();
    assert_eq!(finished, Finished::Accepted);
});

all_dbs!(
    finish_after_takeover_returns_the_new_owners_result,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db, expired()).await;
        let stale = begin_run(&engine, "a").await;
        let claim = engine.recover_now(&names()).await.unwrap().remove(0);
        let owner = Run::new(claim.lease, claim.serialization, claim.deadline_epoch_ms);

        let late = engine.clone();
        let finishing = tokio::spawn(async move {
            late.finish(&stale, Outcome::Success("\"stale\"".to_owned()))
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!finishing.is_finished());

        engine
            .finish(&owner, Outcome::Success("\"owner\"".to_owned()))
            .await
            .unwrap();
        let Finished::Superseded(result) = finishing.await.unwrap().unwrap() else {
            panic!("the stale owner's result should be discarded");
        };
        assert_eq!(
            result.outcome,
            FinalOutcome::Success("\"owner\"".to_owned())
        );
    }
);

all_dbs!(release_ignores_a_lost_lease, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, expired()).await;
    let stale = begin_run(&engine, "a").await;
    engine.recover_now(&names()).await.unwrap();
    engine.release(&stale).await.unwrap();
    let claim = engine.store().get_workflow("a").await.unwrap().unwrap();
    assert_eq!(claim.recovery_attempts, 2);
});

all_dbs!(lost_run_waits_for_the_new_owner, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, expired()).await;
    let stale = begin_run(&engine, "a").await;
    let claim = engine.recover_now(&names()).await.unwrap().remove(0);
    let owner = Run::new(claim.lease, claim.serialization, claim.deadline_epoch_ms);
    let waiter = engine.clone();
    let waiting = tokio::spawn(async move { waiter.conclude(&stale, Ended::LeaseLost).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!waiting.is_finished());
    engine
        .finish(&owner, Outcome::Success("owner".to_owned()))
        .await
        .unwrap();
    let Finished::Superseded(result) = waiting.await.unwrap().unwrap() else {
        panic!("the lost run should return the new owner's result");
    };
    assert_eq!(result.outcome, FinalOutcome::Success("owner".to_owned()));
});

all_dbs!(recovery_tick_returns_crashed_work, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, expired()).await;
    begin_run(&engine, "a").await;
    let claims = engine.recovery_tick(&names()).await;
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].recovery_attempts, 2);
});

all_dbs!(recovery_tick_waits_out_its_interval_when_idle, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(
        &db,
        EngineConfig {
            recovery_interval: Duration::from_millis(40),
            ..config()
        },
    )
    .await;
    let started = std::time::Instant::now();
    assert!(engine.recovery_tick(&names()).await.is_empty());
    assert!(
        started.elapsed() >= Duration::from_millis(20),
        "{:?}",
        started.elapsed()
    );
});

all_dbs!(recovery_tick_respects_the_concurrency_limit, backend, {
    let db = TestDb::migrated(backend).await;
    let limited = EngineConfig {
        max_concurrent_recoveries: 2,
        ..expired()
    };
    let engine = engine(&db, limited).await;
    let active = begin_run(&engine, "active").await;
    let keeper = engine.keep_lease(&active);
    assert_eq!(engine.active_runs(), 1);
    for id in ["a", "b", "c"] {
        begin_run(&engine, id).await;
    }

    assert_eq!(engine.recovery_tick(&names()).await.len(), 1);

    let _second = engine.keep_lease(&active);
    assert!(engine.recovery_tick(&names()).await.is_empty());

    keeper.stop();
    keeper.stop();
    assert_eq!(engine.active_runs(), 1);
    assert_eq!(engine.recovery_tick(&names()).await.len(), 1);
});

all_dbs!(keep_lease_counts_runs_until_stopped, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let run = begin_run(&engine, "a").await;
    let keeper = engine.keep_lease(&run);
    assert_eq!(engine.active_runs(), 1);
    drop(keeper);
    assert_eq!(engine.active_runs(), 0);
});

all_dbs!(keep_lease_reports_a_takeover, backend, {
    let db = TestDb::migrated(backend).await;
    let short = EngineConfig {
        lease: Duration::from_millis(60),
        ..config()
    };
    let engine = engine(&db, short.clone()).await;
    let stale = begin_run(&engine, "a").await;
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(engine.recover_now(&names()).await.unwrap().len(), 1);

    let keeper = engine.keep_lease(&stale);
    let loss = tokio::time::timeout(Duration::from_secs(5), keeper.lost())
        .await
        .unwrap();
    assert_eq!(loss.as_str(), "taken_over");
});

all_dbs!(migrations_can_be_turned_off, backend, {
    let db = TestDb::create(backend).await;
    let engine = engine(
        &db,
        EngineConfig {
            run_migrations: false,
            ..config()
        },
    )
    .await;
    assert!(matches!(
        engine.get_status("a").await,
        Err(Error::Database(_))
    ));
});

#[test]
fn step_attempts_retry_until_the_limit_then_give_up() {
    let mut attempts = StepPolicy::new(true, 1.0, 3, 2.0).unwrap().attempts();
    assert_eq!(attempts.status(), (Some(0), Some(3)));
    assert_eq!(attempts.failed(true), Some(Duration::from_secs(1)));
    assert_eq!(attempts.status(), (Some(1), Some(3)));
    assert_eq!(attempts.failed(true), Some(Duration::from_secs(2)));
    assert_eq!(attempts.status(), (Some(2), Some(3)));
    assert_eq!(attempts.failed(true), None);
}

#[test]
fn only_a_retrying_step_wraps_its_failures() {
    let retrying = StepPolicy::new(true, 1.0, 3, 2.0).unwrap();
    let single = StepPolicy::new(false, 1.0, 3, 2.0).unwrap();
    assert!(retrying.wraps_failures());
    assert!(!single.wraps_failures());
    assert_eq!(single.attempts().failed(true), None);
}

#[test]
fn an_unlimited_recovery_limit_round_trips() {
    assert_eq!(recovery_limit_of(recovery_limit(None)), None);
    assert_eq!(recovery_limit_of(recovery_limit(Some(5))), Some(5));
}

all_dbs!(an_interrupted_workflow_is_released_for_recovery, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = Engine::connect(&db.url, expired()).await.unwrap();
    let run = begin_run(&engine, "a").await;
    assert_eq!(
        engine.conclude(&run, Ended::Interrupted).await.unwrap(),
        Finished::Accepted
    );
    let state = engine.get_status("a").await.unwrap();
    assert_eq!(state.status, Status::Pending);
    assert_eq!(
        state.recovery_attempts, 0,
        "an interruption uses no attempt"
    );
    assert_eq!(engine.recover_now(&names()).await.unwrap().len(), 1);
});

all_dbs!(json_recovery_leaves_pickled_workflows_unclaimed, backend, {
    let db = TestDb::migrated(backend).await;
    let pickled = engine(
        &db,
        EngineConfig {
            pickle: true,
            lease: EXPIRED,
            ..config()
        },
    )
    .await;
    begin_run(&pickled, "a").await;
    let json = Engine::connect(
        &db.url,
        EngineConfig {
            pickle: false,
            run_migrations: false,
            lease: EXPIRED,
            ..config()
        },
    )
    .await
    .unwrap();
    assert!(json.recovery_tick(&names()).await.is_empty());
    let state = json.get_status("a").await.unwrap();
    assert_eq!(state.status, Status::Pending);
    assert_eq!(state.recovery_attempts, 1, "no recovery attempt was used");
    assert_eq!(json.store().count_due_pickled().await.unwrap(), 1);
});

all_dbs!(read_only_engine_can_list_an_existing_database, backend, {
    let db = TestDb::migrated(backend).await;
    let writer = engine(&db, config()).await;
    begin_run(&writer, "a").await;
    let reader = Engine::connect(
        &db.url,
        EngineConfig {
            read_only: true,
            run_migrations: false,
            pickle: false,
            ..config()
        },
    )
    .await
    .unwrap();
    let rows = reader
        .list_workflows(&yado_core::WorkflowFilter::default())
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].workflow_id, "a");
});

all_dbs!(an_ended_workflow_stores_its_outcome, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = Engine::connect(&db.url, config()).await.unwrap();
    let run = begin_run(&engine, "a").await;
    engine
        .conclude(&run, Ended::Success("1".to_owned()))
        .await
        .unwrap();
    assert_eq!(
        engine.get_status("a").await.unwrap().status,
        Status::Success
    );
});
