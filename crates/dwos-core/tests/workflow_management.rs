use std::time::Duration;

use dwos_core::{
    Begin, Begun, Engine, EngineConfig, Error, FinalOutcome, Outcome, Run, Status, StepDecision,
    WorkflowFilter,
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

all_dbs!(cancel_marks_a_pending_workflow_cancelled, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let run = begin_run(&engine, "a").await;
    engine.cancel_workflow("a").await.unwrap();
    let row = engine.get_status("a").await.unwrap();
    assert_eq!(row.status, Status::Cancelled);
    assert!(row.completed_at_ms.is_some());
    let error = engine
        .finish(&run, Outcome::Success("1".to_owned()))
        .await
        .unwrap();
    let dwos_core::Finished::Superseded(result) = error else {
        panic!("the cancelled owner must not store a success");
    };
    assert_eq!(result.outcome, FinalOutcome::Cancelled);
});

all_dbs!(cancel_of_success_or_error_is_a_noop, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let run = begin_run(&engine, "ok").await;
    engine
        .finish(&run, Outcome::Success("1".to_owned()))
        .await
        .unwrap();
    engine.cancel_workflow("ok").await.unwrap();
    assert_eq!(
        engine.get_status("ok").await.unwrap().status,
        Status::Success
    );

    let run = begin_run(&engine, "bad").await;
    engine
        .finish(&run, Outcome::Error("boom".to_owned()))
        .await
        .unwrap();
    engine.cancel_workflow("bad").await.unwrap();
    assert_eq!(
        engine.get_status("bad").await.unwrap().status,
        Status::Error
    );
});

all_dbs!(cancel_of_a_missing_workflow_is_a_noop, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    engine.cancel_workflow("missing").await.unwrap();
});

all_dbs!(check_step_stops_once_the_workflow_is_cancelled, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let run = begin_run(&engine, "a").await;
    engine
        .save_step_output(&run, 1, "one", "1".to_owned())
        .await
        .unwrap();
    engine.cancel_workflow("a").await.unwrap();
    let error = engine.check_step(&run, 2, "two").await.unwrap_err();
    assert!(
        matches!(error, Error::WorkflowCancelled(ref id) if id == "a"),
        "{error:?}"
    );
    let error = engine.check_step(&run, 1, "one").await.unwrap_err();
    assert!(matches!(error, Error::WorkflowCancelled(_)), "{error:?}");
});

all_dbs!(invalid_step_names_are_rejected_before_execution, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let run = begin_run(&engine, "a").await;
    for name in ["bad\0step".to_owned(), "x".repeat(256)] {
        for error in [
            engine.check_step(&run, 1, &name).await.unwrap_err(),
            engine
                .store()
                .load_step("missing", 1, &name)
                .await
                .unwrap_err(),
        ] {
            assert!(
                matches!(
                    error,
                    Error::ContainsNul { field: "step name" }
                        | Error::NameTooLong { field: "step name" }
                ),
                "{error:?}"
            );
        }
    }
    assert_eq!(
        engine.check_step(&run, 1, &"é".repeat(255)).await.unwrap(),
        StepDecision::Run
    );
});

all_dbs!(resume_makes_a_cancelled_workflow_claimable, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    begin_run(&engine, "a").await;
    engine.cancel_workflow("a").await.unwrap();
    let claimed = engine.resume_workflow("a", &names()).await.unwrap();
    let claimed = claimed.expect("this process can run the resumed workflow");
    assert_eq!(claimed.lease.workflow_id, "a");
    assert_eq!(claimed.recovery_attempts, 1);
    let row = engine.get_status("a").await.unwrap();
    assert_eq!(row.status, Status::Pending);
    assert_eq!(row.recovery_attempts, 1);
    assert!(row.completed_at_ms.is_none());
    let run = Run::new(
        claimed.lease,
        claimed.serialization,
        claimed.deadline_epoch_ms,
    );
    assert_eq!(
        engine.check_step(&run, 1, "step").await.unwrap(),
        StepDecision::Run
    );
});

all_dbs!(resume_resets_a_dead_lettered_workflow, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(
        &db,
        EngineConfig {
            lease: EXPIRED,
            ..config()
        },
    )
    .await;
    let mut new = begin(Some("a"));
    new.max_recovery_attempts = 0;
    engine.begin(new).await.unwrap();
    assert!(engine.recover_now(&names()).await.unwrap().is_empty());
    assert_eq!(
        engine.get_status("a").await.unwrap().status,
        Status::MaxRecoveryAttemptsExceeded
    );
    let claimed = engine.resume_workflow("a", &names()).await.unwrap();
    assert!(claimed.is_some());
    let row = engine.get_status("a").await.unwrap();
    assert_eq!(row.status, Status::Pending);
    assert_eq!(row.recovery_attempts, 1);
});

all_dbs!(resume_of_success_is_a_noop, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let run = begin_run(&engine, "a").await;
    engine
        .finish(&run, Outcome::Success("1".to_owned()))
        .await
        .unwrap();
    assert!(
        engine
            .resume_workflow("a", &names())
            .await
            .unwrap()
            .is_none()
    );
    let row = engine.get_status("a").await.unwrap();
    assert_eq!(row.status, Status::Success);
    assert_eq!(row.output.as_deref(), Some("1"));
});

all_dbs!(resume_of_a_missing_workflow_fails, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let error = engine
        .resume_workflow("missing", &names())
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::WorkflowNotFound(ref id) if id == "missing"),
        "{error:?}"
    );
});

all_dbs!(
    resume_without_the_function_leaves_the_row_claimable,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db, config()).await;
        begin_run(&engine, "a").await;
        engine.cancel_workflow("a").await.unwrap();
        assert!(engine.resume_workflow("a", &[]).await.unwrap().is_none());
        let row = engine.get_status("a").await.unwrap();
        assert_eq!(row.status, Status::Pending);
        assert_eq!(row.recovery_attempts, 0);
        let claimed = engine.recover_now(&names()).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].lease.workflow_id, "a");
    }
);

all_dbs!(list_workflows_applies_filters, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    for id in ["a", "b", "c", "other"] {
        let run = begin_run(&engine, id).await;
        engine
            .finish(&run, Outcome::Success("1".to_owned()))
            .await
            .unwrap();
    }
    let run = begin_run(&engine, "pending").await;
    drop(run);

    let all = engine
        .list_workflows(&WorkflowFilter::default())
        .await
        .unwrap();
    assert_eq!(all.len(), 5);

    let by_id = engine
        .list_workflows(&WorkflowFilter {
            workflow_ids: Some(vec!["a".to_owned(), "missing".to_owned()]),
            ..WorkflowFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(by_id.len(), 1);
    assert_eq!(by_id[0].workflow_id, "a");

    let empty_ids = engine
        .list_workflows(&WorkflowFilter {
            workflow_ids: Some(Vec::new()),
            ..WorkflowFilter::default()
        })
        .await
        .unwrap();
    assert!(empty_ids.is_empty());

    let pending = engine
        .list_workflows(&WorkflowFilter {
            statuses: Some(vec![Status::Pending]),
            ..WorkflowFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].workflow_id, "pending");

    let named = engine
        .list_workflows(&WorkflowFilter {
            names: Some(vec!["nope".to_owned()]),
            ..WorkflowFilter::default()
        })
        .await
        .unwrap();
    assert!(named.is_empty());

    let prefix = engine
        .list_workflows(&WorkflowFilter {
            workflow_id_prefixes: Some(vec!["o".to_owned()]),
            ..WorkflowFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(
        prefix
            .iter()
            .map(|row| row.workflow_id.as_str())
            .collect::<Vec<_>>(),
        ["other"]
    );

    let escaped = engine
        .list_workflows(&WorkflowFilter {
            workflow_id_prefixes: Some(vec!["a%".to_owned(), "a_".to_owned()]),
            ..WorkflowFilter::default()
        })
        .await
        .unwrap();
    assert!(escaped.is_empty());

    let limited = engine
        .list_workflows(&WorkflowFilter {
            limit: Some(2),
            offset: Some(1),
            ..WorkflowFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(
        limited
            .iter()
            .map(|row| row.workflow_id.as_str())
            .collect::<Vec<_>>(),
        ["b", "c"]
    );

    let newest = engine
        .list_workflows(&WorkflowFilter {
            sort_desc: true,
            limit: Some(1),
            ..WorkflowFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(newest[0].workflow_id, "pending");

    let roots = engine
        .list_workflows(&WorkflowFilter {
            has_parent: Some(false),
            ..WorkflowFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(roots.len(), 5);
    let children = engine
        .list_workflows(&WorkflowFilter {
            has_parent: Some(true),
            ..WorkflowFilter::default()
        })
        .await
        .unwrap();
    assert!(children.is_empty());
});

all_dbs!(list_workflows_filters_by_parent_and_time, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let parent = begin_run(&engine, "p").await;
    let mut child = begin(Some("child"));
    child.parent = Some(dwos_core::ParentStep {
        run: &parent,
        step_id: parent.next_step_id(),
    });
    let Begun::Run(_) = engine.begin(child).await.unwrap() else {
        panic!("expected to run the child");
    };

    let kids = engine
        .list_workflows(&WorkflowFilter {
            parent_workflow_ids: Some(vec!["p".to_owned()]),
            ..WorkflowFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(kids.len(), 1);
    assert_eq!(kids[0].workflow_id, "child");

    let created = engine.get_status("p").await.unwrap().created_at_ms;
    let before = engine
        .list_workflows(&WorkflowFilter {
            end_time_ms: Some(created - 1),
            ..WorkflowFilter::default()
        })
        .await
        .unwrap();
    assert!(before.is_empty());
    let after = engine
        .list_workflows(&WorkflowFilter {
            start_time_ms: Some(created),
            ..WorkflowFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(after.len(), 2);

    engine.cancel_workflow("child").await.unwrap();
    let completed = engine
        .get_status("child")
        .await
        .unwrap()
        .completed_at_ms
        .unwrap();
    let done = engine
        .list_workflows(&WorkflowFilter {
            completed_after_ms: Some(completed),
            completed_before_ms: Some(completed),
            ..WorkflowFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(done.len(), 1);
    assert_eq!(done[0].workflow_id, "child");
});

all_dbs!(await_result_reports_cancelled, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    begin_run(&engine, "a").await;
    engine.cancel_workflow("a").await.unwrap();
    let result = engine.await_result("a").await.unwrap();
    assert_eq!(result.outcome, FinalOutcome::Cancelled);
});

all_dbs!(resume_preserves_a_live_pending_owner, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let run = begin_run(&engine, "a").await;
    assert!(
        engine
            .resume_workflow("a", &names())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(engine.get_status("a").await.unwrap().recovery_attempts, 1);
    engine
        .save_step_output(&run, 1, "one", "1".to_owned())
        .await
        .unwrap();
    assert!(matches!(
        engine
            .finish(&run, Outcome::Success("1".to_owned()))
            .await
            .unwrap(),
        dwos_core::Finished::Accepted
    ));
});

all_dbs!(repeated_cancel_preserves_completion_time, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    begin_run(&engine, "a").await;
    engine.cancel_workflow("a").await.unwrap();
    let completed = engine.get_status("a").await.unwrap().completed_at_ms;
    tokio::time::sleep(Duration::from_millis(5)).await;
    engine.cancel_workflow("a").await.unwrap();
    assert_eq!(
        engine.get_status("a").await.unwrap().completed_at_ms,
        completed
    );
});

all_dbs!(list_rejects_negative_pagination, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    for filter in [
        WorkflowFilter {
            limit: Some(-1),
            ..WorkflowFilter::default()
        },
        WorkflowFilter {
            offset: Some(-1),
            ..WorkflowFilter::default()
        },
    ] {
        assert!(matches!(
            engine.list_workflows(&filter).await,
            Err(Error::InvalidConfig(_))
        ));
    }
});

all_dbs!(
    replaced_owner_cannot_check_steps_or_start_children,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db, config()).await;
        let old = begin_run(&engine, "a").await;
        engine
            .save_step_output(&old, 1, "one", "1".to_owned())
            .await
            .unwrap();
        engine.release(&old).await.unwrap();
        let claimed = engine.recover_now(&names()).await.unwrap().pop().unwrap();
        let current = Run::new(
            claimed.lease,
            claimed.serialization,
            claimed.deadline_epoch_ms,
        );
        for (step, name) in [(1, "one"), (2, "two")] {
            assert!(matches!(
                engine.check_step(&old, step, name).await,
                Err(Error::LeaseLost(_))
            ));
        }
        assert_eq!(
            engine.check_step(&current, 1, "one").await.unwrap(),
            StepDecision::ReplayOutput("1".to_owned())
        );
        assert_eq!(
            engine.check_step(&current, 2, "two").await.unwrap(),
            StepDecision::Run
        );
        let mut child = begin(Some("child"));
        child.parent = Some(dwos_core::ParentStep {
            run: &old,
            step_id: 3,
        });
        assert!(matches!(
            engine.begin(child).await,
            Err(Error::LeaseLost(_))
        ));
        assert!(matches!(
            engine.get_status("child").await,
            Err(Error::WorkflowNotFound(_))
        ));
    }
);

fn with_parent<'a>(id: &'a str, parent: &'a Run) -> Begin<'a> {
    let mut child = begin(Some(id));
    child.parent = Some(dwos_core::ParentStep {
        run: parent,
        step_id: parent.next_step_id(),
    });
    child
}

async fn must_run(engine: &Engine, begin: Begin<'_>) -> Run {
    match engine.begin(begin).await.unwrap() {
        Begun::Run(run) => run,
        other => panic!("expected to run, got {other:?}"),
    }
}

all_dbs!(cancel_children_reaches_descendants_only, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    let parent = begin_run(&engine, "p").await;
    let child = must_run(&engine, with_parent("c", &parent)).await;
    must_run(&engine, with_parent("g", &child)).await;
    engine
        .finish(&child, Outcome::Success("ok".to_owned()))
        .await
        .unwrap();
    begin_run(&engine, "other").await;

    let alone = begin_run(&engine, "alone").await;
    must_run(&engine, with_parent("kept", &alone)).await;
    engine
        .cancel_workflows(&["alone".to_owned()], false)
        .await
        .unwrap();
    assert_eq!(
        engine.get_status("alone").await.unwrap().status,
        Status::Cancelled
    );
    assert_eq!(
        engine.get_status("kept").await.unwrap().status,
        Status::Pending
    );

    engine
        .cancel_workflows(&["p".to_owned()], true)
        .await
        .unwrap();
    assert_eq!(
        engine.get_status("p").await.unwrap().status,
        Status::Cancelled
    );
    assert_eq!(
        engine.get_status("c").await.unwrap().status,
        Status::Success
    );
    assert_eq!(
        engine.get_status("g").await.unwrap().status,
        Status::Cancelled
    );
    assert_eq!(
        engine.get_status("other").await.unwrap().status,
        Status::Pending
    );
});

all_dbs!(resume_onto_a_queue_waits_for_admission, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    engine
        .ensure_queue("q", std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    engine
        .ensure_queue("other", std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    begin_run(&engine, "a").await;
    engine.cancel_workflow("a").await.unwrap();
    let error = engine
        .resume_workflows(&["a".to_owned()], &names(), Some("missing"))
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::QueueNotFound(ref name) if name == "missing"),
        "{error:?}"
    );
    assert_eq!(
        engine.get_status("a").await.unwrap().status,
        Status::Cancelled
    );

    assert!(
        engine
            .resume_workflows(&["a".to_owned()], &names(), Some("q"))
            .await
            .unwrap()
            .is_empty()
    );
    let row = engine.get_status("a").await.unwrap();
    assert_eq!(row.status, Status::Enqueued);
    assert_eq!(row.queue_name.as_deref(), Some("q"));
    assert!(
        engine
            .store()
            .claim_one("a", &names(), LONG)
            .await
            .unwrap()
            .is_none()
    );

    assert!(
        engine
            .resume_workflows(&["a".to_owned()], &names(), Some("other"))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(engine.claim_queue("q", &names()).await.unwrap().is_empty());
    assert_eq!(
        engine
            .claim_queue("other", &names())
            .await
            .unwrap()
            .pop()
            .unwrap()
            .lease
            .workflow_id,
        "a"
    );

    let done = begin_run(&engine, "done").await;
    engine
        .finish(&done, Outcome::Success("1".to_owned()))
        .await
        .unwrap();
    assert!(
        engine
            .resume_workflows(&["done".to_owned()], &names(), Some("q"))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        engine.get_status("done").await.unwrap().status,
        Status::Success
    );
});

all_dbs!(
    resume_onto_a_queue_keeps_a_live_owner_and_moves_an_expired_one,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db, config()).await;
        for queue in ["q", "other"] {
            engine
                .ensure_queue(queue, std::num::NonZeroU32::new(1).unwrap())
                .await
                .unwrap();
        }
        let mut timed = begin(Some("live"));
        timed.timeout_ms = Some(600_000);
        engine.enqueue("q", timed).await.unwrap();
        engine.enqueue("q", begin(Some("expired"))).await.unwrap();
        let claimed = engine
            .claim_queue("q", &names())
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(claimed.lease.workflow_id, "live");
        let run = Run::new(
            claimed.lease,
            claimed.serialization,
            claimed.deadline_epoch_ms,
        );
        let keeper = engine.keep_lease(&run);
        let before = engine.get_status("live").await.unwrap();
        assert!(before.deadline_epoch_ms.is_some());

        assert!(
            engine
                .resume_workflows(&["live".to_owned()], &names(), Some("other"))
                .await
                .unwrap()
                .is_empty()
        );
        let after = engine.get_status("live").await.unwrap();
        assert_eq!(after.status, Status::Pending);
        assert_eq!(after.queue_name.as_deref(), Some("q"));
        assert_eq!(after.recovery_attempts, before.recovery_attempts);
        assert_eq!(after.deadline_epoch_ms, before.deadline_epoch_ms);
        // The owner still fills "q", and no second copy starts on either queue.
        assert!(engine.claim_queue("q", &names()).await.unwrap().is_empty());
        assert!(
            engine
                .claim_queue("other", &names())
                .await
                .unwrap()
                .is_empty()
        );
        // The poke made the keeper renew. It must not report a loss.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), keeper.lost())
                .await
                .is_err()
        );
        keeper.stop();
        engine
            .save_step_output(&run, 1, "one", "1".to_owned())
            .await
            .unwrap();
        assert!(matches!(
            engine
                .finish(&run, Outcome::Success("1".to_owned()))
                .await
                .unwrap(),
            dwos_core::Finished::Accepted
        ));

        let short = super::engine(
            &db,
            EngineConfig {
                lease: EXPIRED,
                ..config()
            },
        )
        .await;
        let claimed = short
            .claim_queue("q", &names())
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(claimed.lease.workflow_id, "expired");
        let stale = Run::new(
            claimed.lease,
            claimed.serialization,
            claimed.deadline_epoch_ms,
        );
        short
            .resume_workflows(&["expired".to_owned()], &names(), Some("other"))
            .await
            .unwrap();
        let moved = short.get_status("expired").await.unwrap();
        assert_eq!(moved.status, Status::Enqueued);
        assert_eq!(moved.queue_name.as_deref(), Some("other"));
        assert!(matches!(
            short.check_step(&stale, 1, "step").await,
            Err(Error::LeaseLost(_))
        ));
        assert_eq!(short.claim_queue("other", &names()).await.unwrap().len(), 1);
    }
);

all_dbs!(bulk_resume_claims_each_row_or_none, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    begin_run(&engine, "a").await;
    begin_run(&engine, "b").await;
    engine
        .cancel_workflows(&["a".to_owned(), "b".to_owned()], false)
        .await
        .unwrap();
    let claims = engine
        .resume_workflows(&["a".to_owned(), "b".to_owned()], &names(), None)
        .await
        .unwrap();
    let mut ids: Vec<_> = claims
        .iter()
        .map(|claim| claim.lease.workflow_id.as_str())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, ["a", "b"]);

    begin_run(&engine, "c").await;
    engine.cancel_workflow("c").await.unwrap();
    let error = engine
        .resume_workflows(&["c".to_owned(), "missing".to_owned()], &names(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::WorkflowNotFound(ref ids) if ids.contains("missing")),
        "{error:?}"
    );
    assert_eq!(
        engine.get_status("c").await.unwrap().status,
        Status::Cancelled
    );
});

all_dbs!(
    await_result_interval_does_not_delay_a_finished_workflow,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db, config()).await;
        let run = begin_run(&engine, "a").await;
        engine
            .finish(&run, Outcome::Success("1".to_owned()))
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let result = engine
            .await_result_within("a", Some(Duration::from_secs(30)))
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(result.outcome, FinalOutcome::Success("1".to_owned()));
        assert!(matches!(
            engine
                .await_result_within("a", Some(Duration::ZERO))
                .await
                .unwrap_err(),
            Error::InvalidConfig(_)
        ));
    }
);

all_dbs!(list_filters_stored_metadata, backend, {
    let db = TestDb::migrated(backend).await;
    let store = db.open().await;
    let new = dwos_core::NewWorkflow {
        workflow_id: "metadata",
        name: "wf",
        input: "{}",
        max_recovery_attempts: 100,
        lease: LONG,
        parent_workflow_id: None,
        serialization: dwos_core::PORTABLE_JSON,
        timeout_ms: None,
        deadline_epoch_ms: None,
    };
    store
        .start_workflow_with(
            &new,
            &dwos_core::WorkflowOptions {
                application_version: Some("v1".into()),
                authenticated_user: Some("alice".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let filter = WorkflowFilter {
        app_versions: Some(vec!["v1".into()]),
        users: Some(vec!["alice".into()]),
        ..Default::default()
    };
    assert_eq!(store.list_workflows(&filter).await.unwrap().len(), 1);
    assert!(
        store
            .list_workflows(&WorkflowFilter {
                queue_names: Some(vec!["missing".into()]),
                ..filter
            })
            .await
            .unwrap()
            .is_empty()
    );
});
