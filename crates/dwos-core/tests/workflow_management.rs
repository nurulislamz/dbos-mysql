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
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
    let engine = engine(&db, config()).await;
    engine.cancel_workflow("missing").await.unwrap();
});

all_dbs!(check_step_stops_once_the_workflow_is_cancelled, backend, {
    let db = TestDb::create(backend).await;
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

all_dbs!(resume_makes_a_cancelled_workflow_claimable, backend, {
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
        let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
    let engine = engine(&db, config()).await;
    begin_run(&engine, "a").await;
    engine.cancel_workflow("a").await.unwrap();
    let result = engine.await_result("a").await.unwrap();
    assert_eq!(result.outcome, FinalOutcome::Cancelled);
});

all_dbs!(resume_preserves_a_live_pending_owner, backend, {
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
        let db = TestDb::create(backend).await;
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
