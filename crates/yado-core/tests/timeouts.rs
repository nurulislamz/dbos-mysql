use std::time::Duration;

use yado_core::{
    Begin, Begun, Engine, EngineConfig, Error, FinalOutcome, Finished, Outcome, ParentStep, Status,
};

#[macro_use]
mod common;
use common::*;

fn config() -> EngineConfig {
    EngineConfig {
        lease: LONG,
        pickle: false,
        ..EngineConfig::default()
    }
}

async fn setup_engine(backend: Backend) -> (TestDb, Engine) {
    let db = TestDb::migrated(backend).await;
    let engine = Engine::connect(&db.url, config()).await.unwrap();
    (db, engine)
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

async fn begin_run(engine: &Engine, start: Begin<'_>) -> yado_core::Run {
    match engine.begin(start).await.unwrap() {
        Begun::Run(run) => run,
        other => panic!("expected to run, got {other:?}"),
    }
}

all_dbs!(
    a_timeout_stores_deadline_from_the_database_clock,
    backend,
    {
        let (_db, engine) = setup_engine(backend).await;
        let mut start = begin(Some("a"));
        start.timeout_ms = Some(100);
        let run = begin_run(&engine, start).await;
        assert!(run.deadline_epoch_ms().is_some());
        let row = engine.get_status("a").await.unwrap();
        assert_eq!(row.timeout_ms, Some(100));
        assert_eq!(
            row.deadline_epoch_ms.unwrap() - row.created_at_ms,
            100,
            "deadline is created_at plus timeout, from the same INSERT"
        );
    }
);

all_dbs!(a_child_inherits_the_parent_deadline, backend, {
    let (_db, engine) = setup_engine(backend).await;
    let mut parent_begin = begin(Some("p"));
    parent_begin.timeout_ms = Some(5_000);
    let parent = begin_run(&engine, parent_begin).await;
    let mut child = begin(None);
    child.parent = Some(ParentStep {
        run: &parent,
        step_id: parent.next_step_id(),
    });
    let child_run = begin_run(&engine, child).await;
    let parent_row = engine.get_status("p").await.unwrap();
    let child_row = engine.get_status(child_run.workflow_id()).await.unwrap();
    assert_eq!(child_row.timeout_ms, None);
    assert_eq!(child_row.deadline_epoch_ms, parent_row.deadline_epoch_ms);
    assert_eq!(child_run.deadline_epoch_ms(), parent.deadline_epoch_ms());
});

all_dbs!(
    an_explicit_child_timeout_replaces_the_parent_deadline,
    backend,
    {
        let (_db, engine) = setup_engine(backend).await;
        let mut parent_begin = begin(Some("p"));
        parent_begin.timeout_ms = Some(5_000);
        let parent = begin_run(&engine, parent_begin).await;
        let mut child = begin(None);
        child.parent = Some(ParentStep {
            run: &parent,
            step_id: parent.next_step_id(),
        });
        child.timeout_ms = Some(100);
        let child_run = begin_run(&engine, child).await;
        let child_row = engine.get_status(child_run.workflow_id()).await.unwrap();
        assert_eq!(child_row.timeout_ms, Some(100));
        assert_eq!(
            child_row.deadline_epoch_ms.unwrap() - child_row.created_at_ms,
            100
        );
        assert_ne!(
            child_row.deadline_epoch_ms,
            engine.get_status("p").await.unwrap().deadline_epoch_ms
        );
    }
);

all_dbs!(ignoring_the_propagated_deadline_clears_timeout, backend, {
    let (_db, engine) = setup_engine(backend).await;
    let mut parent_begin = begin(Some("p"));
    parent_begin.timeout_ms = Some(5_000);
    let parent = begin_run(&engine, parent_begin).await;
    let mut child = begin(None);
    child.parent = Some(ParentStep {
        run: &parent,
        step_id: parent.next_step_id(),
    });
    child.ignore_propagated_deadline = true;
    let child_run = begin_run(&engine, child).await;
    let child_row = engine.get_status(child_run.workflow_id()).await.unwrap();
    assert_eq!(child_row.timeout_ms, None);
    assert_eq!(child_row.deadline_epoch_ms, None);
});

all_dbs!(cancel_expired_cancels_only_past_deadlines, backend, {
    let (_db, engine) = setup_engine(backend).await;
    let mut expired = begin(Some("old"));
    expired.timeout_ms = Some(1);
    begin_run(&engine, expired).await;
    let mut live = begin(Some("live"));
    live.timeout_ms = Some(60_000);
    begin_run(&engine, live).await;
    begin_run(&engine, begin(Some("none"))).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    engine.store().cancel_expired().await.unwrap();
    assert_eq!(
        engine.get_status("old").await.unwrap().status,
        Status::Cancelled
    );
    assert_eq!(
        engine.get_status("live").await.unwrap().status,
        Status::Pending
    );
    assert_eq!(
        engine.get_status("none").await.unwrap().status,
        Status::Pending
    );
});

all_dbs!(
    reclaim_cancels_an_expired_workflow_instead_of_claiming_it,
    backend,
    {
        let (_db, engine) = setup_engine(backend).await;
        let mut start = begin(Some("a"));
        start.timeout_ms = Some(1);
        let run = begin_run(&engine, start).await;
        engine.release(&run).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let claims = engine.recover_now(&names()).await.unwrap();
        assert!(claims.is_empty(), "{claims:?}");
        assert_eq!(
            engine.get_status("a").await.unwrap().status,
            Status::Cancelled
        );
    }
);

all_dbs!(finish_after_the_deadline_is_cancelled, backend, {
    let (_db, engine) = setup_engine(backend).await;
    let mut start = begin(Some("a"));
    start.timeout_ms = Some(1);
    let run = begin_run(&engine, start).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let finished = engine
        .finish(&run, Outcome::Success("1".to_owned()))
        .await
        .unwrap();
    let Finished::Superseded(result) = finished else {
        panic!("expected cancelled supersede, got {finished:?}");
    };
    assert_eq!(result.outcome, FinalOutcome::Cancelled);
    assert_eq!(
        engine.get_status("a").await.unwrap().status,
        Status::Cancelled
    );
});

all_dbs!(a_non_positive_timeout_is_rejected, backend, {
    let (_db, engine) = setup_engine(backend).await;
    let mut start = begin(Some("a"));
    start.timeout_ms = Some(0);
    let error = engine.begin(start).await.unwrap_err();
    assert!(matches!(error, Error::InvalidConfig(_)), "{error:?}");
});

all_dbs!(expired_workflows_cannot_save_steps, backend, {
    let (_db, engine) = setup_engine(backend).await;
    let mut start = begin(Some("a"));
    start.timeout_ms = Some(1);
    let run = begin_run(&engine, start).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let error = engine
        .save_step_output(&run, 1, "step", "1".to_owned())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::WorkflowCancelled(_)), "{error:?}");
    assert!(engine.list_steps("a").await.unwrap().is_empty());
    assert_eq!(
        engine.get_status("a").await.unwrap().status,
        Status::Cancelled
    );
});

all_dbs!(existing_databases_gain_timeout_support, backend, {
    let db = TestDb::create(backend).await;
    let schema = match backend {
        Backend::Sqlite => include_str!("../schema/sqlite/001_init.sql"),
        Backend::MySql => include_str!("../schema/mysql/001_init.sql"),
        Backend::Postgres => include_str!("../schema/postgres/001_init.sql"),
    };
    let legacy = schema
        .split("CREATE TABLE IF NOT EXISTS workflow_timeouts")
        .next()
        .unwrap();
    let store = yado_core::Store::connect(&db.url).await.unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(legacy))
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO workflow_status (workflow_uuid, name, status, inputs, serialization, max_recovery_attempts, created_at, updated_at) VALUES ('legacy', 'wf', 'PENDING', '{}', 'portable_json', 100, 0, 0)")
        .execute(store.pool()).await.unwrap();
    let engine = Engine::connect(&db.url, config()).await.unwrap();
    assert_eq!(
        engine.get_status("legacy").await.unwrap().deadline_epoch_ms,
        None
    );
    let mut start = begin(Some("new"));
    start.timeout_ms = Some(5_000);
    let run = begin_run(&engine, start).await;
    assert!(run.deadline_epoch_ms().is_some());
});

all_dbs!(
    an_overflowing_timeout_rolls_back_workflow_creation,
    backend,
    {
        let (_db, engine) = setup_engine(backend).await;
        let mut start = begin(Some("a"));
        start.timeout_ms = Some(i64::MAX);
        assert!(matches!(
            engine.begin(start).await,
            Err(Error::InvalidConfig(_))
        ));
        assert!(matches!(
            engine.get_status("a").await,
            Err(Error::WorkflowNotFound(_))
        ));
    }
);

all_dbs!(expired_runs_cannot_send_start_steps_or_resume, backend, {
    let (_db, engine) = setup_engine(backend).await;
    begin_run(&engine, begin(Some("destination"))).await;
    let mut source = begin(Some("source"));
    source.timeout_ms = Some(1);
    let run = begin_run(&engine, source).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let error = engine
        .send(
            "destination",
            None,
            "1",
            yado_core::PORTABLE_JSON,
            None,
            Some(&run),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::WorkflowCancelled(_)), "{error:?}");
    assert!(
        engine
            .store()
            .list_messages("destination")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        engine.check_step(&run, 1, "step").await,
        Err(Error::WorkflowCancelled(_))
    ));
    assert!(
        engine
            .resume_workflow("source", &names())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        engine.await_result("source").await.unwrap().outcome,
        FinalOutcome::Cancelled
    );

    let mut source = begin(Some("unpolled"));
    source.timeout_ms = Some(1);
    let run = begin_run(&engine, source).await;
    engine.release(&run).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        engine
            .store()
            .claim_one("unpolled", &names(), LONG)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        engine.get_status("unpolled").await.unwrap().status,
        Status::Cancelled
    );
});
