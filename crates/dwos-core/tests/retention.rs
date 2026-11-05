use std::num::NonZeroU32;
use std::time::Duration;

use dwos_core::{
    Begin, Begun, Engine, EngineConfig, Error, Outcome, PORTABLE_JSON, Run, Status, Store,
};
use sqlx::AssertSqlSafe;

#[macro_use]
mod common;
use common::*;

const CHILD_TABLES: [(&str, &str); 7] = [
    ("operation_outputs", "workflow_uuid"),
    ("notifications", "destination_uuid"),
    ("workflow_events", "workflow_uuid"),
    ("workflow_event_history", "workflow_uuid"),
    ("workflow_timeouts", "workflow_uuid"),
    ("workflow_queue_entries", "workflow_uuid"),
    ("workflow_attributes", "workflow_uuid"),
];

async fn engine(db: &TestDb) -> Engine {
    let config = EngineConfig {
        lease: LONG,
        pickle: false,
        ..EngineConfig::default()
    };
    Engine::connect(&db.url, config).await.unwrap()
}

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

fn ids(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values
}

async fn execute(store: &Store, sql: String) {
    sqlx::raw_sql(AssertSqlSafe(sql))
        .execute(store.pool())
        .await
        .unwrap();
}

async fn count(store: &Store, sql: String) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(sql))
        .fetch_one(store.pool())
        .await
        .unwrap()
}

async fn exists(store: &Store, id: &str) -> bool {
    store.get_workflow(id).await.unwrap().is_some()
}

/// Finished rows with this many ids `prefix0`, `prefix1` and so on, written in one statement.
async fn seed_finished(store: &Store, prefix: &str, rows: usize, completed_at: i64) {
    let values: Vec<String> = (0..rows)
        .map(|row| {
            format!(
                "('{prefix}{row}', 'wf', 'SUCCESS', '{{}}', '{PORTABLE_JSON}', 100, 1, 1, {completed_at})"
            )
        })
        .collect();
    execute(
        store,
        format!(
            "INSERT INTO workflow_status (workflow_uuid, name, status, inputs, serialization, \
             max_recovery_attempts, created_at, updated_at, completed_at) VALUES {}",
            values.join(", ")
        ),
    )
    .await;
}

async fn start_finished(store: &Store, id: &str, parent: Option<&str>) {
    let mut new = workflow(id, LONG);
    new.parent_workflow_id = parent;
    let lease = start(store, &new).await;
    store
        .finish_workflow(&lease, &Outcome::Success("1".to_owned()))
        .await
        .unwrap();
}

async fn start_pending(store: &Store, id: &str, parent: Option<&str>) {
    let mut new = workflow(id, LONG);
    new.parent_workflow_id = parent;
    start(store, &new).await;
}

/// One row in each final or stopped status, each reached the way the engine reaches it.
async fn one_of_each_stopped_status(engine: &Engine, store: &Store) -> Vec<String> {
    start_finished(store, "ok", None).await;
    let lease = start(store, &workflow("bad", LONG)).await;
    store
        .finish_workflow(&lease, &Outcome::Error("boom".to_owned()))
        .await
        .unwrap();
    start_pending(store, "cancelled", None).await;
    engine.cancel_workflow("cancelled").await.unwrap();
    let mut dead = workflow("dead", EXPIRED);
    dead.max_recovery_attempts = 1;
    start(store, &dead).await;
    store.reclaim(&names(), 10, EXPIRED).await.unwrap();
    store.reclaim(&names(), 10, EXPIRED).await.unwrap();
    for (id, status) in [
        ("ok", Status::Success),
        ("bad", Status::Error),
        ("cancelled", Status::Cancelled),
        ("dead", Status::MaxRecoveryAttemptsExceeded),
    ] {
        let row = store.get_workflow(id).await.unwrap().unwrap();
        assert_eq!(row.status, status, "{id}");
        assert!(row.completed_at_ms.is_some(), "{id}");
    }
    ids(&["bad", "cancelled", "dead", "ok"])
}

all_dbs!(child_rows_go_with_the_workflow, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let store = engine.store().clone();
    engine
        .ensure_queue("q", NonZeroU32::new(1).unwrap())
        .await
        .unwrap();

    let mut queued = begin("a");
    queued.timeout_ms = Some(600_000);
    engine.enqueue("q", queued).await.unwrap();
    let claimed = engine
        .claim_queue("q", &names())
        .await
        .unwrap()
        .pop()
        .unwrap();
    let run = Run::new(
        claimed.lease,
        claimed.serialization,
        claimed.deadline_epoch_ms,
    );
    engine
        .save_step_output(&run, 1, "step", "1".to_owned())
        .await
        .unwrap();
    engine
        .set_event(&run, None, "key", "1", PORTABLE_JSON)
        .await
        .unwrap();
    engine
        .send("a", None, "1", PORTABLE_JSON, None, None, None)
        .await
        .unwrap();
    execute(
        &store,
        "INSERT INTO workflow_attributes (workflow_uuid, attr_key, attr_value) VALUES ('a', 'k', 'v')"
            .to_owned(),
    )
    .await;
    engine
        .finish(&run, Outcome::Success("1".to_owned()))
        .await
        .unwrap();
    for (table, column) in CHILD_TABLES {
        let sql = format!("SELECT COUNT(*) FROM {table} WHERE {column} = 'a'");
        assert!(count(&store, sql).await > 0, "{table} has no row to delete");
    }

    assert_eq!(
        engine
            .delete_workflows(&ids(&["a", "missing"]), false)
            .await
            .unwrap(),
        ids(&["a"])
    );

    assert!(!exists(&store, "a").await);
    for (table, column) in CHILD_TABLES {
        let sql = format!("SELECT COUNT(*) FROM {table} WHERE {column} = 'a'");
        assert_eq!(count(&store, sql).await, 0, "{table} kept a row");
    }
    // The queue itself is not the workflow's to delete.
    assert!(engine.get_queue("q").await.unwrap().is_some());
});

all_dbs!(every_stopped_status_can_be_deleted_by_id, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let store = engine.store().clone();
    let expected = one_of_each_stopped_status(&engine, &store).await;
    let deleted = engine.delete_workflows(&expected, false).await.unwrap();
    assert_eq!(sorted(deleted), expected);
});

all_dbs!(every_stopped_status_can_be_deleted_by_age, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let store = engine.store().clone();
    one_of_each_stopped_status(&engine, &store).await;
    assert_eq!(
        engine.delete_finished_before(i64::MAX, 10).await.unwrap(),
        4
    );
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM workflow_status".to_owned()).await,
        0
    );
});

all_dbs!(pending_rows_stay_by_both_apis, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let store = engine.store().clone();
    engine
        .ensure_queue("q", NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    engine.enqueue("q", begin("waiting")).await.unwrap();
    start_pending(&store, "owned", None).await;
    let row = store.get_workflow("owned").await.unwrap().unwrap();
    assert_eq!(row.status, Status::Pending);
    // A stale completed_at must not be enough: only the status says a row has stopped.
    execute(
        &store,
        "UPDATE workflow_status SET completed_at = 1".to_owned(),
    )
    .await;

    let both = ids(&["waiting", "owned"]);
    assert!(
        engine
            .delete_workflows(&both, false)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        engine.delete_finished_before(i64::MAX, 10).await.unwrap(),
        0
    );
    for id in &both {
        assert!(exists(&store, id).await, "{id}");
    }
});

all_dbs!(a_live_owner_keeps_its_row_and_can_finish, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let store = engine.store().clone();
    let Begun::Run(run) = engine.begin(begin("live")).await.unwrap() else {
        panic!("expected to run");
    };
    assert!(
        engine
            .delete_workflows(&ids(&["live"]), true)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        engine.delete_finished_before(i64::MAX, 10).await.unwrap(),
        0
    );
    engine
        .finish(&run, Outcome::Success("1".to_owned()))
        .await
        .unwrap();
    assert_eq!(
        store.get_workflow("live").await.unwrap().unwrap().status,
        Status::Success
    );
});

all_dbs!(delete_children_keeps_a_pending_child, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let store = engine.store().clone();
    start_finished(&store, "root", None).await;
    start_finished(&store, "done", Some("root")).await;
    start_finished(&store, "done-done", Some("done")).await;
    start_pending(&store, "running", Some("root")).await;
    start_finished(&store, "running-done", Some("running")).await;
    start_finished(&store, "unrelated", None).await;

    let deleted = engine
        .delete_workflows(&ids(&["root"]), true)
        .await
        .unwrap();
    assert_eq!(
        sorted(deleted),
        ids(&["done", "done-done", "root", "running-done"])
    );
    assert!(exists(&store, "running").await);
    assert!(exists(&store, "unrelated").await);
});

all_dbs!(without_delete_children_a_child_stays, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let store = engine.store().clone();
    start_finished(&store, "root", None).await;
    start_finished(&store, "child", Some("root")).await;
    assert_eq!(
        engine
            .delete_workflows(&ids(&["root"]), false)
            .await
            .unwrap(),
        ids(&["root"])
    );
    assert!(exists(&store, "child").await);
});

all_dbs!(the_batch_loop_deletes_more_than_one_batch, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let store = engine.store().clone();
    // An exact multiple needs one more read that comes back empty.
    for (rows, batch) in [(7, 2), (6, 2), (1, 1)] {
        seed_finished(&store, "b", rows, 100).await;
        assert_eq!(
            engine.delete_finished_before(200, batch).await.unwrap(),
            rows as u64
        );
        assert_eq!(
            count(&store, "SELECT COUNT(*) FROM workflow_status".to_owned()).await,
            0
        );
    }
});

all_dbs!(lists_larger_than_a_chunk_are_deleted, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let store = engine.store().clone();
    seed_finished(&store, "by-id", 450, 100).await;
    let targets: Vec<String> = (0..450).map(|row| format!("by-id{row}")).collect();
    assert_eq!(
        engine
            .delete_workflows(&targets, false)
            .await
            .unwrap()
            .len(),
        450
    );
    seed_finished(&store, "by-age", 450, 100).await;
    assert_eq!(
        engine.delete_finished_before(200, u32::MAX).await.unwrap(),
        450
    );
});

all_dbs!(rows_at_or_after_the_cutoff_stay, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let store = engine.store().clone();
    seed_finished(&store, "old", 3, 100).await;
    seed_finished(&store, "at", 1, 200).await;
    seed_finished(&store, "new", 1, 300).await;
    assert_eq!(engine.delete_finished_before(200, 2).await.unwrap(), 3);
    assert!(exists(&store, "at0").await);
    assert!(exists(&store, "new0").await);
    assert!(!exists(&store, "old0").await);
});

all_dbs!(a_deleted_id_can_start_again, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let Begun::Run(run) = engine.begin(begin("a")).await.unwrap() else {
        panic!("expected to run");
    };
    engine
        .save_step_output(&run, 1, "step", "1".to_owned())
        .await
        .unwrap();
    engine
        .finish(&run, Outcome::Success("1".to_owned()))
        .await
        .unwrap();
    assert!(matches!(
        engine.begin(begin("a")).await.unwrap(),
        Begun::Stored(_)
    ));
    engine.delete_workflows(&ids(&["a"]), false).await.unwrap();
    let Begun::Run(again) = engine.begin(begin("a")).await.unwrap() else {
        panic!("a deleted id must run again");
    };
    // The old step went with the old row, so step 1 is free to run.
    engine
        .save_step_output(&again, 1, "other-step", "2".to_owned())
        .await
        .unwrap();
});

all_dbs!(bad_input_is_refused_before_anything_is_deleted, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let store = engine.store().clone();
    start_finished(&store, "a", None).await;
    assert!(matches!(
        engine.delete_finished_before(i64::MAX, 0).await,
        Err(Error::InvalidConfig(_))
    ));
    assert!(matches!(
        engine.delete_workflows(&ids(&["a", "b\0"]), false).await,
        Err(Error::ContainsNul { .. })
    ));
    assert!(matches!(
        engine
            .delete_workflows(&["a".to_owned(), "x".repeat(256)], true)
            .await,
        Err(Error::NameTooLong { .. })
    ));
    assert!(exists(&store, "a").await);
    assert!(engine.delete_workflows(&[], true).await.unwrap().is_empty());
});

all_dbs!(deleting_by_age_does_not_wait_for_a_live_row, backend, {
    // SQLite has no row locks to test.
    if backend == Backend::Sqlite {
        return;
    }
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let store = engine.store().clone();
    start_pending(&store, "live", None).await;
    seed_finished(&store, "old", 5, 100).await;
    let mut holder = store.pool().begin().await.unwrap();
    sqlx::query(
        "SELECT workflow_uuid FROM workflow_status WHERE workflow_uuid = 'live' FOR UPDATE",
    )
    .fetch_one(&mut *holder)
    .await
    .unwrap();
    // Well under the 10s lock wait, which a blocked statement would sit through.
    let deleted = tokio::time::timeout(
        Duration::from_secs(5),
        engine.delete_finished_before(200, 2),
    )
    .await
    .expect("the delete waited for a live row's lock")
    .unwrap();
    assert_eq!(deleted, 5);
    holder.rollback().await.unwrap();
    assert!(exists(&store, "live").await);
});
