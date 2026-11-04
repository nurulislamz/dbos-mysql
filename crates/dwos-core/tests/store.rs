use std::sync::Arc;
use std::time::Duration;

use dwos_core::{Error, Outcome, Started, Status, StepRecord, Store};
use sqlx::Connection;
use sqlx::mysql::MySqlConnection;
use sqlx::postgres::PgConnection;
use sqlx::{AnyPool, Row};

#[macro_use]
mod common;
use common::*;

async fn scratch_rows(pool: &AnyPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM scratch")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn create_scratch(pool: &AnyPool) {
    sqlx::raw_sql("DROP TABLE IF EXISTS scratch")
        .execute(pool)
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE scratch (id INT NOT NULL)")
        .execute(pool)
        .await
        .unwrap();
}

// Holds an open transaction with one uncommitted row, as a workflow write in flight would.
async fn hold_uncommitted_row(store: &Store, hold: Duration) {
    let begin = if store.backend() == "sqlite" {
        "BEGIN IMMEDIATE"
    } else {
        "BEGIN"
    };
    let mut tx = store.pool().begin_with(begin).await.unwrap();
    sqlx::query("INSERT INTO scratch (id) VALUES (1)")
        .execute(&mut *tx)
        .await
        .unwrap();
    tokio::time::sleep(hold).await;
    tx.commit().await.unwrap();
}

all_dbs!(new_workflow_is_owned_and_pending, backend, {
    let (_db, store) = setup(backend).await;
    start(&store, &workflow("a", LONG)).await;
    let state = store.get_workflow("a").await.unwrap().unwrap();
    assert_eq!(state.status, Status::Pending);
    assert_eq!(state.input, "{}");
    assert_eq!(state.recovery_attempts, 1);
});

all_dbs!(missing_workflow_is_none, backend, {
    let (_db, store) = setup(backend).await;
    assert!(store.get_workflow("nope").await.unwrap().is_none());
});

all_dbs!(same_id_attaches_to_existing_workflow, backend, {
    let (_db, store) = setup(backend).await;
    start(&store, &workflow("a", LONG)).await;
    let mut other_input = workflow("a", LONG);
    other_input.input = "{\"changed\":true}";
    let Started::Existing(existing) = store.start_workflow(&other_input).await.unwrap() else {
        panic!("expected the existing workflow");
    };
    assert_eq!(existing.input, "{}");
});

all_dbs!(same_id_with_another_name_conflicts, backend, {
    let (_db, store) = setup(backend).await;
    start(&store, &workflow("a", LONG)).await;
    let mut other = workflow("a", LONG);
    other.name = "other";
    let error = store.start_workflow(&other).await.unwrap_err();
    assert!(
        matches!(error, Error::ConflictingWorkflow { .. }),
        "{error:?}"
    );
    assert_eq!(store.get_workflow("a").await.unwrap().unwrap().name, "wf");
});

all_dbs!(ids_are_case_sensitive, backend, {
    let (_db, store) = setup(backend).await;
    start(&store, &workflow("Job-1", LONG)).await;
    start(&store, &workflow("job-1", LONG)).await;
});

all_dbs!(saved_step_replays_its_output, backend, {
    let (_db, store) = setup(backend).await;
    let lease = start(&store, &workflow("a", LONG)).await;
    assert_eq!(store.load_step("a", 1, "step").await.unwrap(), None);
    store
        .save_step(&lease, 1, "step", &output("\"x\""))
        .await
        .unwrap();
    assert_eq!(
        store.load_step("a", 1, "step").await.unwrap(),
        Some(output("\"x\""))
    );
});

all_dbs!(saved_step_error_replays, backend, {
    let (_db, store) = setup(backend).await;
    let lease = start(&store, &workflow("a", LONG)).await;
    let failed = StepRecord {
        error: Some("{\"type\":\"ValueError\"}".to_owned()),
        ..StepRecord::default()
    };
    store.save_step(&lease, 1, "step", &failed).await.unwrap();
    assert_eq!(store.load_step("a", 1, "step").await.unwrap(), Some(failed));
});

all_dbs!(child_workflow_id_is_stored_with_the_step, backend, {
    let (_db, store) = setup(backend).await;
    let lease = start(&store, &workflow("a", LONG)).await;
    let child = StepRecord {
        child_workflow_id: Some("a-1".to_owned()),
        ..StepRecord::default()
    };
    store.save_step(&lease, 1, "child", &child).await.unwrap();
    assert_eq!(store.load_step("a", 1, "child").await.unwrap(), Some(child));
});

all_dbs!(load_step_on_missing_workflow_fails, backend, {
    let (_db, store) = setup(backend).await;
    let error = store.load_step("nope", 1, "step").await.unwrap_err();
    assert!(matches!(error, Error::WorkflowNotFound(_)), "{error:?}");
});

all_dbs!(nondeterministic_workflow_step_name_is_rejected, backend, {
    let (_db, store) = setup(backend).await;
    let lease = start(&store, &workflow("a", LONG)).await;
    store
        .save_step(&lease, 1, "step", &output("1"))
        .await
        .unwrap();
    let error = store.load_step("a", 1, "renamed").await.unwrap_err();
    assert!(matches!(error, Error::UnexpectedStep { .. }), "{error:?}");
});

all_dbs!(retried_identical_save_is_accepted, backend, {
    let (_db, store) = setup(backend).await;
    let lease = start(&store, &workflow("a", LONG)).await;
    store
        .save_step(&lease, 1, "step", &output("1"))
        .await
        .unwrap();
    store
        .save_step(&lease, 1, "step", &output("1"))
        .await
        .unwrap();
});

all_dbs!(
    step_recorded_twice_by_the_owner_is_nondeterminism,
    backend,
    {
        let (_db, store) = setup(backend).await;
        let lease = start(&store, &workflow("a", LONG)).await;
        store
            .save_step(&lease, 1, "step", &output("1"))
            .await
            .unwrap();
        let error = store
            .save_step(&lease, 1, "step", &output("2"))
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::StepNondeterminism { .. }),
            "{error:?}"
        );
        assert_eq!(
            store.load_step("a", 1, "step").await.unwrap(),
            Some(output("1"))
        );
    }
);

all_dbs!(stale_owner_cannot_save_a_step, backend, {
    let (_db, store) = setup(backend).await;
    let stale = start(&store, &workflow("a", EXPIRED)).await;
    assert_eq!(store.reclaim(&names(), 10, LONG).await.unwrap().len(), 1);
    let error = store
        .save_step(&stale, 1, "step", &output("1"))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::LeaseLost(_)), "{error:?}");
    assert_eq!(store.load_step("a", 1, "step").await.unwrap(), None);
});

all_dbs!(stale_owner_cannot_write_the_outcome, backend, {
    let (_db, store) = setup(backend).await;
    let stale = start(&store, &workflow("a", EXPIRED)).await;
    let claimed = store.reclaim(&names(), 10, LONG).await.unwrap().remove(0);

    let late = Outcome::Success("\"stale\"".to_owned());
    let error = store.finish_workflow(&stale, &late).await.unwrap_err();
    assert!(matches!(error, Error::LeaseLost(_)), "{error:?}");

    store
        .finish_workflow(&claimed.lease, &Outcome::Success("\"owner\"".to_owned()))
        .await
        .unwrap();
    let state = store.get_workflow("a").await.unwrap().unwrap();
    assert_eq!(state.output.as_deref(), Some("\"owner\""));
});

all_dbs!(
    finished_workflow_returns_stored_result_and_is_final,
    backend,
    {
        let (_db, store) = setup(backend).await;
        let lease = start(&store, &workflow("a", LONG)).await;
        store
            .finish_workflow(&lease, &Outcome::Success("1".to_owned()))
            .await
            .unwrap();

        let Started::Existing(existing) = store.start_workflow(&workflow("a", LONG)).await.unwrap()
        else {
            panic!("expected the finished workflow");
        };
        assert_eq!(existing.status, Status::Success);
        assert_eq!(existing.output.as_deref(), Some("1"));

        let again = store
            .finish_workflow(&lease, &Outcome::Error("late".to_owned()))
            .await;
        assert!(matches!(again, Err(Error::LeaseLost(_))), "{again:?}");
        assert!(store.reclaim(&names(), 10, LONG).await.unwrap().is_empty());
    }
);

all_dbs!(workflow_error_is_stored_and_not_recovered, backend, {
    let (_db, store) = setup(backend).await;
    let lease = start(&store, &workflow("a", EXPIRED)).await;
    store
        .finish_workflow(&lease, &Outcome::Error("boom".to_owned()))
        .await
        .unwrap();
    let state = store.get_workflow("a").await.unwrap().unwrap();
    assert_eq!(state.status, Status::Error);
    assert_eq!(state.error.as_deref(), Some("boom"));
    assert_eq!(state.output, None);
    assert!(store.reclaim(&names(), 10, LONG).await.unwrap().is_empty());
});

all_dbs!(reclaim_skips_live_leases, backend, {
    let (_db, store) = setup(backend).await;
    start(&store, &workflow("live", LONG)).await;
    assert!(store.reclaim(&names(), 10, LONG).await.unwrap().is_empty());
});

all_dbs!(renewing_keeps_a_lease_alive, backend, {
    let (_db, store) = setup(backend).await;
    let lease = start(&store, &workflow("a", EXPIRED)).await;
    store.renew_lease(&lease, LONG).await.unwrap();
    assert!(store.reclaim(&names(), 10, LONG).await.unwrap().is_empty());
});

all_dbs!(renewing_after_takeover_reports_lease_lost, backend, {
    let (_db, store) = setup(backend).await;
    let stale = start(&store, &workflow("a", EXPIRED)).await;
    store.reclaim(&names(), 10, LONG).await.unwrap();
    let error = store.renew_lease(&stale, LONG).await.unwrap_err();
    assert!(matches!(error, Error::LeaseLost(_)), "{error:?}");
});

all_dbs!(reclaim_only_takes_registered_names, backend, {
    let (_db, store) = setup(backend).await;
    start(&store, &workflow("a", EXPIRED)).await;
    assert!(
        store
            .reclaim(&["other".to_owned()], 10, LONG)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(store.reclaim(&[], 10, LONG).await.unwrap().is_empty());
    assert_eq!(store.reclaim(&names(), 10, LONG).await.unwrap().len(), 1);
});

all_dbs!(reclaim_respects_its_limit, backend, {
    let (_db, store) = setup(backend).await;
    for id in ["a", "b", "c"] {
        start(&store, &workflow(id, EXPIRED)).await;
    }
    assert_eq!(store.reclaim(&names(), 2, LONG).await.unwrap().len(), 2);
    assert_eq!(store.reclaim(&names(), 2, LONG).await.unwrap().len(), 1);
});

all_dbs!(
    recovery_keeps_saved_steps_and_counts_the_attempt,
    backend,
    {
        let (_db, store) = setup(backend).await;
        let first = start(&store, &workflow("a", EXPIRED)).await;
        store
            .save_step(&first, 1, "step", &output("1"))
            .await
            .unwrap();

        let claimed = store.reclaim(&names(), 10, LONG).await.unwrap().remove(0);
        assert_ne!(claimed.lease.token, first.token);
        assert_eq!(claimed.recovery_attempts, 2);
        assert_eq!(claimed.input, "{}");
        assert_eq!(
            store.load_step("a", 1, "step").await.unwrap(),
            Some(output("1"))
        );
        store
            .save_step(&claimed.lease, 2, "next", &output("2"))
            .await
            .unwrap();
    }
);

all_dbs!(too_many_recoveries_end_the_workflow, backend, {
    let (_db, store) = setup(backend).await;
    let mut new = workflow("a", EXPIRED);
    new.max_recovery_attempts = 1;
    start(&store, &new).await;

    let claimed = store.reclaim(&names(), 10, EXPIRED).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert!(
        store
            .reclaim(&names(), 10, EXPIRED)
            .await
            .unwrap()
            .is_empty()
    );

    let state = store.get_workflow("a").await.unwrap().unwrap();
    assert_eq!(state.status, Status::MaxRecoveryAttemptsExceeded);
    assert!(
        store
            .reclaim(&names(), 10, EXPIRED)
            .await
            .unwrap()
            .is_empty()
    );
    let late = store
        .save_step(&claimed[0].lease, 1, "step", &output("1"))
        .await;
    assert!(matches!(late, Err(Error::LeaseLost(_))), "{late:?}");
});

all_dbs!(release_hands_over_without_using_an_attempt, backend, {
    let (_db, store) = setup(backend).await;
    let lease = start(&store, &workflow("a", LONG)).await;
    store.release_lease(&lease).await.unwrap();

    let claimed = store.reclaim(&names(), 10, LONG).await.unwrap().remove(0);
    assert_eq!(claimed.recovery_attempts, 1);
    let late = store.release_lease(&lease).await;
    assert!(matches!(late, Err(Error::LeaseLost(_))), "{late:?}");
});

all_dbs!(ids_with_trailing_spaces_are_separate, backend, {
    let (_db, store) = setup(backend).await;
    start(&store, &workflow("job", LONG)).await;
    start(&store, &workflow("job ", LONG)).await;
});

all_dbs!(overlong_names_are_rejected, backend, {
    let (_db, store) = setup(backend).await;
    let long = "x".repeat(256);
    let error = store
        .start_workflow(&workflow(&long, LONG))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::NameTooLong { .. }), "{error:?}");
    let exact = "é".repeat(255);
    start(&store, &workflow(&exact, LONG)).await;
    let lease = start(&store, &workflow("a", LONG)).await;
    let error = store
        .save_step(&lease, 1, &long, &output("1"))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::NameTooLong { .. }), "{error:?}");
});

all_dbs!(renewing_repeatedly_keeps_ownership, backend, {
    let (_db, store) = setup(backend).await;
    let lease = start(&store, &workflow("a", LONG)).await;
    for _ in 0..50 {
        store.renew_lease(&lease, LONG).await.unwrap();
    }
});

all_dbs!(concurrent_starts_of_one_id_have_one_owner, backend, {
    let db = Arc::new(TestDb::create(backend).await);
    db.open().await;
    let mut starts = Vec::new();
    for _ in 0..8 {
        let db = db.clone();
        starts.push(tokio::spawn(async move {
            let store = db.open().await;
            matches!(
                store.start_workflow(&workflow("a", LONG)).await.unwrap(),
                Started::Owned(_)
            )
        }));
    }
    let mut owners = 0;
    for task in starts {
        owners += u32::from(task.await.unwrap());
    }
    assert_eq!(owners, 1);
});

all_dbs!(concurrent_saves_of_one_step_keep_one_result, backend, {
    let db = Arc::new(TestDb::create(backend).await);
    let store = db.open().await;
    let lease = start(&store, &workflow("a", LONG)).await;
    let mut saves = Vec::new();
    for index in 0..8 {
        let (store, lease) = (store.clone(), lease.clone());
        saves.push(tokio::spawn(async move {
            store
                .save_step(&lease, 1, "step", &output(&index.to_string()))
                .await
        }));
    }
    let mut accepted = 0;
    for task in saves {
        match task.await.unwrap() {
            Ok(()) => accepted += 1,
            Err(Error::StepNondeterminism { .. }) => {}
            Err(other) => panic!("{other:?}"),
        }
    }
    assert_eq!(accepted, 1);
});

all_dbs!(racing_processes_never_claim_the_same_workflow, backend, {
    let db = Arc::new(TestDb::create(backend).await);
    let store = db.open().await;
    for index in 0..50 {
        start(&store, &workflow(&format!("wf-{index}"), EXPIRED)).await;
    }

    let mut racers = Vec::new();
    for _ in 0..8 {
        let db = db.clone();
        racers.push(tokio::spawn(async move {
            let racer = db.open().await;
            let mut won = Vec::new();
            loop {
                let claimed = racer.reclaim(&names(), 3, LONG).await.unwrap();
                if claimed.is_empty() {
                    return won;
                }
                won.extend(claimed.into_iter().map(|claim| claim.lease.workflow_id));
            }
        }));
    }

    let mut all = Vec::new();
    for racer in racers {
        all.extend(racer.await.unwrap());
    }
    let total = all.len();
    all.sort();
    all.dedup();
    assert_eq!(total, 50, "a workflow was claimed twice or missed");
    assert_eq!(all.len(), 50);
});

all_dbs!(
    two_copies_racing_for_one_expired_workflow_have_one_owner,
    backend,
    {
        let db = TestDb::create(backend).await;
        let first = db.open().await;
        let second = db.open().await;
        let names = names();
        for round in 0..20 {
            let id = format!("wf-{round}");
            start(&first, &workflow(&id, EXPIRED)).await;
            let (a, b) = tokio::join!(
                first.reclaim(&names, 1, LONG),
                second.reclaim(&names, 1, LONG)
            );
            let claims: Vec<_> = a.unwrap().into_iter().chain(b.unwrap()).collect();
            assert_eq!(claims.len(), 1, "round {round}: {claims:?}");
            assert_eq!(claims[0].lease.workflow_id, id);
            assert_eq!(claims[0].recovery_attempts, 2);
            first
                .finish_workflow(&claims[0].lease, &Outcome::Success("1".to_owned()))
                .await
                .unwrap();
        }
    }
);

all_dbs!(dropping_an_operation_rolls_back_its_transaction, backend, {
    let (_db, store) = setup(backend).await;
    create_scratch(store.pool()).await;
    let holder = store.clone();
    let task =
        tokio::spawn(async move { hold_uncommitted_row(&holder, Duration::from_secs(60)).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    task.abort();
    let _ = task.await;
    let mut rows = 1;
    for _ in 0..100 {
        rows = scratch_rows(store.pool()).await;
        if rows == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(rows, 0);
});

all_dbs!(contending_workers_finish_everything_once, backend, {
    let db = Arc::new(TestDb::create(backend).await);
    let store = db.open().await;
    for index in 0..100 {
        start(&store, &workflow(&format!("wf-{index}"), EXPIRED)).await;
    }

    let mut workers = Vec::new();
    for _ in 0..4 {
        let db = db.clone();
        workers.push(tokio::spawn(async move {
            let worker = db.open().await;
            let mut finished = 0;
            loop {
                let claimed = worker.reclaim(&names(), 5, LONG).await.unwrap();
                if claimed.is_empty() {
                    return finished;
                }
                for claim in claimed {
                    for step in 1..=3 {
                        worker
                            .save_step(&claim.lease, step, "step", &output("1"))
                            .await
                            .unwrap();
                    }
                    worker
                        .finish_workflow(&claim.lease, &Outcome::Success("1".to_owned()))
                        .await
                        .unwrap();
                    finished += 1;
                }
            }
        }));
    }
    let mut total = 0;
    for worker in workers {
        total += worker.await.unwrap();
    }
    assert_eq!(total, 100);
    for index in 0..100 {
        let state = store
            .get_workflow(&format!("wf-{index}"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state.status, Status::Success);
    }
});

all_dbs!(state_survives_reopening_the_database, backend, {
    let db = TestDb::create(backend).await;
    {
        let store = db.open().await;
        let lease = start(&store, &workflow("a", EXPIRED)).await;
        store
            .save_step(&lease, 1, "step", &output("1"))
            .await
            .unwrap();
    }
    let store = db.open().await;
    assert_eq!(
        store.load_step("a", 1, "step").await.unwrap(),
        Some(output("1"))
    );
    assert_eq!(store.reclaim(&names(), 10, LONG).await.unwrap().len(), 1);
});

all_dbs!(migrate_can_run_twice, backend, {
    let (_db, store) = setup(backend).await;
    store.migrate().await.unwrap();
});

all_dbs!(large_and_unicode_values_round_trip_exactly, backend, {
    let (_db, store) = setup(backend).await;
    let lease = start(&store, &workflow("a", LONG)).await;
    let large = format!("\"{}日本語🦀\"", "x".repeat(10 * 1024 * 1024));
    store
        .save_step(&lease, 1, "step", &output(&large))
        .await
        .unwrap();
    assert_eq!(
        store.load_step("a", 1, "step").await.unwrap(),
        Some(output(&large))
    );
    store
        .finish_workflow(&lease, &Outcome::Success(large.clone()))
        .await
        .unwrap();
    assert_eq!(
        store.get_workflow("a").await.unwrap().unwrap().output,
        Some(large)
    );
});

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run by crash_keeps_committed_state_and_drops_the_rest"]
async fn crash_child() {
    let store = Store::connect(&std::env::var("DBOS_CRASH_URL").unwrap())
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let lease = start(&store, &workflow("a", EXPIRED)).await;
    store
        .save_step(&lease, 1, "step", &output("1"))
        .await
        .unwrap();
    create_scratch(store.pool()).await;
    let holder = store.clone();
    tokio::spawn(async move { hold_uncommitted_row(&holder, Duration::from_secs(60)).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    std::process::abort();
}

all_dbs!(crash_keeps_committed_state_and_drops_the_rest, backend, {
    let db = TestDb::create(backend).await;
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["crash_child", "--exact", "--ignored", "--nocapture"])
        .env("DBOS_CRASH_URL", &db.url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "the child should have aborted");

    let store = db.open().await;
    assert_eq!(
        store.load_step("a", 1, "step").await.unwrap(),
        Some(output("1"))
    );
    let mut rows = scratch_rows(store.pool()).await;
    for _ in 0..100 {
        if rows == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        rows = scratch_rows(store.pool()).await;
    }
    assert_eq!(rows, 0);
    let claimed = store.reclaim(&names(), 10, LONG).await.unwrap().remove(0);
    assert_eq!(claimed.recovery_attempts, 2);
    store
        .save_step(&claimed.lease, 2, "next", &output("2"))
        .await
        .unwrap();
    store
        .finish_workflow(&claimed.lease, &Outcome::Success("2".to_owned()))
        .await
        .unwrap();
});

mod mysql_only {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wrong_password_reports_the_real_cause_quickly() {
        let started = std::time::Instant::now();
        let url = mysql_admin_url().replace("dbos-test-root", "wrong-secret");
        let message = Store::connect(&url).await.unwrap_err().to_string();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "took {:?}",
            started.elapsed()
        );
        assert!(message.contains("Access denied"), "{message}");
        assert!(!message.contains("wrong-secret"), "{message}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sessions_use_the_required_settings() {
        let (_db, store) = setup(Backend::MySql).await;
        // Every connection in the pool, not only the first.
        let mut held = Vec::new();
        for _ in 0..3 {
            let mut conn = store.pool().acquire().await.unwrap();
            let row = sqlx::query(
                "SELECT @@transaction_isolation, @@session.time_zone, @@session.sql_mode",
            )
            .fetch_one(&mut *conn)
            .await
            .unwrap();
            assert_eq!(row.get::<String, _>(0), "READ-COMMITTED");
            assert_eq!(row.get::<String, _>(1), "+00:00");
            let mode: String = row.get(2);
            assert!(mode.contains("STRICT_ALL_TABLES"), "{mode}");
            held.push(conn);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn oversized_values_fail_clearly_and_store_nothing() {
        let (_db, store) = setup(Backend::MySql).await;
        let lease = start(&store, &workflow("a", LONG)).await;
        let huge = "x".repeat(70 * 1024 * 1024);
        let error = store
            .save_step(&lease, 1, "step", &output(&huge))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::ValueTooLarge { .. }), "{error:?}");
        assert_eq!(store.load_step("a", 1, "step").await.unwrap(), None);
        store.renew_lease(&lease, LONG).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lease_renewal_is_not_starved_by_busy_work() {
        let (_db, store) = setup(Backend::MySql).await;
        let lease = start(&store, &workflow("a", LONG)).await;
        let mut holders = Vec::new();
        for _ in 0..12 {
            let holder = store.clone();
            holders.push(tokio::spawn(async move {
                sqlx::query("SELECT SLEEP(1.5)")
                    .execute(holder.pool())
                    .await
            }));
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let started = std::time::Instant::now();
        store.renew_lease(&lease, LONG).await.unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(800),
            "took {:?}",
            started.elapsed()
        );
        for holder in holders {
            holder.await.unwrap().unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_real_deadlock_is_retried() {
        let db = TestDb::create(Backend::MySql).await;
        let store = db.open().await;
        let lease = start(&store, &workflow("a", LONG)).await;

        let mut blocker = MySqlConnection::connect(&db.url).await.unwrap();
        sqlx::raw_sql("SET SESSION foreign_key_checks = 0")
            .execute(&mut blocker)
            .await
            .unwrap();
        sqlx::raw_sql("BEGIN").execute(&mut blocker).await.unwrap();
        sqlx::raw_sql("INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, serialization, completed_at_epoch_ms) VALUES ('a', 1, 'blocker', 'py_pickle', 0)")
            .execute(&mut blocker)
            .await
            .unwrap();
        for index in 0..200 {
            sqlx::query("INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, serialization, completed_at_epoch_ms) VALUES ('weight', ?, 'w', 'py_pickle', 0)")
                .bind(index)
                .execute(&mut blocker)
                .await
                .unwrap();
        }

        let saver = store.clone();
        let saving =
            tokio::spawn(async move { saver.save_step(&lease, 1, "step", &output("1")).await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        sqlx::query("SELECT 1 FROM workflow_status WHERE workflow_uuid = 'a' FOR UPDATE")
            .fetch_optional(&mut blocker)
            .await
            .unwrap();
        sqlx::raw_sql("ROLLBACK")
            .execute(&mut blocker)
            .await
            .unwrap();

        saving.await.unwrap().unwrap();
        assert!(store.transient_retries() >= 1, "no retry happened");
        assert_eq!(
            store.load_step("a", 1, "step").await.unwrap(),
            Some(output("1"))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn step_save_waits_for_a_takeover_in_progress() {
        let db = TestDb::create(Backend::MySql).await;
        let store = db.open().await;
        let stale = start(&store, &workflow("a", LONG)).await;

        let mut takeover = MySqlConnection::connect(&db.url).await.unwrap();
        sqlx::raw_sql("BEGIN").execute(&mut takeover).await.unwrap();
        sqlx::query("SELECT 1 FROM workflow_status WHERE workflow_uuid = 'a' FOR UPDATE")
            .fetch_one(&mut takeover)
            .await
            .unwrap();

        let saver = store.clone();
        let lease = stale.clone();
        let saving =
            tokio::spawn(async move { saver.save_step(&lease, 1, "step", &output("1")).await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        sqlx::query(
            "UPDATE workflow_status SET lease_token = 'new-owner' WHERE workflow_uuid = 'a'",
        )
        .execute(&mut takeover)
        .await
        .unwrap();
        sqlx::raw_sql("COMMIT")
            .execute(&mut takeover)
            .await
            .unwrap();

        let result = saving.await.unwrap();
        assert!(matches!(result, Err(Error::LeaseLost(_))), "{result:?}");
        assert_eq!(store.load_step("a", 1, "step").await.unwrap(), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn killed_connections_are_replaced() {
        let db = TestDb::create(Backend::MySql).await;
        let store = db.open().await;
        start(&store, &workflow("a", LONG)).await;

        let mut admin = MySqlConnection::connect(&db.url).await.unwrap();
        let database = db.mysql_database.as_deref().unwrap();
        let ids: Vec<u64> = sqlx::query_scalar(
            "SELECT id FROM information_schema.processlist WHERE db = ? AND id <> CONNECTION_ID()",
        )
        .bind(database)
        .fetch_all(&mut admin)
        .await
        .unwrap();
        assert!(!ids.is_empty());
        for id in ids {
            sqlx::raw_sql(sqlx::AssertSqlSafe(format!("KILL {id}")))
                .execute(&mut admin)
                .await
                .unwrap();
        }
        assert!(store.get_workflow("a").await.unwrap().is_some());
    }
}

mod sqlite_only {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sessions_use_the_required_settings() {
        let (_db, store) = setup(Backend::Sqlite).await;
        let mut conn = store.pool().acquire().await.unwrap();
        let pragma = async |conn: &mut sqlx::AnyConnection, name: &str| -> String {
            let row = sqlx::query(sqlx::AssertSqlSafe(format!("PRAGMA {name}")))
                .fetch_one(conn)
                .await
                .unwrap();
            row.try_get::<String, _>(0)
                .unwrap_or_else(|_| row.get::<i64, _>(0).to_string())
        };
        assert_eq!(pragma(&mut conn, "journal_mode").await, "wal");
        assert_eq!(pragma(&mut conn, "synchronous").await, "2");
        assert_eq!(pragma(&mut conn, "foreign_keys").await, "1");
        assert_eq!(pragma(&mut conn, "busy_timeout").await, "30000");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn paths_with_spaces_and_relative_paths_work() {
        let dir = tempfile::TempDir::new().unwrap();
        let spaced = dir.path().join("with space");
        std::fs::create_dir(&spaced).unwrap();
        let url = format!("sqlite://{}", spaced.join("a b.sqlite").display());
        let store = Store::connect(&url).await.unwrap();
        store.migrate().await.unwrap();
        start(&store, &workflow("a", LONG)).await;
        assert!(spaced.join("a b.sqlite").exists());

        let relative = format!("dwos-relative-{}.sqlite", uuid::Uuid::new_v4().simple());
        let store = Store::connect(&format!("sqlite://{relative}"))
            .await
            .unwrap();
        store.migrate().await.unwrap();
        store.pool().close().await;
        let exists = std::path::Path::new(&relative).exists();
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{relative}{suffix}"));
        }
        assert!(exists, "the relative file was not created");
    }
}

mod postgres_only {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgresql_scheme_is_accepted() {
        let db = TestDb::create(Backend::Postgres).await;
        let url = db.url.replacen("postgres://", "postgresql://", 1);
        let store = Store::connect(&url).await.unwrap();
        assert_eq!(store.backend(), "postgres");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wrong_password_reports_the_real_cause_quickly() {
        let started = std::time::Instant::now();
        let url = postgres_admin_url().replace("dbos-test-root", "wrong-secret");
        let message = Store::connect(&url).await.unwrap_err().to_string();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "took {:?}",
            started.elapsed()
        );
        assert!(
            message.contains("password authentication failed"),
            "{message}"
        );
        assert!(!message.contains("wrong-secret"), "{message}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sessions_use_the_required_settings() {
        let (_db, store) = setup(Backend::Postgres).await;
        let mut held = Vec::new();
        for _ in 0..3 {
            let mut conn = store.pool().acquire().await.unwrap();
            let timeout: String = sqlx::query_scalar("SELECT current_setting('lock_timeout')")
                .fetch_one(&mut *conn)
                .await
                .unwrap();
            assert_eq!(timeout, "10s");
            held.push(conn);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lease_renewal_is_not_starved_by_busy_work() {
        let (_db, store) = setup(Backend::Postgres).await;
        let lease = start(&store, &workflow("a", LONG)).await;
        let mut holders = Vec::new();
        for _ in 0..12 {
            let holder = store.clone();
            holders.push(tokio::spawn(async move {
                sqlx::query("SELECT 1 FROM (SELECT pg_sleep(1.5)) AS slept")
                    .execute(holder.pool())
                    .await
            }));
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let started = std::time::Instant::now();
        store.renew_lease(&lease, LONG).await.unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(800),
            "took {:?}",
            started.elapsed()
        );
        for holder in holders {
            holder.await.unwrap().unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_real_deadlock_is_retried() {
        let db = TestDb::create(Backend::Postgres).await;
        let store = db.open().await;
        let lease = start(&store, &workflow("a", LONG)).await;

        // Without foreign key checks, the blocker's insert takes no lock on the workflow row.
        let mut blocker = PgConnection::connect(&db.url).await.unwrap();
        sqlx::raw_sql("SET session_replication_role = replica; BEGIN")
            .execute(&mut blocker)
            .await
            .unwrap();
        sqlx::raw_sql("INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, serialization, completed_at_epoch_ms) VALUES ('a', 1, 'blocker', 'x', 0)")
            .execute(&mut blocker)
            .await
            .unwrap();

        let saver = store.clone();
        let saving =
            tokio::spawn(async move { saver.save_step(&lease, 1, "step", &output("1")).await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        // The saver holds the row and waits on the blocker's insert; now the blocker waits on the
        // row. The saver has waited longest, so its deadlock check fires first and it is the one
        // aborted.
        sqlx::query("SELECT 1 FROM workflow_status WHERE workflow_uuid = 'a' FOR UPDATE")
            .fetch_optional(&mut blocker)
            .await
            .unwrap();
        sqlx::raw_sql("ROLLBACK")
            .execute(&mut blocker)
            .await
            .unwrap();

        saving.await.unwrap().unwrap();
        assert!(store.transient_retries() >= 1, "no retry happened");
        assert_eq!(
            store.load_step("a", 1, "step").await.unwrap(),
            Some(output("1"))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn step_save_waits_for_a_takeover_in_progress() {
        let db = TestDb::create(Backend::Postgres).await;
        let store = db.open().await;
        let stale = start(&store, &workflow("a", LONG)).await;

        let mut takeover = PgConnection::connect(&db.url).await.unwrap();
        sqlx::raw_sql("BEGIN").execute(&mut takeover).await.unwrap();
        sqlx::query("SELECT 1 FROM workflow_status WHERE workflow_uuid = 'a' FOR UPDATE")
            .fetch_one(&mut takeover)
            .await
            .unwrap();

        let saver = store.clone();
        let lease = stale.clone();
        let saving =
            tokio::spawn(async move { saver.save_step(&lease, 1, "step", &output("1")).await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        sqlx::query(
            "UPDATE workflow_status SET lease_token = 'new-owner' WHERE workflow_uuid = 'a'",
        )
        .execute(&mut takeover)
        .await
        .unwrap();
        sqlx::raw_sql("COMMIT")
            .execute(&mut takeover)
            .await
            .unwrap();

        let result = saving.await.unwrap();
        assert!(matches!(result, Err(Error::LeaseLost(_))), "{result:?}");
        assert_eq!(store.load_step("a", 1, "step").await.unwrap(), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn killed_connections_are_replaced() {
        let db = TestDb::create(Backend::Postgres).await;
        let store = db.open().await;
        start(&store, &workflow("a", LONG)).await;

        let mut admin = PgConnection::connect(&db.url).await.unwrap();
        let killed: Vec<bool> = sqlx::query_scalar(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()",
        )
        .fetch_all(&mut admin)
        .await
        .unwrap();
        assert!(!killed.is_empty());
        assert!(store.get_workflow("a").await.unwrap().is_some());
    }
}
