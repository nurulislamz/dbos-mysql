//! Forced interleavings of queue admission against renewals, takeovers and finishes.
//!
//! Nothing here sleeps to win a race. A claim is paused at a known statement by a table lock (`Gate`)
//! or a row lock, and `wait_blocked` asks the server that it is really waiting before the test goes on.
//! SQLite takes the write lock in `BEGIN IMMEDIATE`, so these races cannot exist there. Its branches
//! run each order one after the other and check the same end state.

use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sqlx::{AnyConnection, AssertSqlSafe, Connection};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use yado_core::{
    Begin, Claimed, Engine, EngineConfig, Error, Lease, Outcome, Run, Status, StepDecision,
};

#[macro_use]
mod common;
use common::*;

// Text that identifies one statement in the server's list of running queries.
const LOCK_QUEUE: &str = "SELECT concurrency FROM workflow_queues";
const REVOKE: &str = "lease_token IS NOT NULL AND claim_after <=";
const DEADLINE_CANCEL: &str = "WHERE status = 'PENDING' AND workflow_uuid IN (SELECT workflow_uuid FROM workflow_queue_entries";
const LOCK_CANDIDATE: &str = "AND lease_token IS NULL AND claim_after <=";
const RENEW: &str = "SET claim_after = ";
const RELEASE: &str = "recovery_attempts = recovery_attempts - 1";
const CANCEL: &str = "status IN ('PENDING', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED')";
const CANCEL_IF_EXPIRED: &str = "AND status = 'PENDING' AND workflow_uuid IN";
const READ_STEP: &str = "FROM operation_outputs";

const WAIT_POLLS: u32 = 2000;
const WAIT_POLL: Duration = Duration::from_millis(5);

fn is_sqlite(backend: Backend) -> bool {
    matches!(backend, Backend::Sqlite)
}

fn begin(id: &str) -> Begin<'_> {
    Begin {
        name: "wf",
        input: "{}",
        workflow_id: Some(id),
        parent: None,
        max_recovery_attempts: 100,
        timeout_ms: None,
        ignore_propagated_deadline: false,
    }
}

fn config() -> EngineConfig {
    EngineConfig {
        lease: LONG,
        pickle: false,
        ..EngineConfig::default()
    }
}

async fn engine(db: &TestDb) -> Engine {
    Engine::connect(&db.url, config()).await.unwrap()
}

fn run(claimed: &Claimed) -> Run {
    Run::new(
        claimed.lease.clone(),
        claimed.serialization.clone(),
        claimed.deadline_epoch_ms,
    )
}

async fn make_queue(engine: &Engine, name: &str, concurrency: u32) {
    engine
        .ensure_queue(name, NonZeroU32::new(concurrency).unwrap())
        .await
        .unwrap();
}

async fn enqueue(engine: &Engine, queue: &str, ids: &[&str]) {
    for id in ids {
        engine.enqueue(queue, begin(id)).await.unwrap();
    }
}

fn claim(engine: &Engine, queue: &str) -> JoinHandle<Result<Vec<Claimed>, Error>> {
    let (engine, queue) = (engine.clone(), queue.to_owned());
    tokio::spawn(async move { engine.claim_queue(&queue, &names()).await })
}

fn renew(engine: &Engine, lease: &Lease) -> JoinHandle<Result<(), Error>> {
    let (engine, lease) = (engine.clone(), lease.clone());
    tokio::spawn(async move { engine.store().renew_lease(&lease, LONG).await })
}

fn ids(claimed: &[Claimed]) -> Vec<&str> {
    claimed
        .iter()
        .map(|row| row.lease.workflow_id.as_str())
        .collect()
}

/// A connection of the test's own, with the same isolation level as the store's connections.
async fn connection(db: &TestDb, backend: Backend) -> AnyConnection {
    let mut conn = AnyConnection::connect(&db.url).await.unwrap();
    if matches!(backend, Backend::MySql) {
        exec(
            &mut conn,
            "SET SESSION transaction_isolation = 'READ-COMMITTED'",
        )
        .await;
    }
    conn
}

async fn exec(conn: &mut AnyConnection, sql: &str) {
    sqlx::raw_sql(AssertSqlSafe(sql.to_owned()))
        .execute(conn)
        .await
        .unwrap();
}

async fn scalar(conn: &mut AnyConnection, sql: &str) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(sql.to_owned()))
        .fetch_one(conn)
        .await
        .unwrap()
}

/// The owners that admission counts, read in one statement and so from one snapshot.
fn owners_sql(queue: &str) -> String {
    format!(
        "SELECT COUNT(*) FROM workflow_status WHERE status = 'PENDING' AND lease_token IS NOT NULL \
         AND workflow_uuid IN (SELECT workflow_uuid FROM workflow_queue_entries WHERE queue_name = '{queue}')"
    )
}

async fn owners(db: &TestDb, backend: Backend, queue: &str) -> i64 {
    scalar(&mut connection(db, backend).await, &owners_sql(queue)).await
}

/// Puts a lease in the past without waiting for it to pass.
async fn expire(db: &TestDb, backend: Backend, id: &str) {
    exec(
        &mut connection(db, backend).await,
        &format!("UPDATE workflow_status SET claim_after = 1 WHERE workflow_uuid = '{id}'"),
    )
    .await;
}

async fn attempts(engine: &Engine, id: &str) -> u32 {
    engine
        .store()
        .get_workflow(id)
        .await
        .unwrap()
        .unwrap()
        .recovery_attempts
}

async fn status(engine: &Engine, id: &str) -> Status {
    engine
        .store()
        .get_workflow(id)
        .await
        .unwrap()
        .unwrap()
        .status
}

/// Waits until the server reports a session of this database waiting on a lock in that statement.
async fn wait_blocked(db: &TestDb, backend: Backend, needle: &str) {
    if blocked(db, backend, needle, WAIT_POLLS).await {
        return;
    }
    let sessions = match backend {
        Backend::Postgres => {
            "SELECT CONCAT(wait_event_type, ' | ', state, ' | ', query) FROM pg_stat_activity WHERE datname = current_database()"
        }
        _ => {
            "SELECT CONCAT(COALESCE(PROCESSLIST_STATE, '-'), ' | ', COALESCE(PROCESSLIST_INFO, '-')) FROM performance_schema.threads WHERE PROCESSLIST_DB = DATABASE()"
        }
    };
    let rows: Vec<Option<String>> = sqlx::query_scalar(sessions)
        .fetch_all(&mut connection(db, backend).await)
        .await
        .unwrap();
    panic!("nothing blocked on: {needle}\nsessions: {rows:#?}");
}

async fn blocked(db: &TestDb, backend: Backend, needle: &str, polls: u32) -> bool {
    let needle = needle.replace('\'', "''");
    let sql = match backend {
        Backend::Postgres => format!(
            "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() \
             AND pid <> pg_backend_pid() AND wait_event_type = 'Lock' AND query LIKE '%{needle}%'"
        ),
        // Row waits show in data_locks. Table waits and GET_LOCK waits show in the thread's state.
        Backend::MySql => format!(
            "SELECT COUNT(*) FROM performance_schema.threads t \
             WHERE t.PROCESSLIST_DB = DATABASE() AND t.PROCESSLIST_INFO LIKE '%{needle}%' \
             AND t.PROCESSLIST_ID <> CONNECTION_ID() \
             AND (t.PROCESSLIST_STATE IN ('Waiting for table metadata lock', 'User lock') OR EXISTS \
             (SELECT 1 FROM performance_schema.data_locks l WHERE l.THREAD_ID = t.THREAD_ID AND l.LOCK_STATUS = 'WAITING'))"
        ),
        Backend::Sqlite => panic!("SQLite has no lock list to read"),
    };
    let mut conn = connection(db, backend).await;
    for _ in 0..polls {
        if scalar(&mut conn, &sql).await > 0 {
            return true;
        }
        tokio::time::sleep(WAIT_POLL).await;
    }
    false
}

/// LOCK TABLES also locks foreign key parents, which claims and renewals use, and a child insert takes a
/// shared lock on its parent. These tests are about workflow_status row locks, so the checks can go.
async fn drop_foreign_keys(conn: &mut AnyConnection, table: &str) {
    let foreign_keys: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS WHERE \
         TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table}' AND CONSTRAINT_TYPE = 'FOREIGN KEY'"
    )))
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    for key in foreign_keys {
        exec(conn, &format!("ALTER TABLE {table} DROP FOREIGN KEY {key}")).await;
    }
}

/// An exclusive table lock on a separate connection. It stops the first statement that reads the
/// table, before that statement takes a snapshot or a row lock, and leaves other tables alone.
struct Gate {
    conn: AnyConnection,
    backend: Backend,
}

impl Gate {
    async fn close(db: &TestDb, backend: Backend, table: &str) -> Self {
        let mut conn = connection(db, backend).await;
        match backend {
            Backend::MySql => {
                drop_foreign_keys(&mut conn, table).await;
                exec(&mut conn, &format!("LOCK TABLES {table} WRITE")).await;
            }
            Backend::Postgres => {
                exec(
                    &mut conn,
                    &format!("BEGIN; LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"),
                )
                .await;
            }
            Backend::Sqlite => panic!("SQLite has no table locks"),
        }
        Self { conn, backend }
    }

    async fn open(mut self) {
        let unlock = match self.backend {
            Backend::MySql => "UNLOCK TABLES",
            _ => "COMMIT",
        };
        exec(&mut self.conn, unlock).await;
    }
}

/// Stops a claim after it read its cutoff and before its revoke starts.
async fn before_revoke(db: &TestDb, backend: Backend) -> Gate {
    Gate::close(db, backend, "workflow_queue_entries").await
}

/// Stops a claim after its revoke ran, while it holds the row locks of revoked owners.
async fn after_revoke(db: &TestDb, backend: Backend) -> Gate {
    Gate::close(db, backend, "workflow_timeouts").await
}

/// Prepares a stop inside the claim loop, after the count, at FIFO head `id`. The workflow needs a
/// relative timeout. Call `wait_after_count` once the claim runs, and roll this back to let it go.
///
/// A row lock on `id` is enough on Postgres. On MySQL the revoke reads every queue member by primary
/// key with a lock, so that would stop the claim before its count. There the stop is an uncommitted
/// deadline row, which the claim's insert-if-absent waits for.
async fn stop_after_count(db: &TestDb, backend: Backend, id: &str) -> AnyConnection {
    if !matches!(backend, Backend::MySql) {
        return lock_row(db, backend, id).await;
    }
    let mut conn = connection(db, backend).await;
    drop_foreign_keys(&mut conn, "workflow_timeouts").await;
    exec(
        &mut conn,
        &format!(
            "BEGIN; INSERT INTO workflow_timeouts (workflow_uuid, timeout_ms, deadline_epoch_ms) \
             VALUES ('{id}', NULL, 9000000000000)"
        ),
    )
    .await;
    conn
}

async fn wait_after_count(db: &TestDb, backend: Backend) {
    let needle = match backend {
        Backend::MySql => "INSERT INTO workflow_timeouts",
        _ => LOCK_CANDIDATE,
    };
    wait_blocked(db, backend, needle).await;
}

async fn enqueue_timed(engine: &Engine, queue: &str, id: &str) {
    let mut timed = begin(id);
    timed.timeout_ms = Some(600_000);
    engine.enqueue(queue, timed).await.unwrap();
}

/// Holds one workflow row lock in an open transaction.
async fn lock_row(db: &TestDb, backend: Backend, id: &str) -> AnyConnection {
    let mut conn = connection(db, backend).await;
    exec(
        &mut conn,
        &format!("BEGIN; SELECT 1 FROM workflow_status WHERE workflow_uuid = '{id}' FOR UPDATE"),
    )
    .await;
    conn
}

/// Concurrency 1, owner `a` holding an expired lease, and `b` waiting.
async fn expired_owner(db: &TestDb, backend: Backend) -> (Engine, Engine, Run) {
    let owner = engine(db).await;
    let claimer = engine(db).await;
    make_queue(&owner, "q", 1).await;
    enqueue(&owner, "q", &["a", "b"]).await;
    let first = owner.claim_queue("q", &names()).await.unwrap();
    assert_eq!(ids(&first), ["a"]);
    expire(db, backend, "a").await;
    (owner, claimer, run(&first[0]))
}

/// The owner kept its slot: same attempt, a working token, and nobody else admitted.
async fn assert_kept_slot(owner: &Engine, run: &Run, db: &TestDb, backend: Backend) {
    assert_eq!(
        owner.check_step(run, 1, "s").await.unwrap(),
        StepDecision::Run
    );
    owner
        .store()
        .renew_lease(&lease_of(run), LONG)
        .await
        .unwrap();
    assert_eq!(attempts(owner, "a").await, 1);
    assert_eq!(owners(db, backend, "q").await, 1);
    assert_eq!(attempts(owner, "b").await, 0);
}

/// The owner lost its slot and `a` was admitted again, which spends exactly one attempt.
async fn assert_lost_slot(
    owner: &Engine,
    run: &Run,
    claimed: &[Claimed],
    db: &TestDb,
    backend: Backend,
) {
    assert_eq!(ids(claimed), ["a"]);
    assert_eq!(claimed[0].recovery_attempts, 2);
    assert_ne!(claimed[0].lease.token, run.token());
    assert!(matches!(
        owner.check_step(run, 1, "s").await,
        Err(Error::LeaseLost(_))
    ));
    assert!(matches!(
        owner.store().renew_lease(&lease_of(run), LONG).await,
        Err(Error::LeaseLost(_))
    ));
    assert_eq!(owners(db, backend, "q").await, 1);
}

fn lease_of(run: &Run) -> Lease {
    Lease {
        workflow_id: run.workflow_id().to_owned(),
        token: run.token().to_owned(),
    }
}

// The cutoff is already read and is older than the renewal, so the revoke must see the new expiry.
all_dbs!(
    renewal_committed_between_cutoff_and_revoke_keeps_the_slot,
    backend,
    {
        let db = TestDb::create(backend).await;
        let (owner, claimer, held) = expired_owner(&db, backend).await;
        if is_sqlite(backend) {
            owner
                .store()
                .renew_lease(&lease_of(&held), LONG)
                .await
                .unwrap();
            assert!(claimer.claim_queue("q", &names()).await.unwrap().is_empty());
        } else {
            let gate = before_revoke(&db, backend).await;
            let claiming = claim(&claimer, "q");
            wait_blocked(&db, backend, REVOKE).await;
            renew(&owner, &lease_of(&held)).await.unwrap().unwrap();
            gate.open().await;
            assert!(claiming.await.unwrap().unwrap().is_empty());
        }
        assert_kept_slot(&owner, &held, &db, backend).await;
    }
);

// Postgres re-checks the row after the lock wait. MySQL's semi-consistent read matches the old
// committed row, waits, then re-reads. Both must see the renewed expiry and leave the owner alone.
all_dbs!(
    revoke_waiting_on_an_uncommitted_renewal_rechecks_it,
    backend,
    {
        let db = TestDb::create(backend).await;
        let (owner, claimer, held) = expired_owner(&db, backend).await;
        if is_sqlite(backend) {
            owner
                .store()
                .renew_lease(&lease_of(&held), LONG)
                .await
                .unwrap();
            assert!(claimer.claim_queue("q", &names()).await.unwrap().is_empty());
        } else {
            let mut renewal = connection(&db, backend).await;
            exec(&mut renewal, "BEGIN").await;
            let expiry = owner.store().now_ms().await.unwrap() + 600_000;
            exec(
                &mut renewal,
                &format!(
                    "UPDATE workflow_status SET claim_after = {expiry} WHERE workflow_uuid = 'a' \
                 AND lease_token = '{}' AND status = 'PENDING'",
                    held.token()
                ),
            )
            .await;
            let claiming = claim(&claimer, "q");
            wait_blocked(&db, backend, REVOKE).await;
            exec(&mut renewal, "COMMIT").await;
            assert!(claiming.await.unwrap().unwrap().is_empty());
        }
        assert_kept_slot(&owner, &held, &db, backend).await;
    }
);

all_dbs!(
    revoke_waiting_on_a_renewal_that_rolls_back_takes_the_slot,
    backend,
    {
        let db = TestDb::create(backend).await;
        let (owner, claimer, held) = expired_owner(&db, backend).await;
        let claimed = if is_sqlite(backend) {
            claimer.claim_queue("q", &names()).await.unwrap()
        } else {
            let mut renewal = connection(&db, backend).await;
            exec(&mut renewal, "BEGIN").await;
            exec(
                &mut renewal,
                &format!(
                "UPDATE workflow_status SET claim_after = 9000000000000 WHERE workflow_uuid = 'a' \
                 AND lease_token = '{}' AND status = 'PENDING'",
                held.token()
            ),
            )
            .await;
            let claiming = claim(&claimer, "q");
            wait_blocked(&db, backend, REVOKE).await;
            exec(&mut renewal, "ROLLBACK").await;
            claiming.await.unwrap().unwrap()
        };
        assert_lost_slot(&owner, &held, &claimed, &db, backend).await;
    }
);

// The renewal waits on the row lock of a revoke that has run but not committed.
all_dbs!(renewal_waiting_on_a_revoke_is_refused, backend, {
    let db = TestDb::create(backend).await;
    let (owner, claimer, held) = expired_owner(&db, backend).await;
    let (claimed, renewed) = if is_sqlite(backend) {
        let claimed = claimer.claim_queue("q", &names()).await.unwrap();
        (
            claimed,
            owner.store().renew_lease(&lease_of(&held), LONG).await,
        )
    } else {
        let gate = after_revoke(&db, backend).await;
        let claiming = claim(&claimer, "q");
        wait_blocked(&db, backend, DEADLINE_CANCEL).await;
        let renewing = renew(&owner, &lease_of(&held));
        wait_blocked(&db, backend, RENEW).await;
        gate.open().await;
        (claiming.await.unwrap().unwrap(), renewing.await.unwrap())
    };
    assert!(matches!(renewed, Err(Error::LeaseLost(_))));
    assert_lost_slot(&owner, &held, &claimed, &db, backend).await;
});

// Real renewal and real revoke queue up behind a plain row lock, in both arrival orders. The server
// picks who goes first. Whatever it picks, success and the slot must go together.
all_dbs!(
    renewal_and_revoke_behind_one_lock_agree_in_either_order,
    backend,
    {
        if is_sqlite(backend) {
            return;
        }
        for renewal_first in [true, false] {
            let db = TestDb::create(backend).await;
            let (owner, claimer, held) = expired_owner(&db, backend).await;
            let mut lock = lock_row(&db, backend, "a").await;
            let (renewing, claiming) = if renewal_first {
                let renewing = renew(&owner, &lease_of(&held));
                wait_blocked(&db, backend, RENEW).await;
                let claiming = claim(&claimer, "q");
                wait_blocked(&db, backend, REVOKE).await;
                (renewing, claiming)
            } else {
                let claiming = claim(&claimer, "q");
                wait_blocked(&db, backend, REVOKE).await;
                let renewing = renew(&owner, &lease_of(&held));
                wait_blocked(&db, backend, RENEW).await;
                (renewing, claiming)
            };
            exec(&mut lock, "COMMIT").await;
            let claimed = claiming.await.unwrap().unwrap();
            match renewing.await.unwrap() {
                Ok(()) => {
                    assert!(claimed.is_empty(), "renewed, but also lost the slot");
                    assert_kept_slot(&owner, &held, &db, backend).await;
                }
                Err(Error::LeaseLost(_)) => {
                    assert_lost_slot(&owner, &held, &claimed, &db, backend).await;
                }
                Err(error) => panic!("unexpected renewal error: {error}"),
            }
        }
    }
);

// shortcut: scanning through a view is the only way to pause inside one MySQL statement, drop it if a
// statement-level failpoint ever exists.
//
// The recovery scan reads queue membership through a subquery, which uses the statement's read view.
// The row itself is read by a locking read, which sees the latest commit. A queue member committed
// after the view was made therefore looks like an ordinary workflow unless membership is re-read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_recovery_scan_does_not_claim_a_queue_member_committed_after_its_read_view() {
    let backend = Backend::MySql;
    let db = TestDb::create(backend).await;
    let engine = engine(&db).await;
    make_queue(&engine, "q", 1).await;
    enqueue(&engine, "q", &["a-member"]).await;
    let mut setup = connection(&db, backend).await;
    let lock_name = format!("{}.scan", db.mysql_database.as_deref().unwrap());
    exec(
        &mut setup,
        &format!(
            "UPDATE workflow_status SET claim_after = 1; \
             RENAME TABLE workflow_queue_entries TO real_entries; \
             CREATE FUNCTION scan_gate() RETURNS INT NOT DETERMINISTIC NO SQL BEGIN \
             DECLARE ignored INT; SET ignored = GET_LOCK('{lock_name}', 30); \
             SET ignored = RELEASE_LOCK('{lock_name}'); RETURN 1; END; \
             CREATE VIEW workflow_queue_entries AS SELECT * \
             FROM real_entries WHERE scan_gate() = 1"
        ),
    )
    .await;
    let mut gate = connection(&db, backend).await;
    assert_eq!(
        scalar(&mut gate, &format!("SELECT GET_LOCK('{lock_name}', 30)")).await,
        1
    );
    let mut enqueuer = connection(&db, backend).await;
    exec(
        &mut enqueuer,
        "BEGIN; \
         INSERT INTO workflow_status (workflow_uuid, name, status, inputs, claim_after, recovery_attempts, \
         max_recovery_attempts, serialization, created_at, updated_at) \
         VALUES ('b-member', 'wf', 'PENDING', '{}', 2, 0, 100, 'portable_json', 1, 1); \
         INSERT INTO real_entries (workflow_uuid, queue_name) VALUES ('b-member', 'q')",
    )
    .await;
    let recovery = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.store().reclaim(&names(), 10, LONG).await })
    };
    // The scan has made its read view on a-member and stopped before it reaches b-member.
    wait_blocked(&db, backend, "FOR UPDATE SKIP LOCKED").await;
    exec(&mut enqueuer, "COMMIT").await;
    assert_eq!(
        scalar(&mut gate, &format!("SELECT RELEASE_LOCK('{lock_name}')")).await,
        1
    );
    assert_eq!(ids(&recovery.await.unwrap().unwrap()), Vec::<&str>::new());
    assert_eq!(attempts(&engine, "b-member").await, 0);
    assert_eq!(
        scalar(
            &mut setup,
            "SELECT COUNT(*) FROM workflow_status WHERE lease_token IS NOT NULL"
        )
        .await,
        0
    );
}

// The claim's connection dies after its revoke, with the renewal already waiting on that row lock.
// The rollback must hand the row back, so the renewal wins and the retried claim sees it.
all_dbs!(
    renewal_waiting_on_a_revoke_that_dies_keeps_the_slot,
    backend,
    {
        if is_sqlite(backend) {
            return; // SQLite has no connection to lose and does not retry.
        }
        let db = TestDb::create(backend).await;
        let (owner, claimer, held) = expired_owner(&db, backend).await;
        let gate = after_revoke(&db, backend).await;
        let claiming = claim(&claimer, "q");
        wait_blocked(&db, backend, DEADLINE_CANCEL).await;
        let renewing = renew(&owner, &lease_of(&held));
        wait_blocked(&db, backend, RENEW).await;
        kill_session(&db, backend, DEADLINE_CANCEL).await;
        renewing.await.unwrap().unwrap();
        gate.open().await;
        assert!(claiming.await.unwrap().unwrap().is_empty());
        assert!(claimer.store().transient_retries() >= 1);
        assert_kept_slot(&owner, &held, &db, backend).await;
    }
);

async fn kill_session(db: &TestDb, backend: Backend, needle: &str) {
    let needle = needle.replace('\'', "''");
    let mut conn = connection(db, backend).await;
    match backend {
        Backend::Postgres => {
            let killed = scalar(
                &mut conn,
                &format!(
                    "SELECT COUNT(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname = current_database() \
                     AND pid <> pg_backend_pid() AND query LIKE '%{needle}%'"
                ),
            )
            .await;
            assert_eq!(killed, 1);
        }
        Backend::MySql => {
            let id = scalar(
                &mut conn,
                &format!(
                    "SELECT CAST(PROCESSLIST_ID AS SIGNED) FROM performance_schema.threads WHERE PROCESSLIST_DB = DATABASE() \
                     AND PROCESSLIST_ID <> CONNECTION_ID() AND PROCESSLIST_INFO LIKE '%{needle}%'"
                ),
            )
            .await;
            exec(&mut conn, &format!("KILL {id}")).await;
        }
        Backend::Sqlite => panic!("SQLite has no sessions"),
    }
}

// A renewal that waited past its own duration succeeds but buys an already short lease. MySQL anchors
// it at the statement's start, Postgres at the row update. LeaseKeeper counts from the request, so
// it reports the loss itself. Here the slot must agree with the stored expiry on every database.
all_dbs!(
    renewal_that_outwaits_its_duration_agrees_with_the_stored_expiry,
    backend,
    {
        if is_sqlite(backend) {
            return; // A writer's wait happens before the statement, not inside it.
        }
        let db = TestDb::create(backend).await;
        let owner = engine(&db).await;
        let claimer = engine(&db).await;
        make_queue(&owner, "q", 1).await;
        enqueue(&owner, "q", &["a", "b"]).await;
        let held = run(&owner.claim_queue("q", &names()).await.unwrap()[0]);
        let mut lock = lock_row(&db, backend, "a").await;
        let duration = Duration::from_millis(50);
        let started = owner.store().now_ms().await.unwrap();
        let renewing = {
            let (owner, lease) = (owner.clone(), lease_of(&held));
            tokio::spawn(async move { owner.store().renew_lease(&lease, duration).await })
        };
        wait_blocked(&db, backend, RENEW).await;
        while owner.store().now_ms().await.unwrap() < started + 200 {
            tokio::time::sleep(WAIT_POLL).await;
        }
        exec(&mut lock, "COMMIT").await;
        renewing.await.unwrap().unwrap();
        let mut conn = connection(&db, backend).await;
        let expiry = scalar(
            &mut conn,
            "SELECT claim_after FROM workflow_status WHERE workflow_uuid = 'a'",
        )
        .await;
        let before = owner.store().now_ms().await.unwrap();
        let claimed = claimer.claim_queue("q", &names()).await.unwrap();
        let after = owner.store().now_ms().await.unwrap();
        if claimed.is_empty() {
            assert!(expiry > before, "an expired owner kept its slot");
        } else {
            assert!(expiry <= after, "a live owner lost its slot");
            assert_lost_slot(&owner, &held, &claimed, &db, backend).await;
        }
    }
);

// Both claims want the one free slot. The first holds the queue row while the second waits for it.
all_dbs!(two_claims_at_capacity_minus_one_grant_one_slot, backend, {
    let db = TestDb::create(backend).await;
    let (first, second) = (engine(&db).await, engine(&db).await);
    make_queue(&first, "q", 2).await;
    enqueue(&first, "q", &["x", "y", "z"]).await;
    let taken = first
        .store()
        .claim_queue("q", &names(), 1, LONG)
        .await
        .unwrap();
    assert_eq!(ids(&taken), ["x"]);
    let (one, two) = if is_sqlite(backend) {
        let supported = names();
        tokio::join!(
            first.claim_queue("q", &supported),
            second.claim_queue("q", &supported)
        )
    } else {
        let gate = before_revoke(&db, backend).await;
        let paused = claim(&first, "q");
        wait_blocked(&db, backend, REVOKE).await;
        let waiting = claim(&second, "q");
        wait_blocked(&db, backend, LOCK_QUEUE).await;
        gate.open().await;
        (paused.await.unwrap(), waiting.await.unwrap())
    };
    let (one, two) = (one.unwrap(), two.unwrap());
    assert_eq!(one.len() + two.len(), 1, "granted {one:?} and {two:?}");
    assert_eq!(ids(if one.is_empty() { &two } else { &one }), ["y"]);
    assert_eq!(owners(&db, backend, "q").await, 2);
    assert_eq!(attempts(&first, "z").await, 0);
});

/// A TCP proxy that lets a COMMIT reach the server and then swallows the server's answer. The client
/// sees a lost connection after a commit that happened, which is exactly a lost acknowledgment.
struct AckDropper {
    port: u16,
    armed: Arc<AtomicBool>,
    committed: watch::Receiver<bool>,
    release: watch::Sender<bool>,
}

impl AckDropper {
    async fn start(db: &TestDb) -> Self {
        let target = db
            .url
            .split_once('@')
            .unwrap()
            .1
            .split_once('/')
            .unwrap()
            .0
            .to_owned();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let armed = Arc::new(AtomicBool::new(false));
        let (committed_tx, committed) = watch::channel(false);
        let (release, release_rx) = watch::channel(false);
        let (armed_task, committed_tx) = (armed.clone(), Arc::new(committed_tx));
        tokio::spawn(async move {
            loop {
                let (client, _) = listener.accept().await.unwrap();
                let server = TcpStream::connect(&target).await.unwrap();
                tokio::spawn(pump(
                    client,
                    server,
                    armed_task.clone(),
                    committed_tx.clone(),
                    release_rx.clone(),
                ));
            }
        });
        Self {
            port,
            armed,
            committed,
            release,
        }
    }

    fn url(&self, db: &TestDb, backend: Backend) -> String {
        let (head, tail) = db.url.split_once('@').unwrap();
        let database = tail.split_once('/').unwrap().1;
        let plain = match backend {
            Backend::MySql => "ssl-mode=DISABLED",
            _ => "sslmode=disable",
        };
        format!("{head}@127.0.0.1:{}/{database}?{plain}", self.port)
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Returns once the server has answered the armed COMMIT, so the commit has happened.
    async fn committed(&mut self) {
        self.committed.wait_for(|done| *done).await.unwrap();
    }

    /// Lets the client find out that its connection is gone.
    fn release(&self) {
        self.release.send_replace(true);
    }
}

async fn pump(
    mut client: TcpStream,
    mut server: TcpStream,
    armed: Arc<AtomicBool>,
    committed: Arc<watch::Sender<bool>>,
    mut release: watch::Receiver<bool>,
) {
    let (mut from_client, mut from_server) = (vec![0u8; 65536], vec![0u8; 65536]);
    let mut swallow = false;
    loop {
        tokio::select! {
            read = client.read(&mut from_client) => {
                let Ok(size @ 1..) = read else { return };
                let packet = &from_client[..size];
                if packet.windows(6).any(|part| part == b"COMMIT") && armed.swap(false, Ordering::SeqCst) {
                    swallow = true;
                }
                if server.write_all(packet).await.is_err() { return; }
            }
            read = server.read(&mut from_server) => {
                let Ok(size @ 1..) = read else { return };
                if swallow {
                    committed.send_replace(true);
                    let _ = release.wait_for(|go| *go).await;
                    return;
                }
                if client.write_all(&from_server[..size]).await.is_err() { return; }
            }
        }
    }
}

async fn lossy_engine(db: &TestDb, backend: Backend) -> (AckDropper, Engine, Engine) {
    let proxy = AckDropper::start(db).await;
    let lossy = Engine::connect(&proxy.url(db, backend), config())
        .await
        .unwrap();
    let other = engine(db).await;
    make_queue(&other, "q", 2).await;
    enqueue(&other, "q", &["x", "y", "z"]).await;
    (proxy, lossy, other)
}

// Another claimer runs in the window, then holds the queue row while the replay waits for it.
all_dbs!(
    lost_claim_ack_replays_the_same_slots_while_another_claimer_runs,
    backend,
    {
        if is_sqlite(backend) {
            return; // No network, and `attempt` does not retry. `store::queues` tests the replay itself.
        }
        let db = TestDb::create(backend).await;
        let (mut proxy, lossy, other) = lossy_engine(&db, backend).await;
        proxy.arm();
        let lost = claim(&lossy, "q");
        proxy.committed().await;
        assert!(other.claim_queue("q", &names()).await.unwrap().is_empty());
        assert_eq!(owners(&db, backend, "q").await, 2);
        let gate = before_revoke(&db, backend).await;
        let paused = claim(&other, "q");
        wait_blocked(&db, backend, REVOKE).await;
        proxy.release();
        wait_blocked(&db, backend, LOCK_QUEUE).await;
        gate.open().await;
        assert!(paused.await.unwrap().unwrap().is_empty());
        let replayed = lost.await.unwrap().unwrap();
        assert_eq!(ids(&replayed), ["x", "y"]);
        assert!(replayed.iter().all(|row| row.recovery_attempts == 1));
        assert!(lossy.store().transient_retries() >= 1);
        for row in &replayed {
            lossy.store().renew_lease(&row.lease, LONG).await.unwrap();
        }
        assert_eq!(owners(&db, backend, "q").await, 2);
        assert_eq!(attempts(&other, "z").await, 0);
    }
);

// While the ack is lost one lease expires and another claimer takes that workflow over.
all_dbs!(
    replay_after_a_partial_takeover_returns_only_what_it_still_owns,
    backend,
    {
        if is_sqlite(backend) {
            return;
        }
        let db = TestDb::create(backend).await;
        let (mut proxy, lossy, other) = lossy_engine(&db, backend).await;
        proxy.arm();
        let lost = claim(&lossy, "q");
        proxy.committed().await;
        expire(&db, backend, "x").await;
        let taken = other.claim_queue("q", &names()).await.unwrap();
        assert_eq!(ids(&taken), ["x"]);
        assert_eq!(taken[0].recovery_attempts, 2);
        proxy.release();
        let replayed = lost.await.unwrap().unwrap();
        assert_eq!(ids(&replayed), ["y"]);
        assert_eq!(replayed[0].recovery_attempts, 1);
        assert_eq!(attempts(&other, "x").await, 2);
        other
            .store()
            .renew_lease(&taken[0].lease, LONG)
            .await
            .unwrap();
        assert_eq!(owners(&db, backend, "q").await, 2);
    }
);

// Everything the lost request held was taken over, so its retry is a fresh claim at full capacity.
all_dbs!(replay_after_a_full_takeover_admits_nothing, backend, {
    if is_sqlite(backend) {
        return;
    }
    let db = TestDb::create(backend).await;
    let (mut proxy, lossy, other) = lossy_engine(&db, backend).await;
    proxy.arm();
    let lost = claim(&lossy, "q");
    proxy.committed().await;
    expire(&db, backend, "x").await;
    expire(&db, backend, "y").await;
    assert_eq!(
        ids(&other.claim_queue("q", &names()).await.unwrap()),
        ["x", "y"]
    );
    proxy.release();
    assert!(lost.await.unwrap().unwrap().is_empty());
    assert_eq!(attempts(&other, "x").await, 2);
    assert_eq!(attempts(&other, "y").await, 2);
    assert_eq!(attempts(&other, "z").await, 0);
    assert_eq!(owners(&db, backend, "q").await, 2);
});

async fn assert_refused(engine: &Engine, old: &Run) {
    let lease = lease_of(old);
    assert!(matches!(
        engine.check_step(old, 9, "s").await,
        Err(Error::LeaseLost(_))
    ));
    assert!(matches!(
        engine.save_step_output(old, 9, "s", "1".to_owned()).await,
        Err(Error::LeaseLost(_))
    ));
    assert!(matches!(
        engine
            .store()
            .finish_workflow(&lease, &Outcome::Success("1".to_owned()))
            .await,
        Err(Error::LeaseLost(_))
    ));
    assert!(matches!(
        engine.store().release_lease(&lease).await,
        Err(Error::LeaseLost(_))
    ));
    assert!(matches!(
        engine.store().renew_lease(&lease, LONG).await,
        Err(Error::LeaseLost(_))
    ));
    // The engine's public release swallows the refusal, and must still change nothing.
    engine.release(old).await.unwrap();
}

// A batch shares one token across workflows, so a takeover of `a` must leave `b` with that token alone.
all_dbs!(
    revoked_owner_is_refused_and_cannot_touch_the_new_owner,
    backend,
    {
        let db = TestDb::create(backend).await;
        let (oldest, newer) = (engine(&db).await, engine(&db).await);
        make_queue(&oldest, "q", 2).await;
        enqueue(&oldest, "q", &["a", "b", "c"]).await;
        let batch = oldest.claim_queue("q", &names()).await.unwrap();
        assert_eq!(ids(&batch), ["a", "b"]);
        assert_eq!(batch[0].lease.token, batch[1].lease.token);
        let (old_a, old_b) = (run(&batch[0]), run(&batch[1]));
        expire(&db, backend, "a").await;
        let taken = newer.claim_queue("q", &names()).await.unwrap();
        assert_eq!(ids(&taken), ["a"]);
        assert_eq!(taken[0].recovery_attempts, 2);

        assert_refused(&oldest, &old_a).await;

        assert_eq!(attempts(&newer, "a").await, 2);
        assert_eq!(owners(&db, backend, "q").await, 2);
        assert!(newer.claim_queue("q", &names()).await.unwrap().is_empty());
        let new_a = run(&taken[0]);
        assert_eq!(
            newer.check_step(&new_a, 1, "s").await.unwrap(),
            StepDecision::Run
        );
        oldest
            .store()
            .renew_lease(&lease_of(&old_b), LONG)
            .await
            .unwrap();
        // The new owner's end frees exactly one slot, and the old owner reads the winner's result.
        newer
            .store()
            .finish_workflow(&lease_of(&new_a), &Outcome::Success("new".to_owned()))
            .await
            .unwrap();
        assert_eq!(owners(&db, backend, "q").await, 1);
        assert_eq!(ids(&newer.claim_queue("q", &names()).await.unwrap()), ["c"]);
        assert!(newer.claim_queue("q", &names()).await.unwrap().is_empty());
        assert!(matches!(
            oldest.finish(&old_a, Outcome::Success("old".to_owned())).await.unwrap(),
            yado_core::Finished::Superseded(result) if result.outcome == yado_core::FinalOutcome::Success("new".to_owned())
        ));
        assert_eq!(attempts(&newer, "a").await, 2);
    }
);

#[derive(Clone, Copy, Debug)]
enum Op {
    Finish,
    Cancel,
    Release,
    CancelExpired,
}

const OPS: [Op; 4] = [Op::Finish, Op::Cancel, Op::Release, Op::CancelExpired];

impl Op {
    /// How this operation looks in the server's list of waiting queries.
    fn needle(self) -> &'static str {
        match self {
            Op::Finish => CANCEL_IF_EXPIRED,
            Op::Cancel => CANCEL,
            Op::Release => RELEASE,
            Op::CancelExpired => {
                "WHERE status = 'PENDING' AND workflow_uuid IN (SELECT workflow_uuid FROM workflow_timeouts"
            }
        }
    }

    fn spawn(self, engine: &Engine, lease: &Lease) -> JoinHandle<Result<(), Error>> {
        let (engine, lease) = (engine.clone(), lease.clone());
        tokio::spawn(async move {
            match self {
                Op::Finish => {
                    engine
                        .store()
                        .finish_workflow(&lease, &Outcome::Success("1".to_owned()))
                        .await
                }
                Op::Cancel => engine.cancel_workflow(&lease.workflow_id).await,
                Op::Release => engine.store().release_lease(&lease).await,
                Op::CancelExpired => engine.store().cancel_expired().await.map(|_| ()),
            }
        })
    }
}

async fn past_deadline(db: &TestDb, backend: Backend, id: &str) {
    exec(
        &mut connection(db, backend).await,
        &format!("INSERT INTO workflow_timeouts (workflow_uuid, timeout_ms, deadline_epoch_ms) VALUES ('{id}', NULL, 1)"),
    )
    .await;
}

/// Every workflow in the queue must reach a final state with owners never above the limit. A slot
/// that was lost for good would stop this loop from finishing.
async fn drain(engine: &Engine, db: &TestDb, backend: Backend, concurrency: i64) {
    let mut conn = connection(db, backend).await;
    for _ in 0..500 {
        // Past owners are not coming back in this test, and their expiry must not be waited for.
        exec(&mut conn, "UPDATE workflow_status SET claim_after = 1 WHERE status = 'PENDING' AND lease_token IS NOT NULL").await;
        for row in engine.claim_queue("q", &names()).await.unwrap() {
            assert!(scalar(&mut conn, &owners_sql("q")).await <= concurrency);
            engine
                .store()
                .finish_workflow(&row.lease, &Outcome::Success("1".to_owned()))
                .await
                .unwrap();
        }
        if scalar(
            &mut conn,
            "SELECT COUNT(*) FROM workflow_status WHERE status = 'PENDING'",
        )
        .await
            == 0
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("the queue did not drain, so a slot or a workflow was lost");
}

// The operation commits while the claim sits between reading its cutoff and revoking.
all_dbs!(
    finish_cancel_release_before_the_count_free_one_slot,
    backend,
    {
        for op in OPS {
            let db = TestDb::create(backend).await;
            let (owner, claimer) = (engine(&db).await, engine(&db).await);
            make_queue(&owner, "q", 1).await;
            enqueue(&owner, "q", &["a", "b", "c"]).await;
            let first = owner.claim_queue("q", &names()).await.unwrap();
            if matches!(op, Op::CancelExpired) {
                past_deadline(&db, backend, "a").await;
            }
            let claimed = if is_sqlite(backend) {
                op.spawn(&owner, &first[0].lease).await.unwrap().unwrap();
                claimer.claim_queue("q", &names()).await.unwrap()
            } else {
                let gate = before_revoke(&db, backend).await;
                let claiming = claim(&claimer, "q");
                wait_blocked(&db, backend, REVOKE).await;
                op.spawn(&owner, &first[0].lease).await.unwrap().unwrap();
                gate.open().await;
                claiming.await.unwrap().unwrap()
            };
            assert_eq!(claimed.len(), 1, "{op:?}: {claimed:?}");
            assert_eq!(owners(&db, backend, "q").await, 1, "{op:?}");
            drain(&claimer, &db, backend, 1).await;
        }
    }
);

// The operation commits after the claim counted. The claim uses the slots it saw, and the freed one
// goes to the next claim, so nothing is admitted twice and nothing is lost.
all_dbs!(
    finish_cancel_release_after_the_count_free_one_slot_for_the_next_claim,
    backend,
    {
        for op in OPS {
            let db = TestDb::create(backend).await;
            let (owner, claimer) = (engine(&db).await, engine(&db).await);
            make_queue(&owner, "q", 2).await;
            enqueue(&owner, "q", &["a"]).await;
            enqueue_timed(&owner, "q", "b").await;
            enqueue(&owner, "q", &["c", "d"]).await;
            let first = owner
                .store()
                .claim_queue("q", &names(), 1, LONG)
                .await
                .unwrap();
            assert_eq!(ids(&first), ["a"]);
            let claimed = if is_sqlite(backend) {
                let claimed = claimer
                    .store()
                    .claim_queue("q", &names(), 1, LONG)
                    .await
                    .unwrap();
                if matches!(op, Op::CancelExpired) {
                    past_deadline(&db, backend, "a").await;
                }
                op.spawn(&owner, &first[0].lease).await.unwrap().unwrap();
                claimed
            } else {
                let mut stop = stop_after_count(&db, backend, "b").await;
                let claiming = claim(&claimer, "q");
                wait_after_count(&db, backend).await;
                if matches!(op, Op::CancelExpired) {
                    past_deadline(&db, backend, "a").await;
                }
                let operation = op.spawn(&owner, &first[0].lease);
                let completed = tokio::time::timeout(Duration::from_secs(5), operation).await;
                exec(&mut stop, "ROLLBACK").await;
                completed
                    .expect("a queue claim blocked a live owner's operation")
                    .unwrap()
                    .unwrap();
                claiming.await.unwrap().unwrap()
            };
            assert_eq!(
                ids(&claimed),
                ["b"],
                "{op:?}, retries {}",
                claimer.store().transient_retries()
            );
            assert_eq!(owners(&db, backend, "q").await, 1, "{op:?}");
            assert_eq!(
                claimer.claim_queue("q", &names()).await.unwrap().len(),
                1,
                "{op:?}"
            );
            assert_eq!(owners(&db, backend, "q").await, 2, "{op:?}");
            assert!(
                claimer.claim_queue("q", &names()).await.unwrap().is_empty(),
                "{op:?}"
            );
            drain(&claimer, &db, backend, 2).await;
        }
    }
);

// The operation waits on the row lock of a revoke, which has revoked its expired target.
all_dbs!(
    finish_cancel_release_waiting_on_a_revoke_see_the_new_owner,
    backend,
    {
        for op in OPS {
            let db = TestDb::create(backend).await;
            let (owner, claimer) = (engine(&db).await, engine(&db).await);
            make_queue(&owner, "q", 1).await;
            enqueue(&owner, "q", &["a", "b", "c"]).await;
            let first = owner.claim_queue("q", &names()).await.unwrap();
            expire(&db, backend, "a").await;
            if matches!(op, Op::CancelExpired) {
                past_deadline(&db, backend, "a").await;
            }
            let (claimed, result) = if is_sqlite(backend) {
                let claimed = claimer.claim_queue("q", &names()).await.unwrap();
                (claimed, op.spawn(&owner, &first[0].lease).await.unwrap())
            } else {
                let gate = after_revoke(&db, backend).await;
                let claiming = claim(&claimer, "q");
                wait_blocked(&db, backend, DEADLINE_CANCEL).await;
                let operation = op.spawn(&owner, &first[0].lease);
                wait_blocked(&db, backend, op.needle()).await;
                gate.open().await;
                (claiming.await.unwrap().unwrap(), operation.await.unwrap())
            };
            match op {
                Op::Finish | Op::Release => {
                    assert!(
                        matches!(result, Err(Error::LeaseLost(_))),
                        "{op:?}: {result:?}"
                    );
                    assert_eq!(ids(&claimed), ["a"], "{op:?}");
                    assert_eq!(claimed[0].recovery_attempts, 2, "{op:?}");
                    assert_eq!(owners(&db, backend, "q").await, 1, "{op:?}");
                }
                // Cancel ends the workflow whoever owns it, and that frees the slot exactly once.
                Op::Cancel => {
                    result.unwrap();
                    assert_eq!(ids(&claimed), ["a"], "{op:?}");
                    assert_eq!(status(&owner, "a").await, Status::Cancelled);
                    assert_eq!(owners(&db, backend, "q").await, 0, "{op:?}");
                    let new = run(&claimed[0]);
                    assert!(matches!(
                        claimer.check_step(&new, 1, "s").await,
                        Err(Error::WorkflowCancelled(_))
                    ));
                }
                // The claim's own deadline statement and cancel_expired race for `a`. One cancels it.
                Op::CancelExpired => {
                    result.unwrap();
                    assert_eq!(ids(&claimed), ["b"], "{op:?}");
                    assert_eq!(status(&owner, "a").await, Status::Cancelled);
                    assert_eq!(owners(&db, backend, "q").await, 1, "{op:?}");
                }
            }
            drain(&claimer, &db, backend, 1).await;
        }
    }
);

// The expired workflow is already cancelled by the paused claim. cancel_expired must wait, then
// change nothing, because the row is no longer pending.
all_dbs!(
    cancel_expired_during_a_claim_cancels_each_workflow_once,
    backend,
    {
        let db = TestDb::create(backend).await;
        let (owner, claimer) = (engine(&db).await, engine(&db).await);
        make_queue(&owner, "q", 2).await;
        enqueue(&owner, "q", &["a"]).await;
        enqueue_timed(&owner, "q", "b").await;
        enqueue(&owner, "q", &["c"]).await;
        let first = owner
            .store()
            .claim_queue("q", &names(), 1, LONG)
            .await
            .unwrap();
        assert_eq!(ids(&first), ["a"]);
        past_deadline(&db, backend, "a").await;
        let (claimed, cancelled) = if is_sqlite(backend) {
            let claimed = claimer.claim_queue("q", &names()).await.unwrap();
            (claimed, owner.store().cancel_expired().await.unwrap())
        } else {
            let mut stop = stop_after_count(&db, backend, "b").await;
            let claiming = claim(&claimer, "q");
            wait_after_count(&db, backend).await;
            let cancelling = {
                let owner = owner.clone();
                tokio::spawn(async move { owner.store().cancel_expired().await })
            };
            wait_blocked(&db, backend, CANCEL_IF_EXPIRED).await;
            exec(&mut stop, "ROLLBACK").await;
            (
                claiming.await.unwrap().unwrap(),
                cancelling.await.unwrap().unwrap(),
            )
        };
        assert_eq!(cancelled, 0);
        assert_eq!(status(&owner, "a").await, Status::Cancelled);
        // The claim cancelled `a` before it counted, so both slots were free for it.
        assert_eq!(ids(&claimed), ["b", "c"]);
        assert_eq!(owners(&db, backend, "q").await, 2);
    }
);

// While another transaction holds the queue row, no workflow-row path needs it. That removes every
// workflow-row-then-queue-row edge, so the queue row can never be part of a deadlock cycle.
all_dbs!(only_claim_and_enqueue_wait_for_the_queue_row, backend, {
    if is_sqlite(backend) {
        return; // One writer at a time: no lock order exists.
    }
    let db = TestDb::create(backend).await;
    let engine = engine(&db).await;
    make_queue(&engine, "q", 4).await;
    enqueue(&engine, "q", &["a", "b", "c", "d", "e"]).await;
    let claimed = engine.claim_queue("q", &names()).await.unwrap();
    assert_eq!(ids(&claimed), ["a", "b", "c", "d"]);
    let mut lock = connection(&db, backend).await;
    exec(
        &mut lock,
        "BEGIN; SELECT 1 FROM workflow_queues WHERE name = 'q' FOR UPDATE",
    )
    .await;

    let (a, b, d) = (run(&claimed[0]), run(&claimed[1]), run(&claimed[3]));
    engine
        .store()
        .renew_lease(&lease_of(&a), LONG)
        .await
        .unwrap();
    assert_eq!(
        engine.check_step(&a, 1, "s").await.unwrap(),
        StepDecision::Run
    );
    engine
        .save_step_output(&a, 1, "s", "1".to_owned())
        .await
        .unwrap();
    engine
        .send(
            "b",
            None,
            "1",
            yado_core::PORTABLE_JSON,
            None,
            Some(&a),
            Some(2),
        )
        .await
        .unwrap();
    engine
        .store()
        .finish_workflow(&lease_of(&a), &Outcome::Success("1".to_owned()))
        .await
        .unwrap();
    engine.store().release_lease(&lease_of(&b)).await.unwrap();
    engine.cancel_workflow("c").await.unwrap();
    past_deadline(&db, backend, "d").await;
    assert_eq!(engine.store().cancel_expired().await.unwrap(), 1);
    assert_eq!(status(&engine, d.workflow_id()).await, Status::Cancelled);

    let enqueuing = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.enqueue("q", begin("f")).await })
    };
    wait_blocked(&db, backend, LOCK_QUEUE).await;
    let claiming = claim(&engine, "q");
    exec(&mut lock, "COMMIT").await;
    enqueuing.await.unwrap().unwrap();
    assert!(!claiming.await.unwrap().unwrap().is_empty());
});

// Admission must finish while a send holds another live owner's row, before its foreign-key check.
all_dbs!(
    send_holding_a_live_owner_does_not_block_admission,
    backend,
    {
        let db = TestDb::create(backend).await;
        let (owner, claimer) = (engine(&db).await, engine(&db).await);
        make_queue(&owner, "q", 2).await;
        enqueue(&owner, "q", &["a-destination", "b-sender"]).await;
        let held = owner.claim_queue("q", &names()).await.unwrap();
        assert_eq!(ids(&held), ["a-destination", "b-sender"]);
        let sender = lease_of(&run(&held[1]));
        expire(&db, backend, "a-destination").await;
        let send = |engine: Engine, sender: Lease| async move {
            let run = Run::new(sender, yado_core::PORTABLE_JSON.to_owned(), None);
            engine
                .send(
                    "a-destination",
                    None,
                    "1",
                    yado_core::PORTABLE_JSON,
                    Some("once"),
                    Some(&run),
                    Some(1),
                )
                .await
        };
        let claimed = if is_sqlite(backend) {
            send(owner.clone(), sender.clone()).await.unwrap();
            claimer.claim_queue("q", &names()).await.unwrap()
        } else {
            // The gate stops the send's replay check: after it locked its own row, before its foreign key.
            let gate = Gate::close(&db, backend, "operation_outputs").await;
            let sending = tokio::spawn(send(owner.clone(), sender.clone()));
            wait_blocked(&db, backend, READ_STEP).await;
            let claiming = claim(&claimer, "q");
            let claimed = tokio::time::timeout(Duration::from_secs(5), claiming).await;
            gate.open().await;
            sending.await.unwrap().unwrap();
            claimed
                .expect("a live sender blocked admission")
                .unwrap()
                .unwrap()
        };
        assert_eq!(ids(&claimed), ["a-destination"]);
        assert_eq!(claimed[0].recovery_attempts, 2);
        assert_eq!(owners(&db, backend, "q").await, 2);
        let mut conn = connection(&db, backend).await;
        assert_eq!(
            scalar(&mut conn, "SELECT COUNT(*) FROM notifications").await,
            1
        );
        owner.store().renew_lease(&sender, LONG).await.unwrap();
        assert_eq!(attempts(&owner, "b-sender").await, 1);
    }
);

// The claim is the deadlock victim after it already revoked and re-admitted an owner. A retry must
// redo that from scratch: one attempt per admission that committed, and the same FIFO result.
all_dbs!(
    deadlock_victim_claim_retries_without_spending_attempts,
    backend,
    {
        if is_sqlite(backend) {
            return; // One writer at a time: no deadlock can form.
        }
        let db = TestDb::create(backend).await;
        let claimer = engine(&db).await;
        make_queue(&claimer, "q", 2).await;
        enqueue(&claimer, "q", &["a"]).await;
        enqueue_timed(&claimer, "q", "b").await;
        let first = claimer
            .store()
            .claim_queue("q", &names(), 1, LONG)
            .await
            .unwrap();
        assert_eq!(ids(&first), ["a"]);
        expire(&db, backend, "a").await;
        let mut stop = stop_after_count(&db, backend, "b").await;
        // MySQL rolls back the transaction with less work, so this side does more. Another table keeps
        // workflow_status small enough that the claim's plan does not change.
        exec(
            &mut stop,
            "CREATE TEMPORARY TABLE weight (id INT PRIMARY KEY)",
        )
        .await;
        let values: Vec<String> = (0..500).map(|id| format!("({id})")).collect();
        exec(
            &mut stop,
            &format!("INSERT INTO weight (id) VALUES {}", values.join(", ")),
        )
        .await;
        let claiming = claim(&claimer, "q");
        wait_after_count(&db, backend).await;
        // This closes the cycle: the claim holds the queue row and waits for us, and now we want it.
        exec(
            &mut stop,
            "SELECT 1 FROM workflow_queues WHERE name = 'q' FOR UPDATE",
        )
        .await;
        exec(&mut stop, "ROLLBACK").await;
        let claimed = claiming.await.unwrap().unwrap();
        assert_eq!(ids(&claimed), ["a", "b"]);
        assert_eq!(claimed[0].recovery_attempts, 2);
        assert_eq!(claimed[1].recovery_attempts, 1);
        assert!(claimer.store().transient_retries() >= 1);
        assert_eq!(attempts(&claimer, "a").await, 2);
        assert_eq!(attempts(&claimer, "b").await, 1);
        assert_eq!(owners(&db, backend, "q").await, 2);
        assert!(matches!(
            claimer.check_step(&run(&first[0]), 1, "s").await,
            Err(Error::LeaseLost(_))
        ));
    }
);

// Dead rows at the head of the FIFO are finished, not admitted. They use no slot, order holds, and
// the loop ends even when nothing is left.
all_dbs!(
    claim_loop_passes_dead_heads_without_burning_slots,
    backend,
    {
        let db = TestDb::create(backend).await;
        let engine = engine(&db).await;
        make_queue(&engine, "q", 1).await;
        enqueue(
            &engine,
            "q",
            &["spent-1", "late", "spent-2", "good", "next"],
        )
        .await;
        let mut conn = connection(&db, backend).await;
        exec(
            &mut conn,
            "UPDATE workflow_status SET recovery_attempts = 101 WHERE workflow_uuid LIKE 'spent-%'",
        )
        .await;
        past_deadline(&db, backend, "late").await;
        let claimed = engine.claim_queue("q", &names()).await.unwrap();
        assert_eq!(ids(&claimed), ["good"]);
        assert_eq!(claimed[0].recovery_attempts, 1);
        for spent in ["spent-1", "spent-2"] {
            assert_eq!(
                status(&engine, spent).await,
                Status::MaxRecoveryAttemptsExceeded
            );
            assert_eq!(attempts(&engine, spent).await, 102);
        }
        assert_eq!(status(&engine, "late").await, Status::Cancelled);
        assert_eq!(attempts(&engine, "next").await, 0);
        assert_eq!(owners(&db, backend, "q").await, 1);
        engine
            .store()
            .finish_workflow(&claimed[0].lease, &Outcome::Success("1".to_owned()))
            .await
            .unwrap();
        assert_eq!(
            ids(&engine.claim_queue("q", &names()).await.unwrap()),
            ["next"]
        );

        // Only dead rows left: the loop ends with nothing and the slot stays free for later work.
        make_queue(&engine, "dead", 1).await;
        enqueue(&engine, "dead", &["spent-3", "spent-4"]).await;
        exec(
        &mut conn,
        "UPDATE workflow_status SET recovery_attempts = 101 WHERE workflow_uuid IN ('spent-3', 'spent-4')",
    )
    .await;
        assert!(
            engine
                .claim_queue("dead", &names())
                .await
                .unwrap()
                .is_empty()
        );
        enqueue(&engine, "dead", &["alive"]).await;
        assert_eq!(
            ids(&engine.claim_queue("dead", &names()).await.unwrap()),
            ["alive"]
        );
    }
);

// The head dies while the claim waits for its row lock, so the check after the lock must catch it.
all_dbs!(
    head_that_dies_during_the_claim_loop_is_passed_over,
    backend,
    {
        if is_sqlite(backend) {
            return; // The claim_loop_passes_dead_heads end state covers it: nothing can change mid-claim.
        }
        for exhaust in [true, false] {
            let db = TestDb::create(backend).await;
            let engine = engine(&db).await;
            make_queue(&engine, "q", 1).await;
            enqueue_timed(&engine, "q", "head").await;
            enqueue(&engine, "q", &["next", "last"]).await;
            let mut lock = lock_row(&db, backend, "head").await;
            let claiming = claim(&engine, "q");
            wait_blocked(&db, backend, LOCK_CANDIDATE).await;
            exec(
            &mut lock,
            if exhaust {
                "UPDATE workflow_status SET recovery_attempts = 101 WHERE workflow_uuid = 'head'"
            } else {
                "INSERT INTO workflow_timeouts (workflow_uuid, timeout_ms, deadline_epoch_ms) VALUES ('head', NULL, 1)"
            },
        )
        .await;
            exec(&mut lock, "COMMIT").await;
            let claimed = claiming.await.unwrap().unwrap();
            assert_eq!(ids(&claimed), ["next"]);
            assert_eq!(claimed[0].recovery_attempts, 1);
            assert_eq!(
                status(&engine, "head").await,
                if exhaust {
                    Status::MaxRecoveryAttemptsExceeded
                } else {
                    Status::Cancelled
                }
            );
            assert_eq!(owners(&db, backend, "q").await, 1);
            assert_eq!(attempts(&engine, "last").await, 0);
        }
    }
);

// Enqueue accepts any timeout that fits from its own clock reading. A later claim adds it to a later
// clock reading. If that overflowed, the claim failed for good and the whole queue stopped.
all_dbs!(
    oversized_relative_timeout_does_not_wedge_the_queue,
    backend,
    {
        let db = TestDb::create(backend).await;
        let engine = engine(&db).await;
        make_queue(&engine, "q", 1).await;
        let now = engine.store().now_ms().await.unwrap();
        let mut huge = begin("huge");
        // Fits at enqueue, and overflows at a claim made 24 hours later or more.
        huge.timeout_ms = Some(i64::MAX - now - 86_400_000);
        engine.enqueue("q", huge).await.unwrap();
        enqueue(&engine, "q", &["next"]).await;
        let mut conn = connection(&db, backend).await;
        exec(
            &mut conn,
            &format!(
            "UPDATE workflow_queue_entries SET start_timeout_ms = {} WHERE workflow_uuid = 'huge'",
            i64::MAX
        ),
        )
        .await;
        let claimed = engine.claim_queue("q", &names()).await.unwrap();
        assert_eq!(ids(&claimed), ["huge"]);
        assert_eq!(claimed[0].deadline_epoch_ms, Some(i64::MAX));
        engine
            .store()
            .renew_lease(&claimed[0].lease, LONG)
            .await
            .unwrap();
    }
);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_expired_owner_recheck_does_not_wait_for_live_rows() {
    let backend = Backend::MySql;
    let db = TestDb::create(backend).await;
    let engine = engine(&db).await;
    make_queue(&engine, "q", 2).await;
    enqueue(&engine, "q", &["a", "b"]).await;
    let held = engine.claim_queue("q", &names()).await.unwrap();
    let mut setup = connection(&db, backend).await;
    exec(
        &mut setup,
        "UPDATE workflow_status SET claim_after = 1 WHERE workflow_uuid = 'a'",
    )
    .await;
    exec(
        &mut setup,
        "UPDATE workflow_status SET claim_after = 9000000000000 WHERE workflow_uuid = 'b'",
    )
    .await;
    exec(
        &mut setup,
        "ALTER TABLE workflow_status DROP INDEX ix_workflow_status_claim",
    )
    .await;

    let mut renewal = connection(&db, backend).await;
    exec(&mut renewal, "BEGIN").await;
    exec(
        &mut renewal,
        &format!(
            "UPDATE workflow_status SET claim_after = 9000000000000 WHERE workflow_uuid = 'a' AND lease_token = '{}'",
            held[0].lease.token
        ),
    )
    .await;
    let mut live_lock = lock_row(&db, backend, "b").await;
    let claiming = claim(&engine, "q");
    wait_blocked(&db, backend, REVOKE).await;
    exec(&mut renewal, "COMMIT").await;
    let claimed = tokio::time::timeout(Duration::from_secs(5), claiming).await;
    exec(&mut live_lock, "COMMIT").await;
    assert!(
        claimed
            .expect("rechecking an expired owner blocked on a live workflow")
            .unwrap()
            .unwrap()
            .is_empty()
    );
    assert_eq!(owners(&db, backend, "q").await, 2);
    assert_eq!(attempts(&engine, "a").await, 1);
    engine
        .store()
        .renew_lease(&held[0].lease, LONG)
        .await
        .unwrap();
}

/// Stands in for parties that have already left a barrier. One stand-in fills one
/// empty slot. Two workers can leave while a third is already inside `wait`, so
/// every empty slot needs its own stand-in or that third worker stays there.
fn hold_barrier_open(barrier: Arc<tokio::sync::Barrier>, parties: usize) -> Vec<JoinHandle<()>> {
    (0..parties.saturating_sub(1))
        .map(|_| {
            let barrier = barrier.clone();
            tokio::spawn(async move {
                loop {
                    barrier.wait().await;
                }
            })
        })
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn parked_barrier_waiter_unblocks_after_the_other_parties_leave() {
    let parties = STRESS_ENGINES;
    let barrier = Arc::new(tokio::sync::Barrier::new(parties));
    let parked = barrier.clone();
    let mut wait = std::pin::pin!(async move { parked.wait().await });
    std::future::poll_fn(|cx| {
        let _ = wait.as_mut().poll(cx);
        std::task::Poll::Ready(())
    })
    .await;
    let fillers = hold_barrier_open(barrier, parties);
    tokio::time::timeout(
        Duration::from_secs(2),
        std::future::poll_fn(|cx| wait.as_mut().poll(cx)),
    )
    .await
    .expect("a lone waiter stayed parked when the other parties had left");
    for filler in fillers {
        filler.abort();
    }
}

const STRESS_ENGINES: usize = 3;
const STRESS_CONCURRENCY: i64 = 3;
const STRESS_WORKFLOWS: usize = 18;
const STRESS_LEASE: Duration = Duration::from_millis(300);
const STRESS_BOUND: Duration = Duration::from_secs(90);

async fn token_of(conn: &mut AnyConnection, id: &str) -> Option<String> {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT lease_token FROM workflow_status WHERE workflow_uuid = '{id}'"
    )))
    .fetch_one(conn)
    .await
    .unwrap()
}

/// Stops renewing, as a paused process does. Once the database shows another token, which can never
/// become its token again, every call it makes must be refused.
async fn abandoned_owner(
    engine: Engine,
    claimed: Claimed,
    mut conn: AnyConnection,
    seen: Arc<AtomicBool>,
) {
    let old = run(&claimed);
    while token_of(&mut conn, old.workflow_id()).await.as_deref() == Some(old.token()) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let lease = lease_of(&old);
    for result in [
        engine.check_step(&old, 9, "s").await.map(|_| ()),
        engine
            .save_step_output(&old, 9, "s", "stale".to_owned())
            .await,
        engine
            .store()
            .finish_workflow(&lease, &Outcome::Success("stale".to_owned()))
            .await,
        engine.store().release_lease(&lease).await,
        engine.store().renew_lease(&lease, LONG).await,
    ] {
        assert!(
            matches!(
                result,
                Err(Error::LeaseLost(_) | Error::WorkflowCancelled(_))
            ),
            "a replaced owner got {result:?}"
        );
    }
    seen.store(true, Ordering::SeqCst);
}

/// Renews through the real keeper and works for a while. A lost lease under load is allowed, and the
/// workflow is then finished by whoever is admitted next.
async fn healthy_owner(engine: Engine, claimed: Claimed) {
    let current = run(&claimed);
    let _keeper = engine.keep_lease(&current);
    for step in 1..=3 {
        match engine
            .save_step_output(&current, step, "s", "1".to_owned())
            .await
        {
            Ok(()) => {}
            Err(Error::LeaseLost(_)) => return,
            Err(error) => panic!("unexpected step error: {error}"),
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    match engine
        .store()
        .finish_workflow(&claimed.lease, &Outcome::Success("1".to_owned()))
        .await
    {
        Ok(()) | Err(Error::LeaseLost(_)) => {}
        Err(error) => panic!("unexpected finish error: {error}"),
    }
}

// What this can detect, and what it cannot, is in .workmux/REPORT.md. In short: a violation has to last
// until a sample runs, so it catches owners over the limit that stay there, but not a violation shorter
// than one sample. Every admission is followed by a sample before its owner can do any work.
all_dbs!(
    stress_owners_stay_within_the_limit_and_every_workflow_ends,
    backend,
    {
        let db = TestDb::create(backend).await;
        let mut engines = Vec::new();
        for _ in 0..STRESS_ENGINES {
            engines.push(
                Engine::connect(
                    &db.url,
                    EngineConfig {
                        lease: STRESS_LEASE,
                        ..config()
                    },
                )
                .await
                .unwrap(),
            );
        }
        make_queue(&engines[0], "q", STRESS_CONCURRENCY as u32).await;
        for index in 0..STRESS_WORKFLOWS {
            let id = format!("wf-{index:02}");
            engines[0].enqueue("q", begin(&id)).await.unwrap();
        }

        let done = Arc::new(AtomicBool::new(false));
        let sampler = {
            let (done, mut conn) = (done.clone(), connection(&db, backend).await);
            tokio::spawn(async move {
                let (mut samples, mut highest) = (0u64, 0i64);
                while !done.load(Ordering::SeqCst) {
                    let owners = scalar(&mut conn, &owners_sql("q")).await;
                    assert!(
                        owners <= STRESS_CONCURRENCY,
                        "{owners} owners after {samples} samples"
                    );
                    highest = highest.max(owners);
                    samples += 1;
                }
                (samples, highest)
            })
        };

        let barrier = Arc::new(tokio::sync::Barrier::new(STRESS_ENGINES));
        let abandoned_seen: Vec<_> = (0..STRESS_WORKFLOWS)
            .map(|_| Arc::new(AtomicBool::new(false)))
            .collect();
        let abandoned_seen = Arc::new(abandoned_seen);
        let mut workers = Vec::new();
        for engine in engines.clone() {
            let (done, barrier, abandoned_seen) =
                (done.clone(), barrier.clone(), abandoned_seen.clone());
            let mut conn = connection(&db, backend).await;
            let url_db = db.url.clone();
            workers.push(tokio::spawn(async move {
                let (mut owners_running, mut admitted) = (Vec::new(), 0u32);
                while !done.load(Ordering::SeqCst) {
                    // Every engine starts its claim together, so claims overlap on purpose.
                    barrier.wait().await;
                    let claimed = engine.claim_queue("q", &names()).await.unwrap();
                    if !claimed.is_empty() {
                        let owners = scalar(&mut conn, &owners_sql("q")).await;
                        assert!(
                            owners <= STRESS_CONCURRENCY,
                            "{owners} owners right after an admission"
                        );
                    }
                    for row in claimed {
                        admitted += 1;
                        let index: usize = row.lease.workflow_id[3..].parse().unwrap();
                        let task = engine.clone();
                        // One workflow in three loses its first owner to a stop in renewing.
                        if index.is_multiple_of(3) && row.recovery_attempts == 1 {
                            let watcher = AnyConnection::connect(&url_db).await.unwrap();
                            owners_running.push(tokio::spawn(abandoned_owner(
                                task,
                                row,
                                watcher,
                                abandoned_seen[index].clone(),
                            )));
                        } else {
                            owners_running.push(tokio::spawn(healthy_owner(task, row)));
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                for owner in owners_running {
                    owner.await.unwrap();
                }
                admitted
            }));
        }

        let mut conn = connection(&db, backend).await;
        let ended = tokio::time::timeout(STRESS_BOUND, async {
            while scalar(
                &mut conn,
                "SELECT COUNT(*) FROM workflow_status WHERE status = 'PENDING'",
            )
            .await
                > 0
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        // A worker that already saw `done` stays out of the barrier. The join is
        // outside the wait above, so it has the same bound: a stuck worker fails
        // the test instead of leaving the suite parked.
        done.store(true, Ordering::SeqCst);
        let release_barrier = hold_barrier_open(barrier.clone(), STRESS_ENGINES);
        let admitted = tokio::time::timeout(STRESS_BOUND, async {
            let mut admitted = 0;
            for worker in workers {
                admitted += worker.await.unwrap();
            }
            admitted
        })
        .await
        .expect("a worker did not finish after its workflows ended");
        for release in release_barrier {
            release.abort();
        }
        let (samples, highest) = sampler.await.unwrap();
        assert!(
            ended.is_ok(),
            "a workflow did not end, so a slot or a workflow was lost"
        );
        assert_eq!(
            scalar(
                &mut conn,
                "SELECT COUNT(*) FROM workflow_status WHERE status = 'SUCCESS'"
            )
            .await,
            STRESS_WORKFLOWS as i64
        );
        for index in (0..STRESS_WORKFLOWS).step_by(3) {
            assert!(
                abandoned_seen[index].load(Ordering::SeqCst),
                "wf-{index:02} kept its abandoned owner"
            );
        }
        assert!(admitted as usize >= STRESS_WORKFLOWS + STRESS_WORKFLOWS / 3);
        eprintln!(
            "stress {backend:?}: {samples} samples, {admitted} admissions, highest {highest} of {STRESS_CONCURRENCY}"
        );
    }
);

// A claim stalled in its revoke on one expired owner's row must not stall live owners.
// This passed before any fix, so it guards the behaviour and is not a regression test for a bug.
all_dbs!(stalled_claim_does_not_block_live_owners, backend, {
    if is_sqlite(backend) {
        return; // One writer at a time: a stalled claim blocks every writer by design.
    }
    let db = TestDb::create(backend).await;
    let (owner, claimer) = (engine(&db).await, engine(&db).await);
    make_queue(&owner, "q", 3).await;
    enqueue(&owner, "q", &["a-live", "b-expired", "c-live", "d-waiting"]).await;
    let held = owner.claim_queue("q", &names()).await.unwrap();
    assert_eq!(ids(&held), ["a-live", "b-expired", "c-live"]);
    expire(&db, backend, "b-expired").await;
    let mut lock = lock_row(&db, backend, "b-expired").await;
    let claiming = claim(&claimer, "q");
    wait_blocked(&db, backend, REVOKE).await;
    for live in [0, 2] {
        let current = run(&held[live]);
        tokio::time::timeout(Duration::from_secs(5), async {
            owner
                .store()
                .renew_lease(&held[live].lease, LONG)
                .await
                .unwrap();
            owner
                .save_step_output(&current, 1, "s", "1".to_owned())
                .await
                .unwrap();
        })
        .await
        .expect("a stalled claim blocked a live owner");
    }
    exec(&mut lock, "COMMIT").await;
    assert_eq!(ids(&claiming.await.unwrap().unwrap()), ["b-expired"]);
    assert_eq!(owners(&db, backend, "q").await, 3);
});

all_dbs!(
    claim_paused_after_count_does_not_block_a_live_owner,
    backend,
    {
        if is_sqlite(backend) {
            return;
        }
        for live_id in ["a-live", "z-live"] {
            let db = TestDb::create(backend).await;
            let (owner, claimer) = (engine(&db).await, engine(&db).await);
            make_queue(&owner, "q", 2).await;
            enqueue(&owner, "q", &[live_id]).await;
            let held = owner.claim_queue("q", &names()).await.unwrap();
            enqueue_timed(&owner, "q", "b-waiting").await;
            enqueue(&owner, "q", &["c-waiting", "d-waiting"]).await;
            if matches!(backend, Backend::MySql) {
                exec(
                    &mut connection(&db, backend).await,
                    "ALTER TABLE workflow_status DROP INDEX ix_workflow_status_claim",
                )
                .await;
            }
            let mut stop = stop_after_count(&db, backend, "b-waiting").await;
            let claiming = claim(&claimer, "q");
            wait_after_count(&db, backend).await;
            let renewed = tokio::time::timeout(
                Duration::from_secs(2),
                owner.store().renew_lease(&held[0].lease, LONG),
            )
            .await;
            exec(&mut stop, "ROLLBACK").await;
            assert_eq!(ids(&claiming.await.unwrap().unwrap()), ["b-waiting"]);
            renewed
                .expect("a queue claim blocked renewal of a live owner")
                .unwrap();
        }
    }
);
