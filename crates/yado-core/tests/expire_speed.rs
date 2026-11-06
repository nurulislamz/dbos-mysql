//! Backlogs of expired rows stay correct when cleanup updates many primary keys at once.
//! A locked owner that is not one of those rows must not stall the sweep.

use std::num::NonZeroU32;
use std::time::Duration;

use sqlx::mysql::MySqlConnection;
use sqlx::postgres::PgConnection;
use sqlx::{Any, Connection, Row, Transaction};
use tempfile::TempDir;
use yado_core::{Error, NewWorkflow, PORTABLE_JSON, SendOutcome, Started, Status, Store};

const BACKLOG: usize = 250;
const FAR: i64 = 9_000_000_000_000;
const LONG: Duration = Duration::from_secs(600);

#[derive(Clone, Copy)]
enum Backend {
    Sqlite,
    MySql,
    Postgres,
}

struct TestDb {
    url: String,
    mysql_database: Option<String>,
    postgres_database: Option<String>,
    _dir: Option<TempDir>,
}

fn mysql_admin_url() -> String {
    std::env::var("DBOS_TEST_MYSQL_URL")
        .unwrap_or_else(|_| "mysql://root:dbos-test-root@127.0.0.1:13307/dbos_test".to_owned())
}

fn postgres_admin_url() -> String {
    std::env::var("DBOS_TEST_POSTGRES_URL").unwrap_or_else(|_| {
        "postgres://postgres:dbos-test-root@127.0.0.1:13309/dbos_test".to_owned()
    })
}

impl TestDb {
    async fn create(backend: Backend) -> Self {
        match backend {
            Backend::Sqlite => {
                let dir = TempDir::new().unwrap();
                let url = format!("sqlite://{}", dir.path().join("test.sqlite").display());
                Self {
                    url,
                    mysql_database: None,
                    postgres_database: None,
                    _dir: Some(dir),
                }
            }
            Backend::MySql => {
                let name = format!("dbos_e_{}", uuid::Uuid::new_v4().simple());
                let mut conn = MySqlConnection::connect(&mysql_admin_url()).await.unwrap();
                sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}")))
                    .execute(&mut conn)
                    .await
                    .unwrap();
                conn.close().await.unwrap();
                let base = mysql_admin_url();
                let base = base.rsplit_once('/').unwrap().0;
                Self {
                    url: format!("{base}/{name}"),
                    mysql_database: Some(name),
                    postgres_database: None,
                    _dir: None,
                }
            }
            Backend::Postgres => {
                let name = format!("dbos_e_{}", uuid::Uuid::new_v4().simple());
                let mut conn = PgConnection::connect(&postgres_admin_url()).await.unwrap();
                sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}")))
                    .execute(&mut conn)
                    .await
                    .unwrap();
                conn.close().await.unwrap();
                let base = postgres_admin_url();
                let base = base.rsplit_once('/').unwrap().0;
                Self {
                    url: format!("{base}/{name}"),
                    mysql_database: None,
                    postgres_database: Some(name),
                    _dir: None,
                }
            }
        }
    }

    async fn open(&self) -> Store {
        let store = Store::connect(&self.url).await.unwrap();
        store.migrate().await.unwrap();
        store
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let mysql = self.mysql_database.take();
        let postgres = self.postgres_database.take();
        if mysql.is_none() && postgres.is_none() {
            return;
        }
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                if let Some(name) = mysql {
                    let mut conn = MySqlConnection::connect(&mysql_admin_url()).await.unwrap();
                    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                        "DROP DATABASE IF EXISTS {name}"
                    )))
                    .execute(&mut conn)
                    .await
                    .unwrap();
                }
                if let Some(name) = postgres {
                    let mut conn = PgConnection::connect(&postgres_admin_url()).await.unwrap();
                    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                        "DROP DATABASE IF EXISTS {name} WITH (FORCE)"
                    )))
                    .execute(&mut conn)
                    .await
                    .unwrap();
                }
            });
        })
        .join()
        .unwrap();
    }
}

async fn setup(backend: Backend) -> (TestDb, Store) {
    let db = TestDb::create(backend).await;
    let store = db.open().await;
    (db, store)
}

fn names() -> Vec<String> {
    vec!["wf".to_owned()]
}

fn placeholder(postgres: bool, index: usize) -> String {
    if postgres {
        format!("${index}")
    } else {
        "?".to_owned()
    }
}

/// Placeholders only. Row values are bound.
fn insert_sql(postgres: bool, into: &str, columns: usize, rows: usize) -> String {
    let mut values = Vec::with_capacity(rows);
    let mut index = 1;
    for _ in 0..rows {
        let slots: Vec<String> = (0..columns)
            .map(|offset| placeholder(postgres, index + offset))
            .collect();
        values.push(format!("({})", slots.join(", ")));
        index += columns;
    }
    format!("{into} VALUES {}", values.join(", "))
}

async fn insert_workflows(store: &Store, rows: &[(&str, &str, i64, Option<&str>)]) {
    let sql = insert_sql(
        store.backend() == "postgres",
        "INSERT INTO workflow_status (workflow_uuid, name, status, inputs, claim_after, lease_token, \
         recovery_attempts, max_recovery_attempts, serialization, created_at, updated_at)",
        11,
        rows.len(),
    );
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
    for (id, status, claim_after, token) in rows {
        query = query
            .bind(*id)
            .bind("wf")
            .bind(*status)
            .bind("{}")
            .bind(*claim_after)
            .bind(*token)
            .bind(0_i64)
            .bind(100_i64)
            .bind(PORTABLE_JSON)
            .bind(1_i64)
            .bind(1_i64);
    }
    query.execute(store.pool()).await.unwrap();
}

async fn insert_members(store: &Store, ids: &[&str]) {
    let sql = insert_sql(
        store.backend() == "postgres",
        "INSERT INTO workflow_queue_entries (workflow_uuid, queue_name)",
        2,
        ids.len(),
    );
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
    for id in ids {
        query = query.bind(*id).bind("q");
    }
    query.execute(store.pool()).await.unwrap();
}

async fn insert_deadlines(store: &Store, rows: &[(&str, i64)]) {
    let sql = insert_sql(
        store.backend() == "postgres",
        "INSERT INTO workflow_timeouts (workflow_uuid, timeout_ms, deadline_epoch_ms)",
        3,
        rows.len(),
    );
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
    for (id, deadline) in rows {
        query = query.bind(*id).bind(None::<i64>).bind(*deadline);
    }
    query.execute(store.pool()).await.unwrap();
}

async fn token(store: &Store, id: &str) -> Option<String> {
    let sql = if store.backend() == "postgres" {
        "SELECT lease_token FROM workflow_status WHERE workflow_uuid = $1"
    } else {
        "SELECT lease_token FROM workflow_status WHERE workflow_uuid = ?"
    };
    sqlx::query(sql)
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap()
        .try_get("lease_token")
        .unwrap()
}

async fn status_of(store: &Store, id: &str) -> Status {
    store.get_workflow(id).await.unwrap().unwrap().status
}

async fn lock_row(store: &Store, id: &str) -> Transaction<'static, Any> {
    let mut tx = store.pool().begin().await.unwrap();
    let sql = if store.backend() == "postgres" {
        "SELECT workflow_uuid FROM workflow_status WHERE workflow_uuid = $1 FOR UPDATE"
    } else {
        "SELECT workflow_uuid FROM workflow_status WHERE workflow_uuid = ? FOR UPDATE"
    };
    sqlx::query(sql).bind(id).fetch_one(&mut *tx).await.unwrap();
    tx
}

fn expired_ids() -> Vec<String> {
    (0..BACKLOG)
        .map(|index| format!("exp-{index:04}"))
        .collect()
}

async fn queue_backlog(backend: Backend) {
    let (_db, store) = setup(backend).await;
    store
        .ensure_queue("q", NonZeroU32::new(2).unwrap())
        .await
        .unwrap();
    let expired = expired_ids();
    let mut rows = Vec::with_capacity(BACKLOG + 2);
    rows.push(("live", "PENDING", FAR, Some("live-token")));
    for id in &expired {
        rows.push((id.as_str(), "PENDING", 1, Some("stale")));
    }
    rows.push(("waiting", "PENDING", 1, None));
    insert_workflows(&store, &rows).await;
    let ids: Vec<&str> = rows.iter().map(|row| row.0).collect();
    insert_members(&store, &ids).await;
    let lock = if matches!(backend, Backend::Sqlite) {
        None
    } else {
        Some(lock_row(&store, "live").await)
    };
    let claimed = tokio::time::timeout(
        Duration::from_secs(5),
        store.claim_queue("q", &names(), 1, LONG),
    )
    .await
    .expect("a locked live owner stalled cleanup of the expired backlog")
    .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_ne!(claimed[0].lease.workflow_id, "live");
    assert_eq!(token(&store, "live").await.as_deref(), Some("live-token"));
    let stale: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM workflow_status WHERE lease_token = 'stale'")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(stale, 0);
    drop(lock);
}

async fn timeout_backlog(backend: Backend) {
    let (_db, store) = setup(backend).await;
    let expired = expired_ids();
    let mut rows = Vec::with_capacity(BACKLOG + 2);
    let mut deadlines = Vec::with_capacity(BACKLOG + 2);
    rows.push(("keep", "PENDING", FAR, Some("live-token")));
    rows.push(("done", "SUCCESS", 1, None));
    deadlines.push(("keep", FAR));
    deadlines.push(("done", 1));
    for id in &expired {
        rows.push((id.as_str(), "PENDING", 1, Some("stale")));
        deadlines.push((id.as_str(), 1));
    }
    insert_workflows(&store, &rows).await;
    insert_deadlines(&store, &deadlines).await;
    let lock = if matches!(backend, Backend::Sqlite) {
        None
    } else {
        Some(lock_row(&store, "keep").await)
    };
    let cancelled = tokio::time::timeout(Duration::from_secs(5), store.cancel_expired())
        .await
        .expect("a locked owner stalled cancellation of other expired rows")
        .unwrap();
    assert_eq!(cancelled, BACKLOG as u64);
    assert_eq!(status_of(&store, "keep").await, Status::Pending);
    assert_eq!(token(&store, "keep").await.as_deref(), Some("live-token"));
    assert_eq!(status_of(&store, "done").await, Status::Success);
    assert_eq!(status_of(&store, "exp-0000").await, Status::Cancelled);
    assert_eq!(status_of(&store, "exp-0249").await, Status::Cancelled);
    drop(lock);
}

async fn expired_event_fence(backend: Backend) {
    let (_db, store) = setup(backend).await;
    let started = store
        .start_workflow(&NewWorkflow {
            workflow_id: "owner",
            name: "wf",
            input: "{}",
            max_recovery_attempts: 100,
            lease: LONG,
            parent_workflow_id: None,
            serialization: PORTABLE_JSON,
            timeout_ms: None,
            deadline_epoch_ms: None,
        })
        .await
        .unwrap();
    let Started::Owned(lease, _) = started else {
        panic!("expected a new workflow");
    };
    assert!(matches!(
        store
            .set_event(&lease, Some(1), "key", "1", PORTABLE_JSON)
            .await
            .unwrap(),
        SendOutcome::Sent
    ));
    let sql = if store.backend() == "postgres" {
        "UPDATE workflow_status SET claim_after = 0 WHERE workflow_uuid = $1"
    } else {
        "UPDATE workflow_status SET claim_after = 0 WHERE workflow_uuid = ?"
    };
    sqlx::query(sql)
        .bind("owner")
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store
            .set_event(&lease, Some(1), "key", "1", PORTABLE_JSON)
            .await
            .unwrap(),
        SendOutcome::Replayed
    ));
    let refused = store
        .set_event(&lease, Some(2), "other", "1", PORTABLE_JSON)
        .await
        .unwrap_err();
    assert!(matches!(refused, Error::LeaseLost(_)));
    assert_eq!(store.list_events("owner").await.unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_backlog_sqlite() {
    queue_backlog(Backend::Sqlite).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_backlog_mysql() {
    queue_backlog(Backend::MySql).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_backlog_postgres() {
    queue_backlog(Backend::Postgres).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_backlog_sqlite() {
    timeout_backlog(Backend::Sqlite).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_backlog_mysql() {
    timeout_backlog(Backend::MySql).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_backlog_postgres() {
    timeout_backlog(Backend::Postgres).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_event_fence_sqlite() {
    expired_event_fence(Backend::Sqlite).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_event_fence_mysql() {
    expired_event_fence(Backend::MySql).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_event_fence_postgres() {
    expired_event_fence(Backend::Postgres).await;
}
