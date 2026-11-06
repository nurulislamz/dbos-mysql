#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use sqlx::mysql::{MySqlConnection, MySqlPool, MySqlPoolOptions};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Connection, Row};
use tempfile::TempDir;
use tokio::sync::{OnceCell, mpsc, oneshot};
use yado_core::{Lease, NewWorkflow, PORTABLE_JSON, Started, StepRecord, Store};

pub const LONG: Duration = Duration::from_secs(600);
pub const EXPIRED: Duration = Duration::ZERO;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backend {
    Sqlite,
    MySql,
    Postgres,
}

pub fn mysql_admin_url() -> String {
    std::env::var("DBOS_TEST_MYSQL_URL")
        .unwrap_or_else(|_| "mysql://root:dbos-test-root@127.0.0.1:13307/dbos_test".to_owned())
}

pub fn postgres_admin_url() -> String {
    std::env::var("DBOS_TEST_POSTGRES_URL").unwrap_or_else(|_| {
        "postgres://postgres:dbos-test-root@127.0.0.1:13309/dbos_test".to_owned()
    })
}

pub struct TestDb {
    pub url: String,
    pub mysql_database: Option<String>,
    pub postgres_database: Option<String>,
    pub _dir: Option<TempDir>,
}

impl TestDb {
    /// Empty database. Migration tests, and engines that refuse to migrate, start here.
    pub async fn create(backend: Backend) -> Self {
        Self::provision(backend, false).await
    }

    /// Current schema, copied from a process-wide template. `migrate` only checks the version.
    pub async fn migrated(backend: Backend) -> Self {
        Self::provision(backend, true).await
    }

    async fn provision(backend: Backend, migrated: bool) -> Self {
        if backend == Backend::Sqlite {
            return sqlite_db(migrated).await;
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        admin()
            .send(Request::Make {
                backend,
                migrated,
                reply: reply_tx,
            })
            .expect("the test database admin thread has stopped");
        tokio::time::timeout(Duration::from_secs(60), reply_rx)
            .await
            .expect("timed out creating a test database")
            .expect("the test database admin dropped the reply")
    }

    pub async fn open(&self) -> Store {
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
        let (reply_tx, reply_rx) = oneshot::channel();
        admin()
            .send(Request::Remove {
                mysql,
                postgres,
                reply: reply_tx,
            })
            .expect("the test database admin thread has stopped");
        // `blocking_recv` panics on a runtime thread. The admin runtime does the drop.
        std::thread::spawn(move || {
            reply_rx
                .blocking_recv()
                .expect("the test database admin dropped the reply");
        })
        .join()
        .unwrap();
    }
}

enum Request {
    Make {
        backend: Backend,
        migrated: bool,
        reply: oneshot::Sender<TestDb>,
    },
    Remove {
        mysql: Option<String>,
        postgres: Option<String>,
        reply: oneshot::Sender<()>,
    },
}

struct MysqlTemplate {
    ddl: Vec<String>,
    versions: Vec<i64>,
    charset: String,
    collation: String,
}

fn admin() -> &'static mpsc::UnboundedSender<Request> {
    static ADMIN: OnceLock<mpsc::UnboundedSender<Request>> = OnceLock::new();
    ADMIN.get_or_init(|| {
        let (tx, mut rx) = mpsc::unbounded_channel();
        std::thread::Builder::new()
            .name("dbos-test-admin".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(admin_loop(&mut rx));
            })
            .unwrap();
        tx
    })
}

async fn admin_loop(rx: &mut mpsc::UnboundedReceiver<Request>) {
    let mysql = MySqlPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&mysql_admin_url())
        .await
        .unwrap();
    let postgres = PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&postgres_admin_url())
        .await
        .unwrap();
    drop_stale_templates(&mysql, &postgres).await;
    let mysql_template = Arc::new(OnceCell::<MysqlTemplate>::new());
    let postgres_template = Arc::new(OnceCell::<String>::new());
    while let Some(request) = rx.recv().await {
        let mysql = mysql.clone();
        let postgres = postgres.clone();
        let mysql_template = mysql_template.clone();
        let postgres_template = postgres_template.clone();
        tokio::spawn(async move {
            match request {
                Request::Make {
                    backend,
                    migrated,
                    reply,
                } => {
                    let db = match backend {
                        Backend::MySql => mysql_db(&mysql, &mysql_template, migrated).await,
                        Backend::Postgres => {
                            postgres_db(&postgres, &postgres_template, migrated).await
                        }
                        Backend::Sqlite => unreachable!("sqlite is created in the test runtime"),
                    };
                    let _ = reply.send(db);
                }
                Request::Remove {
                    mysql: mysql_name,
                    postgres: postgres_name,
                    reply,
                } => {
                    if let Some(name) = mysql_name {
                        drop_mysql(&mysql, &name).await;
                    }
                    if let Some(name) = postgres_name {
                        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                            "DROP DATABASE IF EXISTS {name} WITH (FORCE)"
                        )))
                        .execute(&postgres)
                        .await
                        .unwrap();
                    }
                    let _ = reply.send(());
                }
            }
        });
    }
}

fn split_database(url: &str) -> (String, String) {
    let (base, name) = url.rsplit_once('/').unwrap();
    (base.to_owned(), name.split('?').next().unwrap().to_owned())
}

fn fresh_name() -> String {
    format!("dbos_t_{}", uuid::Uuid::new_v4().simple())
}

async fn mysql_db(pool: &MySqlPool, template: &OnceCell<MysqlTemplate>, migrated: bool) -> TestDb {
    let name = fresh_name();
    let admin = mysql_admin_url();
    let (base, _) = split_database(&admin);
    if migrated {
        let owned = pool.clone();
        let template = template
            .get_or_init(|| async move { mysql_template(&owned).await })
            .await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE DATABASE {name} CHARACTER SET {} COLLATE {}",
            template.charset, template.collation
        )))
        .execute(pool)
        .await
        .unwrap();
        let mut conn = MySqlConnection::connect(&format!("{base}/{name}"))
            .await
            .unwrap();
        sqlx::raw_sql("SET FOREIGN_KEY_CHECKS = 0")
            .execute(&mut conn)
            .await
            .unwrap();
        for statement in &template.ddl {
            sqlx::raw_sql(sqlx::AssertSqlSafe(statement.clone()))
                .execute(&mut conn)
                .await
                .unwrap();
        }
        for version in &template.versions {
            sqlx::query("INSERT INTO dwos_schema_migrations (version) VALUES (?)")
                .bind(version)
                .execute(&mut conn)
                .await
                .unwrap();
        }
        sqlx::raw_sql("SET FOREIGN_KEY_CHECKS = 1")
            .execute(&mut conn)
            .await
            .unwrap();
        conn.close().await.unwrap();
    } else {
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}")))
            .execute(pool)
            .await
            .unwrap();
    }
    TestDb {
        url: format!("{base}/{name}"),
        mysql_database: Some(name),
        postgres_database: None,
        _dir: None,
    }
}

async fn mysql_template(pool: &MySqlPool) -> MysqlTemplate {
    let name = format!("dbos_tpl_my_{}", std::process::id());
    let admin = mysql_admin_url();
    let (base, _) = split_database(&admin);
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE IF EXISTS {name}"
    )))
    .execute(pool)
    .await
    .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}")))
        .execute(pool)
        .await
        .unwrap();
    let store = Store::connect(&format!("{base}/{name}")).await.unwrap();
    store.migrate().await.unwrap();
    let row = sqlx::query(
        "SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
    )
    .bind(&name)
    .fetch_one(pool)
    .await
    .unwrap();
    let charset: String = row.try_get(0).unwrap();
    let collation: String = row.try_get(1).unwrap();
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME",
    )
    .bind(&name)
    .fetch_all(pool)
    .await
    .unwrap();
    let mut ddl = Vec::with_capacity(tables.len());
    for table in tables {
        let create = format!("SHOW CREATE TABLE `{name}`.`{table}`");
        let row = sqlx::query(sqlx::AssertSqlSafe(create))
            .fetch_one(pool)
            .await
            .unwrap();
        ddl.push(row.try_get::<String, _>(1).unwrap());
    }
    let versions: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT version FROM `{name}`.dwos_schema_migrations ORDER BY version"
    )))
    .fetch_all(pool)
    .await
    .unwrap();
    store.pool().close().await;
    drop(store);
    kill_mysql(pool, &name).await;
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE IF EXISTS {name}"
    )))
    .execute(pool)
    .await
    .unwrap();
    MysqlTemplate {
        ddl,
        versions,
        charset,
        collation,
    }
}

async fn postgres_db(pool: &PgPool, template: &OnceCell<String>, migrated: bool) -> TestDb {
    let name = fresh_name();
    let admin = postgres_admin_url();
    let (base, _) = split_database(&admin);
    let statement = if migrated {
        let owned = pool.clone();
        let template = template
            .get_or_init(|| async move { postgres_template(&owned).await })
            .await;
        format!("CREATE DATABASE {name} TEMPLATE {template}")
    } else {
        format!("CREATE DATABASE {name}")
    };
    sqlx::raw_sql(sqlx::AssertSqlSafe(statement))
        .execute(pool)
        .await
        .unwrap();
    TestDb {
        url: format!("{base}/{name}"),
        mysql_database: None,
        postgres_database: Some(name),
        _dir: None,
    }
}

async fn postgres_template(pool: &PgPool) -> String {
    let name = format!("dbos_tpl_pg_{}", std::process::id());
    let admin = postgres_admin_url();
    let (base, _) = split_database(&admin);
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE IF EXISTS {name} WITH (FORCE)"
    )))
    .execute(pool)
    .await
    .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}")))
        .execute(pool)
        .await
        .unwrap();
    let store = Store::connect(&format!("{base}/{name}")).await.unwrap();
    store.migrate().await.unwrap();
    store.pool().close().await;
    drop(store);
    for _ in 0..50 {
        let left: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname = $1")
                .bind(&name)
                .fetch_one(pool)
                .await
                .unwrap();
        if left == 0 {
            break;
        }
        sqlx::query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
        )
        .bind(&name)
        .execute(pool)
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    sqlx::query("UPDATE pg_database SET datallowconn = false WHERE datname = $1")
        .bind(&name)
        .execute(pool)
        .await
        .unwrap();
    name
}

async fn sqlite_db(migrated: bool) -> TestDb {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.sqlite");
    if migrated {
        let template = sqlite_template().await;
        std::fs::copy(template, &path).unwrap();
        for suffix in ["-wal", "-shm"] {
            let extra = PathBuf::from(format!("{}{suffix}", template.display()));
            if extra.exists() {
                std::fs::copy(&extra, format!("{}{suffix}", path.display())).unwrap();
            }
        }
    }
    TestDb {
        url: format!("sqlite://{}", path.display()),
        mysql_database: None,
        postgres_database: None,
        _dir: Some(dir),
    }
}

async fn sqlite_template() -> &'static Path {
    static TEMPLATE: OnceCell<PathBuf> = OnceCell::const_new();
    TEMPLATE
        .get_or_init(|| async {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("template.sqlite");
            let url = format!("sqlite://{}", path.display());
            let store = Store::connect(&url).await.unwrap();
            store.migrate().await.unwrap();
            store.pool().close().await;
            drop(store);
            let mut conn = sqlx::sqlite::SqliteConnection::connect(&url).await.unwrap();
            sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
                .execute(&mut conn)
                .await
                .unwrap();
            let _mode: String = sqlx::query_scalar("PRAGMA journal_mode = DELETE")
                .fetch_one(&mut conn)
                .await
                .unwrap();
            conn.close().await.unwrap();
            dir.keep().join("template.sqlite")
        })
        .await
}

async fn drop_mysql(pool: &MySqlPool, name: &str) {
    kill_mysql(pool, name).await;
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE IF EXISTS {name}"
    )))
    .execute(pool)
    .await
    .unwrap();
}

async fn kill_mysql(pool: &MySqlPool, name: &str) {
    let ids: Vec<u64> = sqlx::query_scalar(
        "SELECT id FROM information_schema.processlist WHERE db = ? AND id <> CONNECTION_ID()",
    )
    .bind(name)
    .fetch_all(pool)
    .await
    .unwrap();
    for id in ids {
        let _ = sqlx::raw_sql(sqlx::AssertSqlSafe(format!("KILL {id}")))
            .execute(pool)
            .await;
    }
}

fn pid_is_gone(pid: u32) -> bool {
    Path::new("/proc").exists() && !Path::new(&format!("/proc/{pid}")).exists()
}

fn stale_template(name: &str) -> bool {
    let Some(pid) = name.rsplit('_').next().and_then(|pid| pid.parse().ok()) else {
        return false;
    };
    pid != std::process::id() && pid_is_gone(pid)
}

async fn drop_stale_templates(mysql: &MySqlPool, postgres: &PgPool) {
    let schemas: Vec<String> = sqlx::query_scalar(
        "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE 'dbos_tpl_%'",
    )
    .fetch_all(mysql)
    .await
    .unwrap();
    for name in schemas {
        if stale_template(&name) {
            let _ = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "DROP DATABASE IF EXISTS {name}"
            )))
            .execute(mysql)
            .await;
        }
    }
    let databases: Vec<String> =
        sqlx::query_scalar("SELECT datname FROM pg_database WHERE datname LIKE 'dbos_tpl_%'")
            .fetch_all(postgres)
            .await
            .unwrap();
    for name in databases {
        if stale_template(&name) {
            let _ = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "DROP DATABASE IF EXISTS {name} WITH (FORCE)"
            )))
            .execute(postgres)
            .await;
        }
    }
}

pub async fn setup(backend: Backend) -> (TestDb, Store) {
    let db = TestDb::migrated(backend).await;
    let store = db.open().await;
    (db, store)
}

#[allow(unused_macros)]
macro_rules! all_dbs {
    ($name:ident, $backend:ident, $body:block) => {
        mod $name {
            use super::*;

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn sqlite() {
                let $backend = Backend::Sqlite;
                $body
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn mysql() {
                let $backend = Backend::MySql;
                $body
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn postgres() {
                let $backend = Backend::Postgres;
                $body
            }
        }
    };
}

pub fn workflow<'a>(id: &'a str, lease: Duration) -> NewWorkflow<'a> {
    NewWorkflow {
        workflow_id: id,
        name: "wf",
        input: "{}",
        max_recovery_attempts: 100,
        lease,
        parent_workflow_id: None,
        serialization: PORTABLE_JSON,
        timeout_ms: None,
        deadline_epoch_ms: None,
    }
}

pub async fn start(store: &Store, new: &NewWorkflow<'_>) -> Lease {
    match store.start_workflow(new).await.unwrap() {
        Started::Owned(lease, _) => lease,
        Started::Existing(existing) => panic!("expected a new workflow, got {existing:?}"),
    }
}

pub fn output(value: &str) -> StepRecord {
    StepRecord {
        output: Some(value.to_owned()),
        ..StepRecord::default()
    }
}

pub fn names() -> Vec<String> {
    vec!["wf".to_owned()]
}
