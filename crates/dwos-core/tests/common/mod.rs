#![allow(dead_code)]

use std::time::Duration;

use dwos_core::{Lease, NewWorkflow, PORTABLE_JSON, Started, StepRecord, Store};
use sqlx::Connection;
use sqlx::mysql::MySqlConnection;
use sqlx::postgres::PgConnection;
use tempfile::TempDir;

pub const LONG: Duration = Duration::from_secs(600);
pub const EXPIRED: Duration = Duration::ZERO;

#[derive(Clone, Copy, Debug)]
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
    pub async fn create(backend: Backend) -> Self {
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
                let name = format!("dbos_t_{}", uuid::Uuid::new_v4().simple());
                let admin = mysql_admin_url();
                let mut conn = MySqlConnection::connect(&admin).await.unwrap();
                sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}")))
                    .execute(&mut conn)
                    .await
                    .unwrap();
                conn.close().await.unwrap();
                let base = admin.rsplit_once('/').unwrap().0;
                Self {
                    url: format!("{base}/{name}"),
                    mysql_database: Some(name),
                    postgres_database: None,
                    _dir: None,
                }
            }
            Backend::Postgres => {
                let name = format!("dbos_t_{}", uuid::Uuid::new_v4().simple());
                let admin = postgres_admin_url();
                let mut conn = PgConnection::connect(&admin).await.unwrap();
                sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}")))
                    .execute(&mut conn)
                    .await
                    .unwrap();
                conn.close().await.unwrap();
                let base = admin.rsplit_once('/').unwrap().0;
                Self {
                    url: format!("{base}/{name}"),
                    mysql_database: None,
                    postgres_database: Some(name),
                    _dir: None,
                }
            }
        }
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
                    // FORCE: a store's pool may still hold connections while it closes.
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

pub async fn setup(backend: Backend) -> (TestDb, Store) {
    let db = TestDb::create(backend).await;
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
