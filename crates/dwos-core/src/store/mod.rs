use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sqlx::any::{AnyConnectOptions, AnyPoolOptions, AnyRow, install_default_drivers};
use sqlx::{Any, AnyConnection, AnyPool, AssertSqlSafe, Connection, Row, Transaction};

use crate::Error;
use crate::retry::with_retry;
use crate::workflow::{Lease, PORTABLE_JSON, PY_PICKLE, Status, StepRecord, Workflow};

mod leases;
mod steps;
mod workflows;

const MAX_NAME_CHARS: usize = 255;
const POOL_SIZE: u32 = 10;
const LEASE_POOL_SIZE: u32 = 2;
const SQLITE_POOL_SIZE: u32 = 5;
const PACKET_MARGIN: u64 = 4096;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SQLITE_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(40);
const POSTGRES_MIGRATION_LOCK: &str =
    "SELECT 1 FROM (SELECT pg_advisory_xact_lock(7240319)) AS locked";
const SELECT_WORKFLOW: &str = "SELECT workflow_uuid, name, status, inputs, output, error, recovery_attempts, max_recovery_attempts, parent_workflow_id, created_at, updated_at, completed_at, serialization, lease_token \
     FROM workflow_status WHERE workflow_uuid = ?";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Dialect {
    Postgres,
    MySql,
    Sqlite,
}

impl Dialect {
    fn from_url(url: &str) -> Option<Self> {
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Some(Self::Postgres)
        } else if url.starts_with("mysql://") {
            Some(Self::MySql)
        } else if url.starts_with("sqlite://") {
            Some(Self::Sqlite)
        } else {
            None
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::MySql => "mysql",
            Self::Sqlite => "sqlite",
        }
    }

    // shortcut: 001 is edited in place and has no migration runner, add numbered migrations before the first release.
    fn schema(self) -> &'static str {
        match self {
            Self::Postgres => include_str!("../../schema/postgres/001_init.sql"),
            Self::MySql => include_str!("../../schema/mysql/001_init.sql"),
            Self::Sqlite => include_str!("../../schema/sqlite/001_init.sql"),
        }
    }

    fn session_settings(self, read_only: bool) -> &'static str {
        match (self, read_only) {
            (Self::Postgres, _) => "SET lock_timeout = '10s'",
            (Self::MySql, _) => {
                "SET SESSION transaction_isolation = 'READ-COMMITTED', time_zone = '+00:00', \
                 innodb_lock_wait_timeout = 10, sql_mode = CONCAT(@@sql_mode, ',STRICT_ALL_TABLES')"
            }
            // Read-only must not set WAL: that writes to the file the viewer is only looking at.
            (Self::Sqlite, true) => {
                "PRAGMA busy_timeout = 30000; PRAGMA query_only = ON; PRAGMA foreign_keys = ON"
            }
            // busy_timeout comes first so that the other settings wait for a busy database.
            (Self::Sqlite, false) => {
                "PRAGMA busy_timeout = 30000; PRAGMA journal_mode = WAL; \
                 PRAGMA synchronous = FULL; PRAGMA foreign_keys = ON"
            }
        }
    }

    fn now_ms(self) -> &'static str {
        match self {
            Self::Postgres => "(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT",
            Self::MySql => "CAST(UNIX_TIMESTAMP(NOW(6)) * 1000 AS SIGNED)",
            Self::Sqlite => "CAST(unixepoch('subsec') * 1000 AS INTEGER)",
        }
    }

    fn insert_if_absent(self) -> &'static str {
        match self {
            Self::Postgres | Self::Sqlite => "ON CONFLICT (workflow_uuid) DO NOTHING",
            Self::MySql => "ON DUPLICATE KEY UPDATE workflow_uuid = workflow_uuid",
        }
    }

    // SQLite takes the whole write lock in `begin`, so it has no row locks.
    fn lock_row(self) -> &'static str {
        match self {
            Self::Postgres | Self::MySql => "FOR UPDATE",
            Self::Sqlite => "",
        }
    }

    fn lock_free_rows(self) -> &'static str {
        match self {
            Self::Postgres | Self::MySql => "FOR UPDATE SKIP LOCKED",
            Self::Sqlite => "",
        }
    }
}

fn check_name(field: &'static str, value: &str) -> Result<(), Error> {
    if value.contains('\0') {
        return Err(Error::ContainsNul { field });
    }
    if value.chars().count() > MAX_NAME_CHARS {
        return Err(Error::NameTooLong { field });
    }
    Ok(())
}

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn get_u32(row: &AnyRow, column: &str) -> Result<u32, Error> {
    let value: i64 = row.try_get(column)?;
    u32::try_from(value).map_err(|_| Error::Corrupt(format!("{column} is out of range: {value}")))
}

fn workflow_from_row(row: &AnyRow) -> Result<Workflow, Error> {
    Ok(Workflow {
        workflow_id: row.try_get("workflow_uuid")?,
        name: row.try_get("name")?,
        status: Status::parse(row.try_get("status")?)?,
        input: row.try_get("inputs")?,
        output: row.try_get("output")?,
        error: row.try_get("error")?,
        recovery_attempts: get_u32(row, "recovery_attempts")?,
        max_recovery_attempts: get_u32(row, "max_recovery_attempts")?,
        parent_workflow_id: row.try_get("parent_workflow_id")?,
        created_at_ms: row.try_get("created_at")?,
        updated_at_ms: row.try_get("updated_at")?,
        completed_at_ms: row.try_get("completed_at")?,
        serialization: row.try_get("serialization")?,
    })
}

fn record_from_row(row: &AnyRow) -> Result<StepRecord, Error> {
    Ok(StepRecord {
        output: row.try_get("output")?,
        error: row.try_get("error")?,
        child_workflow_id: row.try_get("child_workflow_id")?,
    })
}

fn require_owner(changed: u64, lease: &Lease) -> Result<(), Error> {
    if changed == 0 {
        return Err(Error::LeaseLost(lease.workflow_id.clone()));
    }
    Ok(())
}

fn pool_options(dialect: Dialect, size: u32, read_only: bool) -> AnyPoolOptions {
    let timeout = match dialect {
        Dialect::Sqlite => SQLITE_ACQUIRE_TIMEOUT,
        Dialect::Postgres | Dialect::MySql => CONNECT_TIMEOUT,
    };
    let settings = dialect.session_settings(read_only);
    AnyPoolOptions::new()
        .max_connections(size)
        .acquire_timeout(timeout)
        .after_connect(move |conn, _| {
            Box::pin(async move {
                sqlx::raw_sql(settings).execute(conn).await?;
                Ok(())
            })
        })
}

// A first connection reports a wrong password at once, not after the pool's timeout.
async fn probe(dialect: Dialect, options: &AnyConnectOptions) -> Result<Option<u64>, Error> {
    let mut conn = AnyConnection::connect_with(options).await?;
    sqlx::raw_sql(dialect.session_settings(false))
        .execute(&mut conn)
        .await?;
    let max_packet = if dialect == Dialect::MySql {
        let value: i64 = sqlx::query_scalar("SELECT CAST(@@max_allowed_packet AS SIGNED)")
            .fetch_one(&mut conn)
            .await?;
        Some(value.unsigned_abs())
    } else {
        None
    };
    conn.close().await?;
    Ok(max_packet)
}

fn sqlite_url(url: &str, read_only: bool) -> String {
    if read_only {
        let base = url.split('?').next().unwrap_or(url);
        return format!("{base}?mode=ro");
    }
    match url.split_once('?') {
        Some((_, query)) if query.split('&').any(|part| part.starts_with("mode=")) => {
            url.to_owned()
        }
        Some(_) => format!("{url}&mode=rwc"),
        None => format!("{url}?mode=rwc"),
    }
}

#[derive(Clone, Debug)]
pub struct Store {
    dialect: Dialect,
    pool: AnyPool,
    lease_pool: AnyPool,
    max_packet: Option<u64>,
    retries: Arc<AtomicU64>,
    pickle: bool,
}

impl Store {
    pub async fn connect(url: &str) -> Result<Self, Error> {
        Self::open(url, false).await
    }

    pub async fn connect_read_only(url: &str) -> Result<Self, Error> {
        Self::open(url, true).await
    }

    async fn open(url: &str, read_only: bool) -> Result<Self, Error> {
        let dialect = Dialect::from_url(url).ok_or(Error::UnsupportedUrl)?;
        install_default_drivers();

        if dialect == Dialect::Sqlite {
            let options = AnyConnectOptions::from_str(&sqlite_url(url, read_only))
                .map_err(|_| Error::InvalidUrl)?;
            let pool = pool_options(dialect, SQLITE_POOL_SIZE, read_only)
                .connect_with(options)
                .await?;
            log::info!("connected to {}", dialect.name());
            return Ok(Self {
                dialect,
                lease_pool: pool.clone(),
                pool,
                max_packet: None,
                retries: Arc::new(AtomicU64::new(0)),
                pickle: false,
            });
        }

        let options = AnyConnectOptions::from_str(url).map_err(|_| Error::InvalidUrl)?;
        let max_packet = tokio::time::timeout(CONNECT_TIMEOUT, probe(dialect, &options))
            .await
            .map_err(|_| Error::Timeout(CONNECT_TIMEOUT))??;
        log::info!("connected to {}", dialect.name());
        // Connects now: opening a connection takes about 50ms, too long for a short lease to wait.
        let lease_pool = pool_options(dialect, LEASE_POOL_SIZE, read_only)
            .min_connections(1)
            .connect_with(options.clone())
            .await?;
        Ok(Self {
            dialect,
            pool: pool_options(dialect, POOL_SIZE, read_only).connect_lazy_with(options),
            lease_pool,
            max_packet,
            retries: Arc::new(AtomicU64::new(0)),
            pickle: false,
        })
    }

    /// Lets this store claim pickled workflows. Without it they wait for a process that can read them.
    pub fn with_pickle(mut self) -> Self {
        self.pickle = true;
        self
    }

    fn readable_formats(&self) -> String {
        if self.pickle {
            format!("'{PORTABLE_JSON}', '{PY_PICKLE}'")
        } else {
            format!("'{PORTABLE_JSON}'")
        }
    }

    pub fn backend(&self) -> &'static str {
        self.dialect.name()
    }

    pub fn transient_retries(&self) -> u64 {
        self.retries.load(Ordering::Relaxed)
    }

    /// For tests that must reach the database on the store's own connections.
    #[doc(hidden)]
    pub fn pool(&self) -> &AnyPool {
        &self.pool
    }

    /// Turns shared SQL into this database's SQL: fills in `{NOW}`, and `?` becomes `$n` on Postgres.
    fn sql(&self, text: &str) -> AssertSqlSafe<String> {
        let text = text.replace("{NOW}", self.dialect.now_ms());
        if self.dialect != Dialect::Postgres {
            return AssertSqlSafe(text);
        }
        let mut rendered = String::with_capacity(text.len());
        for (index, piece) in text.split('?').enumerate() {
            if index > 0 {
                rendered.push('$');
                rendered.push_str(&index.to_string());
            }
            rendered.push_str(piece);
        }
        AssertSqlSafe(rendered)
    }

    // SQLite waits on its own busy timeout. It has no connection to lose, so it does not retry.
    async fn attempt<T, F, Fut>(&self, mut operation: F) -> Result<T, Error>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        if self.dialect == Dialect::Sqlite {
            return operation().await;
        }
        with_retry(&self.retries, operation).await
    }

    async fn begin(&self) -> Result<Transaction<'static, Any>, sqlx::Error> {
        match self.dialect {
            Dialect::Sqlite => self.pool.begin_with("BEGIN IMMEDIATE").await,
            Dialect::Postgres | Dialect::MySql => self.pool.begin().await,
        }
    }

    fn check_size(&self, values: &[Option<&str>]) -> Result<(), Error> {
        let Some(max_packet) = self.max_packet else {
            return Ok(());
        };
        let size: u64 = values
            .iter()
            .flatten()
            .map(|value| value.len() as u64)
            .sum();
        let limit = max_packet.saturating_sub(PACKET_MARGIN);
        if size > limit {
            return Err(Error::ValueTooLarge { size, limit });
        }
        Ok(())
    }

    pub async fn migrate(&self) -> Result<(), Error> {
        let schema = AssertSqlSafe(self.dialect.schema());
        match self.dialect {
            Dialect::MySql => {
                sqlx::raw_sql(schema).execute(&self.pool).await?;
            }
            Dialect::Sqlite => {
                let mut tx = self.begin().await?;
                sqlx::raw_sql(schema).execute(&mut *tx).await?;
                tx.commit().await?;
            }
            // Concurrent CREATE TABLE IF NOT EXISTS can fail on Postgres, so copies take turns.
            Dialect::Postgres => {
                let mut tx = self.begin().await?;
                sqlx::raw_sql(POSTGRES_MIGRATION_LOCK)
                    .execute(&mut *tx)
                    .await?;
                sqlx::raw_sql(schema).execute(&mut *tx).await?;
                tx.commit().await?;
            }
        }
        Ok(())
    }
}
