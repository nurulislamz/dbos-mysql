use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sqlx::any::{AnyConnectOptions, AnyPoolOptions, AnyRow, install_default_drivers};
use sqlx::{Any, AnyConnection, AnyPool, AssertSqlSafe, Connection, Row, Transaction};

use crate::Error;
use crate::lease::LeaseKeeper;
use crate::retry::with_retry;
use crate::workflow::{
    Claimed, Lease, NewWorkflow, Outcome, PORTABLE_JSON, PY_PICKLE, RecordedStep, Started, Status,
    StepRecord, Workflow, attempts_exhausted, check_recorded_step, check_step_name,
    check_workflow_name, new_token,
};

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

    fn schema(self) -> &'static str {
        match self {
            Self::Postgres => include_str!("../schema/postgres/001_init.sql"),
            Self::MySql => include_str!("../schema/mysql/001_init.sql"),
            Self::Sqlite => include_str!("../schema/sqlite/001_init.sql"),
        }
    }

    fn session_settings(self) -> &'static str {
        match self {
            Self::Postgres => "SET lock_timeout = '10s'",
            Self::MySql => {
                "SET SESSION transaction_isolation = 'READ-COMMITTED', time_zone = '+00:00', \
                 innodb_lock_wait_timeout = 10, sql_mode = CONCAT(@@sql_mode, ',STRICT_ALL_TABLES')"
            }
            // busy_timeout comes first so that the other settings wait for a busy database.
            Self::Sqlite => {
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

fn pool_options(dialect: Dialect, size: u32) -> AnyPoolOptions {
    let timeout = match dialect {
        Dialect::Sqlite => SQLITE_ACQUIRE_TIMEOUT,
        Dialect::Postgres | Dialect::MySql => CONNECT_TIMEOUT,
    };
    let settings = dialect.session_settings();
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
    sqlx::raw_sql(dialect.session_settings())
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

fn sqlite_url(url: &str) -> String {
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
        let dialect = Dialect::from_url(url).ok_or(Error::UnsupportedUrl)?;
        install_default_drivers();

        if dialect == Dialect::Sqlite {
            let options =
                AnyConnectOptions::from_str(&sqlite_url(url)).map_err(|_| Error::InvalidUrl)?;
            let pool = pool_options(dialect, SQLITE_POOL_SIZE)
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
        let lease_pool = pool_options(dialect, LEASE_POOL_SIZE)
            .min_connections(1)
            .connect_with(options.clone())
            .await?;
        Ok(Self {
            dialect,
            pool: pool_options(dialect, POOL_SIZE).connect_lazy_with(options),
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

    pub async fn start_workflow(&self, new: &NewWorkflow<'_>) -> Result<Started, Error> {
        check_name("workflow ID", new.workflow_id)?;
        check_name("workflow name", new.name)?;
        self.check_size(&[Some(new.input)])?;
        let token = new_token();
        self.attempt(|| self.start_workflow_once(new, &token)).await
    }

    async fn start_workflow_once(
        &self,
        new: &NewWorkflow<'_>,
        token: &str,
    ) -> Result<Started, Error> {
        let mut tx = self.begin().await?;
        sqlx::query(self.sql(&format!(
            "INSERT INTO workflow_status (workflow_uuid, name, status, inputs, claim_after, lease_token, max_recovery_attempts, parent_workflow_id, serialization, created_at, updated_at) \
             VALUES (?, ?, 'PENDING', ?, {{NOW}} + ?, ?, ?, ?, ?, {{NOW}}, {{NOW}}) {}",
            self.dialect.insert_if_absent()
        )))
        .bind(new.workflow_id)
        .bind(new.name)
        .bind(new.input)
        .bind(millis(new.lease))
        .bind(token)
        .bind(i64::from(new.max_recovery_attempts))
        .bind(new.parent_workflow_id)
        .bind(new.serialization)
        .execute(&mut *tx)
        .await?;

        // Compare tokens: a duplicate insert reports 1 row on MySQL and 0 on the others.
        let row = sqlx::query(self.sql(SELECT_WORKFLOW))
            .bind(new.workflow_id)
            .fetch_one(&mut *tx)
            .await?;
        let owner: Option<String> = row.try_get("lease_token")?;
        if owner.as_deref() == Some(token) {
            tx.commit().await?;
            return Ok(Started::Owned(Lease {
                workflow_id: new.workflow_id.to_owned(),
                token: token.to_owned(),
            }));
        }
        let existing = workflow_from_row(&row)?;
        check_workflow_name(new, &existing.name)?;
        tx.commit().await?;
        Ok(Started::Existing(existing))
    }

    pub async fn get_workflow(&self, workflow_id: &str) -> Result<Option<Workflow>, Error> {
        self.attempt(|| async {
            let row = sqlx::query(self.sql(SELECT_WORKFLOW))
                .bind(workflow_id)
                .fetch_optional(&self.pool)
                .await?;
            row.as_ref().map(workflow_from_row).transpose()
        })
        .await
    }

    pub async fn load_step(
        &self,
        workflow_id: &str,
        step_id: u32,
        step_name: &str,
    ) -> Result<Option<StepRecord>, Error> {
        self.attempt(|| async {
            let mut conn = self.pool.acquire().await?;
            let exists =
                sqlx::query(self.sql("SELECT 1 FROM workflow_status WHERE workflow_uuid = ?"))
                    .bind(workflow_id)
                    .fetch_optional(&mut *conn)
                    .await?;
            if exists.is_none() {
                return Err(Error::WorkflowNotFound(workflow_id.to_owned()));
            }
            let Some((recorded_name, record)) =
                self.read_step(&mut conn, workflow_id, step_id).await?
            else {
                return Ok(None);
            };
            check_step_name(workflow_id, step_id, step_name, recorded_name)?;
            Ok(Some(record))
        })
        .await
    }

    async fn read_step(
        &self,
        conn: &mut AnyConnection,
        workflow_id: &str,
        step_id: u32,
    ) -> Result<Option<(String, StepRecord)>, Error> {
        let row = sqlx::query(self.sql(
            "SELECT function_name, output, error, child_workflow_id FROM operation_outputs WHERE workflow_uuid = ? AND function_id = ?",
        ))
        .bind(workflow_id)
        .bind(i64::from(step_id))
        .fetch_optional(conn)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some((
            row.try_get("function_name")?,
            record_from_row(&row)?,
        )))
    }

    pub async fn save_step(
        &self,
        lease: &Lease,
        step_id: u32,
        step_name: &str,
        record: &StepRecord,
    ) -> Result<(), Error> {
        check_name("step name", step_name)?;
        if let Some(child) = &record.child_workflow_id {
            check_name("child workflow ID", child)?;
        }
        self.check_size(&[record.output.as_deref(), record.error.as_deref()])?;
        self.attempt(|| self.save_step_once(lease, step_id, step_name, record))
            .await
    }

    async fn save_step_once(
        &self,
        lease: &Lease,
        step_id: u32,
        step_name: &str,
        record: &StepRecord,
    ) -> Result<(), Error> {
        let mut tx = self.begin().await?;
        let owner = sqlx::query(self.sql(&format!(
            "SELECT serialization FROM workflow_status WHERE workflow_uuid = ? AND lease_token = ? AND status = 'PENDING' {}",
            self.dialect.lock_row()
        )))
        .bind(lease.workflow_id.as_str())
        .bind(lease.token.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(owner) = owner else {
            return Err(Error::LeaseLost(lease.workflow_id.clone()));
        };
        let serialization: String = owner.try_get("serialization")?;
        if let Some(recorded) = self.read_step(&mut tx, &lease.workflow_id, step_id).await? {
            return check_recorded_step(&lease.workflow_id, step_id, step_name, record, &recorded);
        }
        sqlx::query(self.sql(
            "INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, output, error, child_workflow_id, serialization, completed_at_epoch_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, {NOW})",
        ))
        .bind(lease.workflow_id.as_str())
        .bind(i64::from(step_id))
        .bind(step_name)
        .bind(record.output.as_deref())
        .bind(record.error.as_deref())
        .bind(record.child_workflow_id.as_deref())
        .bind(serialization.as_str())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn finish_workflow(&self, lease: &Lease, outcome: &Outcome) -> Result<(), Error> {
        let (status, output, error) = match outcome {
            Outcome::Success(value) => (Status::Success, Some(value.as_str()), None),
            Outcome::Error(value) => (Status::Error, None, Some(value.as_str())),
        };
        self.check_size(&[output, error])?;
        let changed = self
            .attempt(|| async {
                let result = sqlx::query(self.sql(
                    "UPDATE workflow_status SET status = ?, output = ?, error = ?, lease_token = NULL, claim_after = NULL, completed_at = {NOW}, updated_at = {NOW} WHERE workflow_uuid = ? AND lease_token = ? AND status = 'PENDING'",
                ))
                .bind(status.as_str())
                .bind(output)
                .bind(error)
                .bind(lease.workflow_id.as_str())
                .bind(lease.token.as_str())
                .execute(&self.pool)
                .await?;
                Ok(result.rows_affected())
            })
            .await?;
        require_owner(changed, lease)
    }

    // Uses its own pool, so busy workflows cannot starve a renewal.
    pub async fn renew_lease(&self, lease: &Lease, duration: Duration) -> Result<(), Error> {
        let changed = self
            .attempt(|| async {
                let result = sqlx::query(self.sql(
                    "UPDATE workflow_status SET claim_after = {NOW} + ?, updated_at = {NOW} \
                     WHERE workflow_uuid = ? AND lease_token = ? AND status = 'PENDING'",
                ))
                .bind(millis(duration))
                .bind(lease.workflow_id.as_str())
                .bind(lease.token.as_str())
                .execute(&self.lease_pool)
                .await?;
                Ok(result.rows_affected())
            })
            .await?;
        require_owner(changed, lease)
    }

    pub async fn release_lease(&self, lease: &Lease) -> Result<(), Error> {
        let changed = self
            .attempt(|| async {
                let result = sqlx::query(self.sql(
                    "UPDATE workflow_status SET lease_token = NULL, claim_after = {NOW}, \
                     recovery_attempts = recovery_attempts - 1, updated_at = {NOW} \
                     WHERE workflow_uuid = ? AND lease_token = ? AND status = 'PENDING'",
                ))
                .bind(lease.workflow_id.as_str())
                .bind(lease.token.as_str())
                .execute(&self.pool)
                .await?;
                Ok(result.rows_affected())
            })
            .await?;
        require_owner(changed, lease)
    }

    pub async fn reclaim(
        &self,
        names: &[String],
        limit: u32,
        duration: Duration,
    ) -> Result<Vec<Claimed>, Error> {
        if names.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        self.attempt(|| self.reclaim_once(names, limit, duration))
            .await
    }

    async fn reclaim_once(
        &self,
        names: &[String],
        limit: u32,
        duration: Duration,
    ) -> Result<Vec<Claimed>, Error> {
        let mut tx = self.begin().await?;
        let select_sql = self.sql(&format!(
            "SELECT workflow_uuid, name, inputs, recovery_attempts, max_recovery_attempts, serialization FROM workflow_status \
             WHERE claim_after <= {{NOW}} AND status = 'PENDING' AND serialization IN ({}) AND name IN ({}) \
             ORDER BY claim_after, workflow_uuid LIMIT ? {}",
            self.readable_formats(),
            vec!["?"; names.len()].join(", "),
            self.dialect.lock_free_rows()
        ));
        let mut select = sqlx::query(select_sql);
        for name in names {
            select = select.bind(name.as_str());
        }
        let rows = select.bind(i64::from(limit)).fetch_all(&mut *tx).await?;

        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let workflow_id: String = row.try_get("workflow_uuid")?;
            let attempts = get_u32(&row, "recovery_attempts")? + 1;
            if attempts_exhausted(attempts, get_u32(&row, "max_recovery_attempts")?) {
                sqlx::query(self.sql(
                    "UPDATE workflow_status SET status = 'MAX_RECOVERY_ATTEMPTS_EXCEEDED', recovery_attempts = ?, \
                     lease_token = NULL, claim_after = NULL, completed_at = {NOW}, updated_at = {NOW} WHERE workflow_uuid = ?",
                ))
                .bind(i64::from(attempts))
                .bind(workflow_id.as_str())
                .execute(&mut *tx)
                .await?;
                continue;
            }
            let token = new_token();
            sqlx::query(self.sql(
                "UPDATE workflow_status SET lease_token = ?, recovery_attempts = ?, \
                 claim_after = {NOW} + ?, updated_at = {NOW} WHERE workflow_uuid = ?",
            ))
            .bind(token.as_str())
            .bind(i64::from(attempts))
            .bind(millis(duration))
            .bind(workflow_id.as_str())
            .execute(&mut *tx)
            .await?;
            claimed.push(Claimed {
                lease: Lease { workflow_id, token },
                name: row.try_get("name")?,
                input: row.try_get("inputs")?,
                recovery_attempts: attempts,
                serialization: row.try_get("serialization")?,
            });
        }
        tx.commit().await?;
        Ok(claimed)
    }

    pub async fn list_workflows(&self) -> Result<Vec<Workflow>, Error> {
        self.attempt(|| async {
            let rows = sqlx::query(
                "SELECT workflow_uuid, name, status, inputs, output, error, recovery_attempts, max_recovery_attempts, parent_workflow_id, created_at, updated_at, completed_at, serialization \
                 FROM workflow_status ORDER BY created_at, workflow_uuid",
            )
            .fetch_all(&self.pool)
            .await?;
            rows.iter().map(workflow_from_row).collect()
        })
        .await
    }

    pub async fn list_steps(&self, workflow_id: &str) -> Result<Vec<RecordedStep>, Error> {
        self.attempt(|| async {
            let rows = sqlx::query(self.sql(
                "SELECT function_id, function_name, output, error, child_workflow_id, serialization FROM operation_outputs WHERE workflow_uuid = ? ORDER BY function_id",
            ))
            .bind(workflow_id)
            .fetch_all(&self.pool)
            .await?;
            rows.iter()
                .map(|row| {
                    Ok(RecordedStep {
                        step_id: get_u32(row, "function_id")?,
                        step_name: row.try_get("function_name")?,
                        record: record_from_row(row)?,
                        serialization: row.try_get("serialization")?,
                    })
                })
                .collect()
        })
        .await
    }

    pub fn keep_lease(&self, lease: Lease, duration: Duration, max_run: Duration) -> LeaseKeeper {
        let store = self.clone();
        LeaseKeeper::spawn(duration, max_run, move || {
            let (store, lease) = (store.clone(), lease.clone());
            async move { store.renew_lease(&lease, duration).await }
        })
    }
}
