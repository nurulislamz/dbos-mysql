use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::{ConnectOptions, Connection, Executor, MySql, MySqlConnection, QueryBuilder, Row};

use crate::Error;
use crate::retry::with_retry;
use crate::workflow::{
    Claimed, Lease, NewWorkflow, Outcome, Started, Status, StepRecord, Workflow,
    attempts_exhausted, check_recorded_step, check_step_name, check_workflow_name, new_token,
};

const SCHEMA: &str = include_str!("../schema/mysql/001_init.sql");
const NOW: &str = "UTC_TIMESTAMP(6)";
const POOL_SIZE: u32 = 10;
const LEASE_POOL_SIZE: u32 = 2;
const PACKET_MARGIN: u64 = 4096;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SESSION_SETTINGS: &str = "SET SESSION transaction_isolation = 'READ-COMMITTED', time_zone = '+00:00', \
     innodb_lock_wait_timeout = 10, sql_mode = CONCAT(@@sql_mode, ',STRICT_ALL_TABLES')";
const SELECT_WORKFLOW: &str = "SELECT workflow_id, name, status, input, output, error, recovery_attempts, max_recovery_attempts \
     FROM dbos_workflows WHERE workflow_id = ?";

fn micros(duration: Duration) -> i64 {
    i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
}

async fn configure(conn: &mut MySqlConnection) -> Result<(), sqlx::Error> {
    conn.execute(SESSION_SETTINGS).await?;
    Ok(())
}

fn build_pool(options: MySqlConnectOptions, size: u32) -> MySqlPool {
    MySqlPoolOptions::new()
        .max_connections(size)
        .acquire_timeout(CONNECT_TIMEOUT)
        .after_connect(|conn, _| Box::pin(configure(conn)))
        .connect_lazy_with(options)
}

#[derive(Clone, Debug)]
pub struct MySqlStore {
    pool: MySqlPool,
    lease_pool: MySqlPool,
    max_packet: u64,
    retries: Arc<AtomicU64>,
}

impl MySqlStore {
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let options = MySqlConnectOptions::from_str(url).map_err(|_| Error::InvalidUrl)?;
        let max_packet = tokio::time::timeout(CONNECT_TIMEOUT, Self::probe(&options))
            .await
            .map_err(|_| Error::Timeout(CONNECT_TIMEOUT))??;
        log::info!("connected to mysql");
        Ok(Self {
            pool: build_pool(options.clone(), POOL_SIZE),
            lease_pool: build_pool(options, LEASE_POOL_SIZE),
            max_packet,
            retries: Arc::new(AtomicU64::new(0)),
        })
    }

    async fn probe(options: &MySqlConnectOptions) -> Result<u64, Error> {
        let mut conn = options.connect().await?;
        configure(&mut conn).await?;
        let max_packet: u64 = sqlx::query_scalar("SELECT @@max_allowed_packet")
            .fetch_one(&mut conn)
            .await?;
        conn.close().await?;
        Ok(max_packet)
    }

    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    pub fn transient_retries(&self) -> u64 {
        self.retries.load(Ordering::Relaxed)
    }

    fn check_size(&self, values: &[Option<&str>]) -> Result<(), Error> {
        let size: u64 = values
            .iter()
            .flatten()
            .map(|value| value.len() as u64)
            .sum();
        let limit = self.max_packet.saturating_sub(PACKET_MARGIN);
        if size > limit {
            return Err(Error::ValueTooLarge { size, limit });
        }
        Ok(())
    }

    pub async fn migrate(&self) -> Result<(), Error> {
        sqlx::raw_sql(SCHEMA).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn start_workflow(&self, new: &NewWorkflow<'_>) -> Result<Started, Error> {
        self.check_size(&[Some(new.input)])?;
        let token = new_token();
        with_retry(&self.retries, || self.start_workflow_once(new, &token)).await
    }

    async fn start_workflow_once(
        &self,
        new: &NewWorkflow<'_>,
        token: &str,
    ) -> Result<Started, Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO dbos_workflows (workflow_id, name, status, input, claim_after, lease_token, \
             max_recovery_attempts, created_at, updated_at) \
             VALUES (?, ?, 'PENDING', ?, {NOW} + INTERVAL ? MICROSECOND, ?, ?, {NOW}, {NOW}) \
             ON DUPLICATE KEY UPDATE workflow_id = workflow_id"
        )))
        .bind(new.workflow_id)
        .bind(new.name)
        .bind(new.input)
        .bind(micros(new.lease))
        .bind(token)
        .bind(new.max_recovery_attempts)
        .execute(&mut *tx)
        .await?;

        let row = sqlx::query(
            "SELECT workflow_id, name, status, input, output, error, recovery_attempts, max_recovery_attempts, lease_token \
             FROM dbos_workflows WHERE workflow_id = ?",
        )
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
        with_retry(&self.retries, || async {
            let row = sqlx::query(SELECT_WORKFLOW)
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
        with_retry(&self.retries, || async {
            let mut conn = self.pool.acquire().await?;
            let exists = sqlx::query("SELECT 1 FROM dbos_workflows WHERE workflow_id = ?")
                .bind(workflow_id)
                .fetch_optional(&mut *conn)
                .await?;
            if exists.is_none() {
                return Err(Error::WorkflowNotFound(workflow_id.to_owned()));
            }
            let Some((recorded_name, record)) = read_step(&mut conn, workflow_id, step_id).await?
            else {
                return Ok(None);
            };
            check_step_name(workflow_id, step_id, step_name, recorded_name)?;
            Ok(Some(record))
        })
        .await
    }

    pub async fn save_step(
        &self,
        lease: &Lease,
        step_id: u32,
        step_name: &str,
        record: &StepRecord,
    ) -> Result<(), Error> {
        self.check_size(&[record.output.as_deref(), record.error.as_deref()])?;
        with_retry(&self.retries, || {
            self.save_step_once(lease, step_id, step_name, record)
        })
        .await
    }

    async fn save_step_once(
        &self,
        lease: &Lease,
        step_id: u32,
        step_name: &str,
        record: &StepRecord,
    ) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;
        let owner = sqlx::query("SELECT 1 FROM dbos_workflows WHERE workflow_id = ? AND lease_token = ? AND status = 'PENDING' FOR UPDATE")
            .bind(&lease.workflow_id)
            .bind(&lease.token)
            .fetch_optional(&mut *tx)
            .await?;
        if owner.is_none() {
            return Err(Error::LeaseLost(lease.workflow_id.clone()));
        }
        if let Some(recorded) = read_step(&mut tx, &lease.workflow_id, step_id).await? {
            return check_recorded_step(&lease.workflow_id, step_id, step_name, record, &recorded);
        }
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO dbos_steps (workflow_id, step_id, step_name, output, error, child_workflow_id, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, {NOW})"
        )))
        .bind(&lease.workflow_id)
        .bind(step_id)
        .bind(step_name)
        .bind(&record.output)
        .bind(&record.error)
        .bind(&record.child_workflow_id)
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
        let changed = with_retry(&self.retries, || async {
            let result = sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE dbos_workflows SET status = ?, output = ?, error = ?, lease_token = NULL, claim_after = NULL, \
                 updated_at = {NOW} WHERE workflow_id = ? AND lease_token = ? AND status = 'PENDING'"
            )))
            .bind(status.as_str())
            .bind(output)
            .bind(error)
            .bind(&lease.workflow_id)
            .bind(&lease.token)
            .execute(&self.pool)
            .await?;
            Ok(result.rows_affected())
        })
        .await?;
        require_owner(changed, lease)
    }

    pub async fn renew_lease(&self, lease: &Lease, duration: Duration) -> Result<(), Error> {
        let changed = with_retry(&self.retries, || async {
            let result = sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE dbos_workflows SET claim_after = {NOW} + INTERVAL ? MICROSECOND, updated_at = {NOW} \
                 WHERE workflow_id = ? AND lease_token = ? AND status = 'PENDING'"
            )))
            .bind(micros(duration))
            .bind(&lease.workflow_id)
            .bind(&lease.token)
            .execute(&self.lease_pool)
            .await?;
            Ok(result.rows_affected())
        })
        .await?;
        require_owner(changed, lease)
    }

    pub async fn release_lease(&self, lease: &Lease) -> Result<(), Error> {
        let changed = with_retry(&self.retries, || async {
            let result = sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE dbos_workflows SET lease_token = NULL, claim_after = {NOW}, \
                 recovery_attempts = recovery_attempts - 1, updated_at = {NOW} \
                 WHERE workflow_id = ? AND lease_token = ? AND status = 'PENDING'"
            )))
            .bind(&lease.workflow_id)
            .bind(&lease.token)
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
        with_retry(&self.retries, || self.reclaim_once(names, limit, duration)).await
    }

    async fn reclaim_once(
        &self,
        names: &[String],
        limit: u32,
        duration: Duration,
    ) -> Result<Vec<Claimed>, Error> {
        let mut tx = self.pool.begin().await?;
        let mut select = QueryBuilder::<MySql>::new(format!(
            "SELECT workflow_id, name, input, recovery_attempts, max_recovery_attempts FROM dbos_workflows \
             WHERE claim_after <= {NOW} AND status = 'PENDING' AND name IN ("
        ));
        let mut separated = select.separated(", ");
        for name in names {
            separated.push_bind(name);
        }
        select.push(") ORDER BY claim_after, workflow_id LIMIT ");
        select.push_bind(limit);
        select.push(" FOR UPDATE SKIP LOCKED");
        let rows = select.build().fetch_all(&mut *tx).await?;

        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let workflow_id: String = row.try_get("workflow_id")?;
            let attempts = row.try_get::<u32, _>("recovery_attempts")? + 1;
            if attempts_exhausted(attempts, row.try_get("max_recovery_attempts")?) {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE dbos_workflows SET status = 'MAX_RECOVERY_ATTEMPTS_EXCEEDED', recovery_attempts = ?, \
                     lease_token = NULL, claim_after = NULL, updated_at = {NOW} WHERE workflow_id = ?"
                )))
                .bind(attempts)
                .bind(&workflow_id)
                .execute(&mut *tx)
                .await?;
                continue;
            }
            let token = new_token();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE dbos_workflows SET lease_token = ?, recovery_attempts = ?, \
                 claim_after = {NOW} + INTERVAL ? MICROSECOND, updated_at = {NOW} WHERE workflow_id = ?"
            )))
            .bind(&token)
            .bind(attempts)
            .bind(micros(duration))
            .bind(&workflow_id)
            .execute(&mut *tx)
            .await?;
            claimed.push(Claimed {
                lease: Lease { workflow_id, token },
                name: row.try_get("name")?,
                input: row.try_get("input")?,
                recovery_attempts: attempts,
            });
        }
        tx.commit().await?;
        Ok(claimed)
    }
}

fn workflow_from_row(row: &MySqlRow) -> Result<Workflow, Error> {
    Ok(Workflow {
        workflow_id: row.try_get("workflow_id")?,
        name: row.try_get("name")?,
        status: Status::parse(row.try_get("status")?)?,
        input: row.try_get("input")?,
        output: row.try_get("output")?,
        error: row.try_get("error")?,
        recovery_attempts: row.try_get("recovery_attempts")?,
        max_recovery_attempts: row.try_get("max_recovery_attempts")?,
    })
}

async fn read_step(
    conn: &mut MySqlConnection,
    workflow_id: &str,
    step_id: u32,
) -> Result<Option<(String, StepRecord)>, Error> {
    let row = sqlx::query("SELECT step_name, output, error, child_workflow_id FROM dbos_steps WHERE workflow_id = ? AND step_id = ?")
        .bind(workflow_id)
        .bind(step_id)
        .fetch_optional(conn)
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some((
        row.try_get("step_name")?,
        StepRecord {
            output: row.try_get("output")?,
            error: row.try_get("error")?,
            child_workflow_id: row.try_get("child_workflow_id")?,
        },
    )))
}

fn require_owner(changed: u64, lease: &Lease) -> Result<(), Error> {
    if changed == 0 {
        return Err(Error::LeaseLost(lease.workflow_id.clone()));
    }
    Ok(())
}
