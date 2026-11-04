use std::time::Duration;

use sqlx::sqlite::{SqlitePool, SqliteRow};
use sqlx::{QueryBuilder, Row, Sqlite, SqliteConnection};

use crate::Error;
use crate::workflow::{
    Claimed, Lease, NewWorkflow, Outcome, Started, Status, StepRecord, Workflow,
};

const SCHEMA: &str = include_str!("../schema/sqlite/001_init.sql");
const NOW_MS: &str = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn new_token() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub async fn migrate(pool: &SqlitePool) -> Result<(), Error> {
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::raw_sql(sqlx::AssertSqlSafe(SCHEMA))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

fn workflow_from_row(row: &SqliteRow) -> Result<Workflow, Error> {
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

const SELECT_WORKFLOW: &str = "SELECT workflow_id, name, status, input, output, error, recovery_attempts, max_recovery_attempts \
     FROM dbos_workflows WHERE workflow_id = ?";

pub async fn start_workflow(pool: &SqlitePool, new: &NewWorkflow<'_>) -> Result<Started, Error> {
    let token = new_token();
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    let inserted = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO dbos_workflows (workflow_id, name, status, input, claim_after_ms, lease_token, \
         max_recovery_attempts, created_at_ms, updated_at_ms) \
         VALUES (?, ?, 'PENDING', ?, {NOW_MS} + ?, ?, ?, {NOW_MS}, {NOW_MS}) \
         ON CONFLICT (workflow_id) DO NOTHING"
    )))
    .bind(new.workflow_id)
    .bind(new.name)
    .bind(new.input)
    .bind(millis(new.lease))
    .bind(&token)
    .bind(new.max_recovery_attempts)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if inserted == 1 {
        tx.commit().await?;
        return Ok(Started::Owned(Lease {
            workflow_id: new.workflow_id.to_owned(),
            token,
        }));
    }

    let row = sqlx::query(SELECT_WORKFLOW)
        .bind(new.workflow_id)
        .fetch_one(&mut *tx)
        .await?;
    let existing = workflow_from_row(&row)?;
    if existing.name != new.name {
        return Err(Error::ConflictingWorkflow {
            workflow_id: new.workflow_id.to_owned(),
            recorded_name: existing.name,
            requested_name: new.name.to_owned(),
        });
    }
    tx.commit().await?;
    Ok(Started::Existing(existing))
}

pub async fn get_workflow(pool: &SqlitePool, workflow_id: &str) -> Result<Option<Workflow>, Error> {
    let row = sqlx::query(SELECT_WORKFLOW)
        .bind(workflow_id)
        .fetch_optional(pool)
        .await?;
    row.as_ref().map(workflow_from_row).transpose()
}

pub async fn load_step(
    pool: &SqlitePool,
    workflow_id: &str,
    step_id: u32,
    step_name: &str,
) -> Result<Option<StepRecord>, Error> {
    let mut conn = pool.acquire().await?;
    let exists = sqlx::query("SELECT 1 FROM dbos_workflows WHERE workflow_id = ?")
        .bind(workflow_id)
        .fetch_optional(&mut *conn)
        .await?;
    if exists.is_none() {
        return Err(Error::WorkflowNotFound(workflow_id.to_owned()));
    }
    let Some((recorded_name, record)) = read_step(&mut conn, workflow_id, step_id).await? else {
        return Ok(None);
    };
    if recorded_name != step_name {
        return Err(Error::UnexpectedStep {
            workflow_id: workflow_id.to_owned(),
            step_id,
            expected_name: step_name.to_owned(),
            recorded_name,
        });
    }
    Ok(Some(record))
}

async fn read_step(
    conn: &mut SqliteConnection,
    workflow_id: &str,
    step_id: u32,
) -> Result<Option<(String, StepRecord)>, Error> {
    let row = sqlx::query(
        "SELECT step_name, output, error, child_workflow_id FROM dbos_steps WHERE workflow_id = ? AND step_id = ?",
    )
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

pub async fn save_step(
    pool: &SqlitePool,
    lease: &Lease,
    step_id: u32,
    step_name: &str,
    record: &StepRecord,
) -> Result<(), Error> {
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    let owner = sqlx::query("SELECT 1 FROM dbos_workflows WHERE workflow_id = ? AND lease_token = ? AND status = 'PENDING'")
        .bind(&lease.workflow_id)
        .bind(&lease.token)
        .fetch_optional(&mut *tx)
        .await?;
    if owner.is_none() {
        return Err(Error::LeaseLost(lease.workflow_id.clone()));
    }

    if let Some((recorded_name, recorded)) = read_step(&mut tx, &lease.workflow_id, step_id).await?
    {
        if recorded_name == step_name && &recorded == record {
            return Ok(());
        }
        return Err(Error::StepNondeterminism {
            workflow_id: lease.workflow_id.clone(),
            step_id,
        });
    }

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO dbos_steps (workflow_id, step_id, step_name, output, error, child_workflow_id, created_at_ms) \
         VALUES (?, ?, ?, ?, ?, ?, {NOW_MS})"
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

pub async fn finish_workflow(
    pool: &SqlitePool,
    lease: &Lease,
    outcome: &Outcome,
) -> Result<(), Error> {
    let (status, output, error) = match outcome {
        Outcome::Success(value) => (Status::Success, Some(value), None),
        Outcome::Error(value) => (Status::Error, None, Some(value)),
    };
    let changed = sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE dbos_workflows SET status = ?, output = ?, error = ?, lease_token = NULL, claim_after_ms = NULL, \
         updated_at_ms = {NOW_MS} WHERE workflow_id = ? AND lease_token = ? AND status = 'PENDING'"
    )))
    .bind(status.as_str())
    .bind(output)
    .bind(error)
    .bind(&lease.workflow_id)
    .bind(&lease.token)
    .execute(pool)
    .await?
    .rows_affected();
    require_owner(changed, lease)
}

pub async fn renew_lease(
    pool: &SqlitePool,
    lease: &Lease,
    duration: Duration,
) -> Result<(), Error> {
    let changed = sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE dbos_workflows SET claim_after_ms = {NOW_MS} + ?, updated_at_ms = {NOW_MS} \
         WHERE workflow_id = ? AND lease_token = ? AND status = 'PENDING'"
    )))
    .bind(millis(duration))
    .bind(&lease.workflow_id)
    .bind(&lease.token)
    .execute(pool)
    .await?
    .rows_affected();
    require_owner(changed, lease)
}

pub async fn release_lease(pool: &SqlitePool, lease: &Lease) -> Result<(), Error> {
    let changed = sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE dbos_workflows SET lease_token = NULL, claim_after_ms = {NOW_MS}, \
         recovery_attempts = recovery_attempts - 1, updated_at_ms = {NOW_MS} \
         WHERE workflow_id = ? AND lease_token = ? AND status = 'PENDING'"
    )))
    .bind(&lease.workflow_id)
    .bind(&lease.token)
    .execute(pool)
    .await?
    .rows_affected();
    require_owner(changed, lease)
}

fn require_owner(changed: u64, lease: &Lease) -> Result<(), Error> {
    if changed == 0 {
        return Err(Error::LeaseLost(lease.workflow_id.clone()));
    }
    Ok(())
}

pub async fn reclaim(
    pool: &SqlitePool,
    names: &[String],
    limit: u32,
    duration: Duration,
) -> Result<Vec<Claimed>, Error> {
    if names.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;

    let mut select = QueryBuilder::<Sqlite>::new(format!(
        "SELECT workflow_id, name, input, recovery_attempts, max_recovery_attempts FROM dbos_workflows \
         WHERE status = 'PENDING' AND claim_after_ms <= {NOW_MS} AND name IN ("
    ));
    let mut separated = select.separated(", ");
    for name in names {
        separated.push_bind(name);
    }
    select.push(") ORDER BY claim_after_ms, workflow_id LIMIT ");
    select.push_bind(limit);
    let rows = select.build().fetch_all(&mut *tx).await?;

    let mut claimed = Vec::with_capacity(rows.len());
    for row in rows {
        let workflow_id: String = row.try_get("workflow_id")?;
        let attempts: u32 = row.try_get::<u32, _>("recovery_attempts")? + 1;
        let max_attempts: u32 = row.try_get("max_recovery_attempts")?;

        if attempts > max_attempts.saturating_add(1) {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE dbos_workflows SET status = 'MAX_RECOVERY_ATTEMPTS_EXCEEDED', recovery_attempts = ?, \
                 lease_token = NULL, claim_after_ms = NULL, updated_at_ms = {NOW_MS} WHERE workflow_id = ?"
            )))
            .bind(attempts)
            .bind(&workflow_id)
            .execute(&mut *tx)
            .await?;
            continue;
        }

        let token = new_token();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE dbos_workflows SET lease_token = ?, recovery_attempts = ?, claim_after_ms = {NOW_MS} + ?, \
             updated_at_ms = {NOW_MS} WHERE workflow_id = ?"
        )))
        .bind(&token)
        .bind(attempts)
        .bind(millis(duration))
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
