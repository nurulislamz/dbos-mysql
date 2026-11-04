use std::time::Duration;

use sqlx::sqlite::{SqlitePool, SqliteRow};
use sqlx::{QueryBuilder, Row, Sqlite, SqliteConnection};

use crate::Error;
use crate::workflow::{
    Claimed, Lease, NewWorkflow, Outcome, RecordedStep, SERIALIZATION, Started, Status, StepRecord,
    Workflow, attempts_exhausted, check_recorded_step, check_step_name, check_workflow_name,
    new_token,
};

const SCHEMA: &str = include_str!("../schema/sqlite/001_init.sql");
const NOW_MS: &str = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
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
        workflow_id: row.try_get("workflow_uuid")?,
        name: row.try_get("name")?,
        status: Status::parse(row.try_get("status")?)?,
        input: row.try_get("inputs")?,
        output: row.try_get("output")?,
        error: row.try_get("error")?,
        recovery_attempts: row.try_get("recovery_attempts")?,
        max_recovery_attempts: row.try_get("max_recovery_attempts")?,
        parent_workflow_id: row.try_get("parent_workflow_id")?,
        created_at_ms: row.try_get("created_at")?,
        updated_at_ms: row.try_get("updated_at")?,
        completed_at_ms: row.try_get("completed_at")?,
    })
}

const SELECT_WORKFLOW: &str = "SELECT workflow_uuid, name, status, inputs, output, error, recovery_attempts, max_recovery_attempts, parent_workflow_id, created_at, updated_at, completed_at \
     FROM workflow_status WHERE workflow_uuid = ?";

pub async fn start_workflow(pool: &SqlitePool, new: &NewWorkflow<'_>) -> Result<Started, Error> {
    let token = new_token();
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    let inserted = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO workflow_status (workflow_uuid, name, status, inputs, claim_after, lease_token, max_recovery_attempts, parent_workflow_id, serialization, created_at, updated_at) \
         VALUES (?, ?, 'PENDING', ?, {NOW_MS} + ?, ?, ?, ?, ?, {NOW_MS}, {NOW_MS}) \
         ON CONFLICT (workflow_uuid) DO NOTHING"
    )))
    .bind(new.workflow_id)
    .bind(new.name)
    .bind(new.input)
    .bind(millis(new.lease))
    .bind(&token)
    .bind(new.max_recovery_attempts)
    .bind(new.parent_workflow_id)
        .bind(SERIALIZATION)
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
    check_workflow_name(new, &existing.name)?;
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
    let exists = sqlx::query("SELECT 1 FROM workflow_status WHERE workflow_uuid = ?")
        .bind(workflow_id)
        .fetch_optional(&mut *conn)
        .await?;
    if exists.is_none() {
        return Err(Error::WorkflowNotFound(workflow_id.to_owned()));
    }
    let Some((recorded_name, record)) = read_step(&mut conn, workflow_id, step_id).await? else {
        return Ok(None);
    };
    check_step_name(workflow_id, step_id, step_name, recorded_name)?;
    Ok(Some(record))
}

async fn read_step(
    conn: &mut SqliteConnection,
    workflow_id: &str,
    step_id: u32,
) -> Result<Option<(String, StepRecord)>, Error> {
    let row = sqlx::query(
        "SELECT function_name, output, error, child_workflow_id FROM operation_outputs WHERE workflow_uuid = ? AND function_id = ?",
    )
    .bind(workflow_id)
    .bind(step_id)
    .fetch_optional(conn)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some((
        row.try_get("function_name")?,
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
    let owner = sqlx::query("SELECT 1 FROM workflow_status WHERE workflow_uuid = ? AND lease_token = ? AND status = 'PENDING'")
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
        "INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, output, error, child_workflow_id, serialization, completed_at_epoch_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?, {NOW_MS})"
    )))
    .bind(&lease.workflow_id)
    .bind(step_id)
    .bind(step_name)
    .bind(&record.output)
    .bind(&record.error)
    .bind(&record.child_workflow_id)
        .bind(SERIALIZATION)
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
        "UPDATE workflow_status SET status = ?, output = ?, error = ?, lease_token = NULL, claim_after = NULL, completed_at = {NOW_MS}, updated_at = {NOW_MS} WHERE workflow_uuid = ? AND lease_token = ? AND status = 'PENDING'"
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
        "UPDATE workflow_status SET claim_after = {NOW_MS} + ?, updated_at = {NOW_MS} \
         WHERE workflow_uuid = ? AND lease_token = ? AND status = 'PENDING'"
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
        "UPDATE workflow_status SET lease_token = NULL, claim_after = {NOW_MS}, \
         recovery_attempts = recovery_attempts - 1, updated_at = {NOW_MS} \
         WHERE workflow_uuid = ? AND lease_token = ? AND status = 'PENDING'"
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
        "SELECT workflow_uuid, name, inputs, recovery_attempts, max_recovery_attempts FROM workflow_status \
         WHERE status = 'PENDING' AND claim_after <= {NOW_MS} AND name IN ("
    ));
    let mut separated = select.separated(", ");
    for name in names {
        separated.push_bind(name);
    }
    select.push(") ORDER BY claim_after, workflow_uuid LIMIT ");
    select.push_bind(limit);
    let rows = select.build().fetch_all(&mut *tx).await?;

    let mut claimed = Vec::with_capacity(rows.len());
    for row in rows {
        let workflow_id: String = row.try_get("workflow_uuid")?;
        let attempts: u32 = row.try_get::<u32, _>("recovery_attempts")? + 1;

        if attempts_exhausted(attempts, row.try_get("max_recovery_attempts")?) {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE workflow_status SET status = 'MAX_RECOVERY_ATTEMPTS_EXCEEDED', recovery_attempts = ?, \
                 lease_token = NULL, claim_after = NULL, completed_at = {NOW_MS}, updated_at = {NOW_MS} WHERE workflow_uuid = ?"
            )))
            .bind(attempts)
            .bind(&workflow_id)
            .execute(&mut *tx)
            .await?;
            continue;
        }

        let token = new_token();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE workflow_status SET lease_token = ?, recovery_attempts = ?, claim_after = {NOW_MS} + ?, \
             updated_at = {NOW_MS} WHERE workflow_uuid = ?"
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
            input: row.try_get("inputs")?,
            recovery_attempts: attempts,
        });
    }
    tx.commit().await?;
    Ok(claimed)
}

pub async fn list_workflows(pool: &SqlitePool) -> Result<Vec<Workflow>, Error> {
    let rows = sqlx::query(
        "SELECT workflow_uuid, name, status, inputs, output, error, recovery_attempts, max_recovery_attempts, parent_workflow_id, created_at, updated_at, completed_at \
         FROM workflow_status ORDER BY created_at, workflow_uuid",
    )
    .fetch_all(pool)
    .await?;
    rows.iter().map(workflow_from_row).collect()
}

pub async fn list_steps(pool: &SqlitePool, workflow_id: &str) -> Result<Vec<RecordedStep>, Error> {
    let rows = sqlx::query(
        "SELECT function_id, function_name, output, error, child_workflow_id FROM operation_outputs WHERE workflow_uuid = ? ORDER BY function_id",
    )
    .bind(workflow_id)
    .fetch_all(pool)
    .await?;
    rows.iter()
        .map(|row| {
            Ok(RecordedStep {
                step_id: row.try_get("function_id")?,
                step_name: row.try_get("function_name")?,
                record: StepRecord {
                    output: row.try_get("output")?,
                    error: row.try_get("error")?,
                    child_workflow_id: row.try_get("child_workflow_id")?,
                },
            })
        })
        .collect()
}
