use sqlx::Row;

use super::{Store, check_name};
use crate::Error;
use crate::workflow::{
    Lease, NewWorkflow, ReusePolicy, WorkflowOptions, check_step_name, check_workflow_name,
    id_in_use, new_token,
};

/// The step name a debounce called inside a workflow is saved under.
pub const DEBOUNCE_STEP: &str = "DBOS.debounce";

#[derive(Clone, Copy, Debug)]
pub struct Debounce<'a> {
    /// The queue deduplication ID the debounce holds while its workflow waits.
    pub key: &'a str,
    pub period_ms: i64,
    /// Measured from the first call: later calls never delay the workflow past it.
    pub timeout_ms: Option<i64>,
}

impl Store {
    /// Coalesces this call into the workflow still waiting under `debounce.key`, giving it these
    /// inputs and a later start, or enqueues a new delayed workflow. Returns the workflow ID.
    /// With `step`, the result is saved as that step of the calling workflow in the same commit,
    /// so a replay returns the same ID and never debounces twice.
    pub async fn debounce_workflow(
        &self,
        queue: &str,
        new: &NewWorkflow<'_>,
        options: &WorkflowOptions,
        debounce: &Debounce<'_>,
        step: Option<(&Lease, u32)>,
    ) -> Result<String, Error> {
        check_name("queue name", queue)?;
        check_name("workflow ID", new.workflow_id)?;
        check_name("workflow name", new.name)?;
        check_name("deduplication ID", debounce.key)?;
        if debounce.period_ms < 0 {
            return Err(Error::InvalidConfig(
                "debounce period must be non-negative".to_owned(),
            ));
        }
        if debounce.timeout_ms.is_some_and(|timeout| timeout <= 0) {
            return Err(Error::InvalidConfig(
                "debounce timeout must be positive".to_owned(),
            ));
        }
        if new.timeout_ms.is_some_and(|timeout| timeout <= 0) {
            return Err(Error::InvalidConfig(
                "workflow timeout must be positive".to_owned(),
            ));
        }
        self.check_size(&[Some(new.input), options.attributes.as_deref()])?;
        super::workflows::validate_start_attributes(options.attributes.as_deref())?;
        let token = new_token();
        self.attempt(|| self.debounce_once(queue, new, options, debounce, step, &token))
            .await
    }

    async fn debounce_once(
        &self,
        queue: &str,
        new: &NewWorkflow<'_>,
        options: &WorkflowOptions,
        debounce: &Debounce<'_>,
        step: Option<(&Lease, u32)>,
        token: &str,
    ) -> Result<String, Error> {
        let (mut tx, step_serialization) = match step {
            Some((lease, step_id)) => {
                let (mut tx, serialization) = self.owned_transaction(lease).await?;
                if let Some((recorded, record)) =
                    self.read_step(&mut tx, &lease.workflow_id, step_id).await?
                {
                    check_step_name(&lease.workflow_id, step_id, DEBOUNCE_STEP, recorded)?;
                    return record
                        .child_workflow_id
                        .ok_or_else(|| Error::StepKindMismatch {
                            workflow_id: lease.workflow_id.clone(),
                            step_id,
                            expected: "debounce",
                        });
                }
                (tx, Some(serialization))
            }
            None => (self.begin().await?, None),
        };
        // Holding the queue row serializes this with enqueues and claims, so no other call can take the key meanwhile.
        let partitioned: Option<i64> = sqlx::query_scalar(self.sql(&format!(
            "SELECT CASE WHEN partition_concurrency IS NOT NULL OR partition_worker_concurrency IS NOT NULL OR partition_rate_limit IS NOT NULL THEN 1 ELSE 0 END FROM workflow_queues WHERE name = ? AND deleted = 0 {}",
            self.dialect.lock_row(),
        )))
        .bind(queue)
        .fetch_optional(&mut *tx)
        .await?;
        match partitioned {
            None => return Err(Error::QueueNotFound(queue.to_owned())),
            Some(0) => {}
            Some(_) => {
                return Err(Error::InvalidConfig(
                    "partitioned queues do not support deduplication, which debouncing requires"
                        .to_owned(),
                ));
            }
        }
        let now: i64 = sqlx::query_scalar(self.sql("SELECT {NOW}"))
            .fetch_one(&mut *tx)
            .await?;
        let until = now
            .checked_add(debounce.period_ms)
            .ok_or_else(|| Error::InvalidConfig("debounce period is too large".to_owned()))?;
        // A debounced workflow holds its key only while it waits. Once released it keeps running, and the key is free again.
        sqlx::query(self.sql(
            "UPDATE workflow_queue_entries SET deduplication_id = NULL \
             WHERE queue_name = ? AND deduplication_id = ? AND debounced = 1 AND NOT EXISTS ( \
             SELECT 1 FROM workflow_status WHERE workflow_status.workflow_uuid = workflow_queue_entries.workflow_uuid \
             AND workflow_status.status = 'PENDING' AND workflow_status.lease_token IS NULL \
             AND workflow_status.started_at_epoch_ms IS NULL AND workflow_status.claim_after > ?)",
        ))
        .bind(queue)
        .bind(debounce.key)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let holder = sqlx::query(self.sql(
            "SELECT workflow_queue_entries.workflow_uuid AS workflow_uuid, workflow_queue_entries.debounced AS debounced, \
             workflow_queue_entries.debounce_deadline_ms AS deadline, workflow_status.name AS name, \
             workflow_status.application_name AS application_name \
             FROM workflow_queue_entries JOIN workflow_status ON workflow_status.workflow_uuid = workflow_queue_entries.workflow_uuid \
             WHERE workflow_queue_entries.queue_name = ? AND workflow_queue_entries.deduplication_id = ? \
             AND workflow_status.status IN ('PENDING', 'ENQUEUED', 'DELAYED') LIMIT 1",
        ))
        .bind(queue)
        .bind(debounce.key)
        .fetch_optional(&mut *tx)
        .await?;
        let target_app = options
            .application_name
            .as_deref()
            .or(self.identity.application_name.as_deref());
        let workflow_id = if let Some(holder) = holder {
            let holder_id: String = holder.try_get("workflow_uuid")?;
            let debounced: i64 = holder.try_get("debounced")?;
            let name: String = holder.try_get("name")?;
            let app: Option<String> = holder.try_get("application_name")?;
            let foreign = matches!((&app, target_app), (Some(app), Some(target)) if app != target);
            // A user's own deduplicated workflow, another workflow's colliding key, or another application's holder is a conflict.
            if debounced == 0 || name != new.name || foreign {
                return Err(Error::QueueDeduplicated {
                    queue: queue.to_owned(),
                    deduplication_id: debounce.key.to_owned(),
                });
            }
            let deadline: Option<i64> = holder.try_get("deadline")?;
            let start = deadline.map_or(until, |deadline| until.min(deadline));
            sqlx::query(self.sql(
                "UPDATE workflow_status SET inputs = ?, serialization = ?, claim_after = ?, \
                 application_name = COALESCE(application_name, ?), updated_at = {NOW} WHERE workflow_uuid = ?",
            ))
            .bind(new.input)
            .bind(new.serialization)
            .bind(start)
            .bind(target_app)
            .bind(holder_id.as_str())
            .execute(&mut *tx)
            .await?;
            sqlx::query(self.sql(
                "UPDATE workflow_queue_entries SET delay_until_ms = ? WHERE workflow_uuid = ?",
            ))
            .bind(start)
            .bind(holder_id.as_str())
            .execute(&mut *tx)
            .await?;
            holder_id
        } else {
            let deadline = debounce
                .timeout_ms
                .map(|timeout| now.checked_add(timeout))
                .map(|deadline| {
                    deadline.ok_or_else(|| {
                        Error::InvalidConfig("debounce timeout is too large".to_owned())
                    })
                })
                .transpose()?;
            let start = deadline.map_or(until, |deadline| until.min(deadline));
            if new
                .timeout_ms
                .is_some_and(|timeout| now.checked_add(timeout).is_none())
            {
                return Err(Error::InvalidConfig(
                    "workflow timeout is too large".to_owned(),
                ));
            }
            // The temporary token distinguishes insertion from an existing row on every backend.
            let insert = sqlx::query(self.sql(&format!(
                "INSERT INTO workflow_status (workflow_uuid, name, status, inputs, claim_after, lease_token, recovery_attempts, max_recovery_attempts, parent_workflow_id, serialization, created_at, updated_at, {}) \
                 VALUES (?, ?, 'PENDING', ?, ?, ?, 0, ?, ?, ?, {{NOW}}, {{NOW}}, {}) {}",
                super::workflows::START_COLUMNS,
                super::workflows::START_VALUES,
                self.dialect.insert_if_absent(),
            )))
            .bind(new.workflow_id)
            .bind(new.name)
            .bind(new.input)
            .bind(start)
            .bind(token)
            .bind(i64::from(new.max_recovery_attempts))
            .bind(new.parent_workflow_id)
            .bind(new.serialization);
            self.bind_start_columns(insert, options)
                .execute(&mut *tx)
                .await?;
            let row = sqlx::query(self.sql(super::SELECT_WORKFLOW))
                .bind(new.workflow_id)
                .fetch_one(&mut *tx)
                .await?;
            let owner: Option<String> = row.try_get("lease_token")?;
            if owner.as_deref() == Some(token) {
                sqlx::query(self.sql(
                    "INSERT INTO workflow_queue_entries (workflow_uuid, queue_name, start_timeout_ms, deduplication_id, priority, delay_until_ms, debounced, debounce_deadline_ms) VALUES (?, ?, ?, ?, 0, ?, 1, ?)",
                ))
                .bind(new.workflow_id)
                .bind(queue)
                .bind(new.timeout_ms)
                .bind(debounce.key)
                .bind(start)
                .bind(deadline)
                .execute(&mut *tx)
                .await?;
                if let Some(deadline) = new.deadline_epoch_ms {
                    sqlx::query(self.sql(
                        "INSERT INTO workflow_timeouts (workflow_uuid, timeout_ms, deadline_epoch_ms) VALUES (?, NULL, ?)",
                    ))
                    .bind(new.workflow_id)
                    .bind(deadline)
                    .execute(&mut *tx)
                    .await?;
                }
                sqlx::query(self.sql(
                    "UPDATE workflow_status SET lease_token = NULL WHERE workflow_uuid = ? AND lease_token = ?",
                ))
                .bind(new.workflow_id)
                .bind(token)
                .execute(&mut *tx)
                .await?;
            } else {
                // A pinned ID that already has a row returns that workflow, as an enqueue does.
                let existing = super::workflow_from_row(&row)?;
                if options.reuse == ReusePolicy::Reject {
                    return Err(id_in_use(&existing));
                }
                check_workflow_name(new, &existing.name)?;
            }
            new.workflow_id.to_owned()
        };
        if let (Some((lease, step_id)), Some(serialization)) = (step, step_serialization) {
            sqlx::query(self.sql(
                "INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, output, error, child_workflow_id, serialization, completed_at_epoch_ms) \
                 VALUES (?, ?, ?, NULL, NULL, ?, ?, {NOW})",
            ))
            .bind(lease.workflow_id.as_str())
            .bind(i64::from(step_id))
            .bind(DEBOUNCE_STEP)
            .bind(workflow_id.as_str())
            .bind(serialization.as_str())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(workflow_id)
    }
}
