use std::collections::HashMap;

use sqlx::{Any, Row, Transaction};

use super::timeouts::RECHECK_CHUNK;
use super::workflow_management::in_clause;
use super::{SELECT_WORKFLOW, Store, check_name, workflow_from_row};
use crate::Error;
use crate::workflow::id_in_use;

const TERMINAL: [&str; 4] = [
    "SUCCESS",
    "ERROR",
    "CANCELLED",
    "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
];
const COPIED_COLUMNS: &str = "application_name, class_name, config_name, authenticated_user, assumed_role, authenticated_roles, attributes";

/// Options for `fork_workflow`, as in DBOS.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ForkOptions {
    /// `None` keeps the original's version.
    pub application_version: Option<String>,
    /// `None` runs the fork here, at once.
    pub queue_name: Option<String>,
    pub queue_partition_key: Option<String>,
    /// Starts when the fork starts: on admission when it is queued.
    pub timeout_ms: Option<i64>,
    /// Original child workflow ID to the ID the fork's replay attaches to instead.
    pub replacement_children: HashMap<String, String>,
}

fn check_queue(queue_name: Option<&str>, partition_key: Option<&str>) -> Result<(), Error> {
    if let Some(queue) = queue_name {
        check_name("queue name", queue)?;
    }
    if let Some(key) = partition_key {
        if queue_name.is_none() {
            return Err(Error::InvalidConfig(
                "queue_partition_key requires queue_name".to_owned(),
            ));
        }
        check_name("queue partition key", key)?;
    }
    Ok(())
}

impl Store {
    /// Starts `new_id` as a copy of `original` with the steps before `start_step` saved, so they
    /// replay and the rest runs. Copies the events those steps set, and marks `original` as forked
    /// from. A fork that already exists under `new_id` is kept, so a retried call does not fail.
    pub async fn fork_workflow(
        &self,
        original: &str,
        new_id: &str,
        start_step: u32,
        options: &ForkOptions,
    ) -> Result<(), Error> {
        check_name("workflow ID", original)?;
        check_name("workflow ID", new_id)?;
        check_queue(
            options.queue_name.as_deref(),
            options.queue_partition_key.as_deref(),
        )?;
        if let Some(version) = &options.application_version {
            check_name("application version", version)?;
        }
        if options.timeout_ms.is_some_and(|timeout| timeout <= 0) {
            return Err(Error::InvalidConfig(
                "workflow timeout must be positive".to_owned(),
            ));
        }
        for (from, to) in &options.replacement_children {
            check_name("workflow ID", from)?;
            check_name("workflow ID", to)?;
        }
        self.attempt(|| self.fork_once(original, new_id, start_step, options))
            .await
    }

    async fn fork_once(
        &self,
        original: &str,
        new_id: &str,
        start_step: u32,
        options: &ForkOptions,
    ) -> Result<(), Error> {
        let mut tx = self.begin().await?;
        let source = sqlx::query(self.sql(&format!(
            "SELECT name, inputs, max_recovery_attempts, serialization, application_version FROM workflow_status \
             WHERE workflow_uuid = ? {}",
            self.dialect.lock_row(),
        )))
        .bind(original)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| Error::WorkflowNotFound(original.to_owned()))?;
        if let Some(row) = sqlx::query(self.sql(SELECT_WORKFLOW))
            .bind(new_id)
            .fetch_optional(&mut *tx)
            .await?
        {
            let existing = workflow_from_row(&row)?;
            if existing.forked_from.as_deref() == Some(original) {
                tx.commit().await?;
                return Ok(());
            }
            return Err(id_in_use(&existing));
        }
        if let Some(queue) = &options.queue_name {
            self.check_queue_entry(&mut tx, queue, options.queue_partition_key.as_deref())
                .await?;
        }
        let version: Option<String> = source.try_get("application_version")?;
        sqlx::query(self.sql(&format!(
            "INSERT INTO workflow_status (workflow_uuid, name, status, inputs, claim_after, recovery_attempts, \
             max_recovery_attempts, serialization, forked_from, application_version, step_base, {COPIED_COLUMNS}, created_at, updated_at) \
             SELECT ?, name, 'PENDING', inputs, {{NOW}}, 0, max_recovery_attempts, serialization, workflow_uuid, ?, step_base, \
             {COPIED_COLUMNS}, {{NOW}}, {{NOW}} FROM workflow_status WHERE workflow_uuid = ?"
        )))
        .bind(new_id)
        .bind(options.application_version.as_deref().or(version.as_deref()))
        .bind(original)
        .execute(&mut *tx)
        .await?;
        self.index_attributes(&mut tx, new_id).await?;
        sqlx::query(self.sql(
            "INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, output, error, child_workflow_id, \
             serialization, completed_at_epoch_ms, started_at_epoch_ms) \
             SELECT ?, function_id, function_name, output, error, child_workflow_id, serialization, completed_at_epoch_ms, \
             started_at_epoch_ms FROM operation_outputs WHERE workflow_uuid = ? AND function_id < ?",
        ))
        .bind(new_id)
        .bind(original)
        .bind(i64::from(start_step))
        .execute(&mut *tx)
        .await?;
        for (from, to) in &options.replacement_children {
            sqlx::query(self.sql(
                "UPDATE operation_outputs SET child_workflow_id = ? WHERE workflow_uuid = ? AND child_workflow_id = ?",
            ))
            .bind(to.as_str())
            .bind(new_id)
            .bind(from.as_str())
            .execute(&mut *tx)
            .await?;
        }
        if let Some(cutoff) = self.kept_until(&mut tx, original, start_step).await? {
            sqlx::query(self.sql(
                "INSERT INTO workflow_event_history (workflow_uuid, event_key, revision, created_at_epoch_ms, value, serialization) \
                 SELECT ?, event_key, revision, created_at_epoch_ms, value, serialization FROM workflow_event_history \
                 WHERE workflow_uuid = ? AND created_at_epoch_ms <= ?",
            ))
            .bind(new_id)
            .bind(original)
            .bind(cutoff)
            .execute(&mut *tx)
            .await?;
            self.rebuild_events(&mut tx, new_id).await?;
        }
        match &options.queue_name {
            Some(queue) => {
                sqlx::query(self.sql(
                    "INSERT INTO workflow_queue_entries (workflow_uuid, queue_name, start_timeout_ms, partition_key) VALUES (?, ?, ?, ?)",
                ))
                .bind(new_id)
                .bind(queue.as_str())
                .bind(options.timeout_ms)
                .bind(options.queue_partition_key.as_deref())
                .execute(&mut *tx)
                .await?;
            }
            None => {
                if let Some(timeout) = options.timeout_ms {
                    sqlx::query(self.sql(
                        "INSERT INTO workflow_timeouts (workflow_uuid, timeout_ms, deadline_epoch_ms) VALUES (?, ?, {NOW} + ?)",
                    ))
                    .bind(new_id)
                    .bind(timeout)
                    .bind(timeout)
                    .execute(&mut *tx)
                    .await?;
                }
            }
        }
        sqlx::query(
            self.sql("UPDATE workflow_status SET was_forked_from = 1 WHERE workflow_uuid = ?"),
        )
        .bind(original)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Discards the steps from `start_step` on, the events they set, the messages they consumed and
    /// every unconsumed message, then makes the workflow claimable again under its own ID. Only a
    /// workflow in a final state can be rewound. Children and their rows are left alone.
    pub async fn rewind_workflow(
        &self,
        workflow_id: &str,
        start_step: u32,
        application_version: Option<&str>,
        queue_name: Option<&str>,
        queue_partition_key: Option<&str>,
    ) -> Result<(), Error> {
        check_name("workflow ID", workflow_id)?;
        check_queue(queue_name, queue_partition_key)?;
        if let Some(version) = application_version {
            check_name("application version", version)?;
        }
        self.attempt(|| {
            self.rewind_once(
                workflow_id,
                start_step,
                application_version,
                queue_name,
                queue_partition_key,
            )
        })
        .await
    }

    async fn rewind_once(
        &self,
        workflow_id: &str,
        start_step: u32,
        application_version: Option<&str>,
        queue_name: Option<&str>,
        queue_partition_key: Option<&str>,
    ) -> Result<(), Error> {
        let mut tx = self.begin().await?;
        let status: String = sqlx::query_scalar(self.sql(&format!(
            "SELECT status FROM workflow_status WHERE workflow_uuid = ? {}",
            self.dialect.lock_row(),
        )))
        .bind(workflow_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| Error::WorkflowNotFound(workflow_id.to_owned()))?;
        if !TERMINAL.contains(&status.as_str()) {
            return Err(Error::InvalidConfig(format!(
                "workflow {workflow_id} is {status}. Only a workflow in a terminal state can be rewound, so cancel it first"
            )));
        }
        if let Some(queue) = queue_name {
            self.check_queue_entry(&mut tx, queue, queue_partition_key)
                .await?;
        }
        let step = i64::from(start_step);
        let cutoff = self.kept_until(&mut tx, workflow_id, start_step).await?;
        sqlx::query(
            self.sql("DELETE FROM operation_outputs WHERE workflow_uuid = ? AND function_id >= ?"),
        )
        .bind(workflow_id)
        .bind(step)
        .execute(&mut *tx)
        .await?;
        sqlx::query(self.sql(
            "DELETE FROM workflow_event_history WHERE workflow_uuid = ? AND created_at_epoch_ms > ?",
        ))
        .bind(workflow_id)
        .bind(cutoff.unwrap_or(i64::MIN))
        .execute(&mut *tx)
        .await?;
        self.rebuild_events(&mut tx, workflow_id).await?;
        sqlx::query(self.sql(
            "DELETE FROM notifications WHERE destination_uuid = ? AND (consumed = 0 OR consumed_by_function_id >= ?)",
        ))
        .bind(workflow_id)
        .bind(step)
        .execute(&mut *tx)
        .await?;
        for table in ["workflow_timeouts", "workflow_queue_entries"] {
            sqlx::query(self.sql(&format!("DELETE FROM {table} WHERE workflow_uuid = ?")))
                .bind(workflow_id)
                .execute(&mut *tx)
                .await?;
        }
        if let Some(queue) = queue_name {
            sqlx::query(self.sql(
                "INSERT INTO workflow_queue_entries (workflow_uuid, queue_name, start_timeout_ms, partition_key) VALUES (?, ?, NULL, ?)",
            ))
            .bind(workflow_id)
            .bind(queue)
            .bind(queue_partition_key)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(self.sql(
            "UPDATE workflow_status SET status = 'PENDING', output = NULL, error = NULL, lease_token = NULL, \
             claim_after = {NOW}, recovery_attempts = 0, completed_at = NULL, started_at_epoch_ms = NULL, \
             application_version = COALESCE(?, application_version), updated_at = {NOW} WHERE workflow_uuid = ?",
        ))
        .bind(application_version)
        .bind(workflow_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Every workflow forked from `workflow_id`, and their forks in turn, as they are now.
    pub async fn fork_descendants(&self, workflow_id: &str) -> Result<Vec<String>, Error> {
        self.attempt(|| async {
            let mut seen = vec![workflow_id.to_owned()];
            let mut frontier = seen.clone();
            while !frontier.is_empty() {
                let mut next = Vec::new();
                for chunk in frontier.chunks(RECHECK_CHUNK) {
                    let sql = format!(
                        "SELECT workflow_uuid FROM workflow_status WHERE 1 = 1{} ORDER BY workflow_uuid",
                        in_clause("forked_from", chunk.len())
                    );
                    let mut query = sqlx::query_scalar(self.sql(&sql));
                    for id in chunk {
                        query = query.bind(id.as_str());
                    }
                    let found: Vec<String> = query.fetch_all(&self.pool).await?;
                    next.extend(found.into_iter().filter(|id| !seen.contains(id)));
                }
                seen.extend(next.iter().cloned());
                frontier = next;
            }
            seen.remove(0);
            Ok(seen)
        })
        .await
    }

    /// The queue must exist, and a partition key is given exactly when the queue is partitioned.
    async fn check_queue_entry(
        &self,
        tx: &mut Transaction<'static, Any>,
        queue: &str,
        partition_key: Option<&str>,
    ) -> Result<(), Error> {
        let partitioned: i64 = sqlx::query_scalar(self.sql(&format!(
            "SELECT CASE WHEN partition_concurrency IS NOT NULL OR partition_worker_concurrency IS NOT NULL \
             OR partition_rate_limit IS NOT NULL THEN 1 ELSE 0 END FROM workflow_queues WHERE name = ? AND deleted = 0 {}",
            self.dialect.lock_row(),
        )))
        .bind(queue)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| Error::QueueNotFound(queue.to_owned()))?;
        if (partitioned != 0) != partition_key.is_some() {
            return Err(Error::InvalidConfig(
                "partition key is required exactly when the queue is partitioned".to_owned(),
            ));
        }
        Ok(())
    }

    /// When the last step kept before `start_step` was saved, or `None` when none is kept.
    // shortcut: event history has no step ID, so events are split from steps by database time. An event
    // saved in the same millisecond as the last kept step, or by steps running side by side, can land on
    // the wrong side. Upgrade by storing the step ID in workflow_event_history.
    async fn kept_until(
        &self,
        tx: &mut Transaction<'static, Any>,
        workflow_id: &str,
        start_step: u32,
    ) -> Result<Option<i64>, Error> {
        Ok(sqlx::query_scalar(self.sql(
            "SELECT MAX(completed_at_epoch_ms) FROM operation_outputs WHERE workflow_uuid = ? AND function_id < ?",
        ))
        .bind(workflow_id)
        .bind(i64::from(start_step))
        .fetch_one(&mut **tx)
        .await?)
    }

    /// Sets each event to its newest remaining revision, and removes events with none left.
    async fn rebuild_events(
        &self,
        tx: &mut Transaction<'static, Any>,
        workflow_id: &str,
    ) -> Result<(), Error> {
        sqlx::query(self.sql("DELETE FROM workflow_events WHERE workflow_uuid = ?"))
            .bind(workflow_id)
            .execute(&mut **tx)
            .await?;
        sqlx::query(self.sql(
            "INSERT INTO workflow_events (workflow_uuid, {KEY}, value, serialization) \
             SELECT history.workflow_uuid, history.event_key, history.value, history.serialization \
             FROM workflow_event_history AS history WHERE history.workflow_uuid = ? AND history.revision = \
             (SELECT MAX(newest.revision) FROM workflow_event_history AS newest \
             WHERE newest.workflow_uuid = history.workflow_uuid AND newest.event_key = history.event_key)",
        ))
        .bind(workflow_id)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}
