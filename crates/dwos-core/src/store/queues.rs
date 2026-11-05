use std::collections::HashMap;
use std::num::NonZeroU32;
use std::time::Duration;

use sqlx::{Any, Row, Transaction};

use super::{Dialect, SELECT_WORKFLOW, Store, check_name, get_u32, workflow_from_row};
use crate::Error;
use crate::workflow::{
    Claimed, NewWorkflow, ReusePolicy, WorkflowOptions, check_workflow_name, id_in_use, new_token,
};

fn check_queue_name(name: &str) -> Result<(), Error> {
    check_name("queue name", name)?;
    if name.is_empty() {
        return Err(Error::InvalidConfig(
            "queue name must not be empty".to_owned(),
        ));
    }
    Ok(())
}

impl Store {
    pub async fn get_queue(&self, name: &str) -> Result<Option<NonZeroU32>, Error> {
        check_queue_name(name)?;
        self.attempt(|| async {
            let row =
                sqlx::query(self.sql("SELECT concurrency FROM workflow_queues WHERE name = ?"))
                    .bind(name)
                    .fetch_optional(&self.pool)
                    .await?;
            row.map(|row| {
                NonZeroU32::new(get_u32(&row, "concurrency")?)
                    .ok_or_else(|| Error::Corrupt(format!("queue {name} has zero concurrency")))
            })
            .transpose()
        })
        .await
    }

    pub async fn ensure_queue(&self, name: &str, concurrency: NonZeroU32) -> Result<(), Error> {
        check_queue_name(name)?;
        self.attempt(|| async {
            let mut tx = self.begin().await?;
            let upsert = match self.dialect {
                Dialect::MySql => {
                    "AS incoming ON DUPLICATE KEY UPDATE concurrency = incoming.concurrency"
                }
                Dialect::Postgres | Dialect::Sqlite => {
                    "ON CONFLICT (name) DO UPDATE SET concurrency = excluded.concurrency"
                }
            };
            sqlx::query(self.sql(&format!(
                "INSERT INTO workflow_queues (name, concurrency) VALUES (?, ?) {upsert}"
            )))
            .bind(name)
            .bind(i64::from(concurrency.get()))
            .execute(&mut *tx)
            .await?;
            self.lock_queue(&mut tx, name).await?;
            tx.commit().await?;
            Ok(())
        })
        .await
    }

    /// Creates a queue with no limit, and leaves an existing queue's limit alone.
    pub async fn ensure_queue_exists(&self, name: &str) -> Result<(), Error> {
        check_queue_name(name)?;
        let conflict = match self.dialect {
            Dialect::MySql => "ON DUPLICATE KEY UPDATE name = name",
            Dialect::Postgres | Dialect::Sqlite => "ON CONFLICT (name) DO NOTHING",
        };
        self.attempt(|| async {
            sqlx::query(self.sql(&format!(
                "INSERT INTO workflow_queues (name, concurrency) VALUES (?, ?) {conflict}"
            )))
            .bind(name)
            .bind(i64::from(u32::MAX))
            .execute(&self.pool)
            .await?;
            Ok(())
        })
        .await
    }

    async fn lock_queue(
        &self,
        tx: &mut Transaction<'static, Any>,
        queue: &str,
    ) -> Result<u32, Error> {
        let row = sqlx::query(self.sql(&format!(
            "SELECT concurrency FROM workflow_queues WHERE name = ? {}",
            self.dialect.lock_row(),
        )))
        .bind(queue)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| Error::QueueNotFound(queue.to_owned()))?;
        let concurrency = get_u32(&row, "concurrency")?;
        if concurrency == 0 {
            return Err(Error::Corrupt(format!(
                "queue {queue} has zero concurrency"
            )));
        }
        Ok(concurrency)
    }

    /// Puts each resumable workflow on `queue` and clears its deadline, so admission, not the old
    /// timeout, decides when it runs. SUCCESS, ERROR and a PENDING row whose lease has not expired
    /// are left alone, so a live owner keeps running and counting. One missing id rolls back.
    pub async fn place_on_queue(&self, workflow_ids: &[String], queue: &str) -> Result<(), Error> {
        check_queue_name(queue)?;
        if workflow_ids.is_empty() {
            return Ok(());
        }
        for id in workflow_ids {
            check_name("workflow ID", id)?;
        }
        self.attempt(|| self.place_on_queue_once(workflow_ids, queue))
            .await
    }

    async fn place_on_queue_once(&self, workflow_ids: &[String], queue: &str) -> Result<(), Error> {
        let mut tx = self.begin().await?;
        self.lock_queue(&mut tx, queue).await?;
        for id in workflow_ids {
            let row = sqlx::query(self.sql(&format!(
                "SELECT status, lease_token, claim_after, {{NOW}} AS now_ms FROM workflow_status \
                 WHERE workflow_uuid = ? {}",
                self.dialect.lock_row(),
            )))
            .bind(id.as_str())
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| Error::WorkflowNotFound(id.clone()))?;
            let status: String = row.try_get("status")?;
            let lease_token: Option<String> = row.try_get("lease_token")?;
            let claim_after: Option<i64> = row.try_get("claim_after")?;
            let now_ms: i64 = row.try_get("now_ms")?;
            let live_owner = status == "PENDING"
                && lease_token.is_some()
                && claim_after.is_some_and(|claim_after| claim_after > now_ms);
            if status == "SUCCESS" || status == "ERROR" || live_owner {
                continue;
            }
            sqlx::query(self.sql(
                "UPDATE workflow_status SET status = 'PENDING', lease_token = NULL, claim_after = {NOW}, \
                 recovery_attempts = 0, completed_at = NULL, started_at_epoch_ms = NULL, updated_at = {NOW} \
                 WHERE workflow_uuid = ?",
            ))
            .bind(id.as_str())
            .execute(&mut *tx)
            .await?;
            sqlx::query(self.sql("DELETE FROM workflow_timeouts WHERE workflow_uuid = ?"))
                .bind(id.as_str())
                .execute(&mut *tx)
                .await?;
            let entry: Option<String> =
                sqlx::query_scalar(self.sql(
                    "SELECT workflow_uuid FROM workflow_queue_entries WHERE workflow_uuid = ?",
                ))
                .bind(id.as_str())
                .fetch_optional(&mut *tx)
                .await?;
            if entry.is_some() {
                sqlx::query(self.sql(
                    "UPDATE workflow_queue_entries SET queue_name = ? WHERE workflow_uuid = ?",
                ))
                .bind(queue)
                .bind(id.as_str())
                .execute(&mut *tx)
                .await?;
            } else {
                sqlx::query(self.sql(
                    "INSERT INTO workflow_queue_entries (workflow_uuid, queue_name, start_timeout_ms) VALUES (?, ?, NULL)",
                ))
                .bind(id.as_str())
                .bind(queue)
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn enqueue_workflow(
        &self,
        queue: &str,
        new: &NewWorkflow<'_>,
    ) -> Result<String, Error> {
        self.enqueue_workflow_with(queue, new, &WorkflowOptions::default())
            .await
    }

    pub async fn enqueue_workflow_with(
        &self,
        queue: &str,
        new: &NewWorkflow<'_>,
        options: &WorkflowOptions,
    ) -> Result<String, Error> {
        check_queue_name(queue)?;
        check_name("workflow ID", new.workflow_id)?;
        check_name("workflow name", new.name)?;
        if options
            .priority
            .is_some_and(|priority| !(1..=i64::from(i32::MAX)).contains(&priority))
        {
            return Err(Error::InvalidConfig(
                "priority must be between 1 and 2147483647".to_owned(),
            ));
        }
        if let Some(id) = &options.deduplication_id {
            check_name("deduplication ID", id)?;
        } else if options.duplication_policy.is_some() {
            return Err(Error::InvalidConfig(
                "duplication_policy requires a deduplication ID".to_owned(),
            ));
        }
        if let Some(parent) = new.parent_workflow_id {
            check_name("parent workflow ID", parent)?;
        }
        if new.timeout_ms.is_some_and(|timeout| timeout <= 0) {
            return Err(Error::InvalidConfig(
                "workflow timeout must be positive".to_owned(),
            ));
        }
        if new.timeout_ms.is_some() && new.deadline_epoch_ms.is_some() {
            return Err(Error::InvalidConfig(
                "set a workflow timeout or a deadline, not both".to_owned(),
            ));
        }
        self.check_size(&[Some(new.input)])?;
        let token = new_token();
        self.attempt(|| self.enqueue_workflow_once(queue, new, options, &token))
            .await
    }

    async fn enqueue_workflow_once(
        &self,
        queue: &str,
        new: &NewWorkflow<'_>,
        options: &WorkflowOptions,
        token: &str,
    ) -> Result<String, Error> {
        let mut tx = self.begin().await?;
        self.lock_queue(&mut tx, queue).await?;
        // The temporary token distinguishes insertion from an existing unowned row on every backend.
        sqlx::query(self.sql(&format!(
            "INSERT INTO workflow_status (workflow_uuid, name, status, inputs, claim_after, lease_token, recovery_attempts, max_recovery_attempts, parent_workflow_id, serialization, created_at, updated_at) \
             VALUES (?, ?, 'PENDING', ?, {{NOW}}, ?, 0, ?, ?, ?, {{NOW}}, {{NOW}}) {}",
            self.dialect.insert_if_absent(),
        )))
        .bind(new.workflow_id)
        .bind(new.name)
        .bind(new.input)
        .bind(token)
        .bind(i64::from(new.max_recovery_attempts))
        .bind(new.parent_workflow_id)
        .bind(new.serialization)
        .execute(&mut *tx)
        .await?;
        let row = sqlx::query(self.sql(SELECT_WORKFLOW))
            .bind(new.workflow_id)
            .fetch_one(&mut *tx)
            .await?;
        let owner: Option<String> = row.try_get("lease_token")?;
        if owner.as_deref() != Some(token) {
            let existing = workflow_from_row(&row)?;
            if options.reuse == ReusePolicy::Reject {
                return Err(id_in_use(&existing));
            }
            check_workflow_name(new, &existing.name)?;
        }
        if owner.as_deref() == Some(token) {
            if let Some(id) = &options.deduplication_id {
                let existing: Option<String> = sqlx::query_scalar(self.sql(
                    "SELECT workflow_queue_entries.workflow_uuid FROM workflow_queue_entries JOIN workflow_status ON workflow_status.workflow_uuid = workflow_queue_entries.workflow_uuid WHERE workflow_queue_entries.queue_name = ? AND workflow_queue_entries.deduplication_id = ? AND workflow_status.status IN ('PENDING', 'ENQUEUED', 'DELAYED') LIMIT 1",
                )).bind(queue).bind(id.as_str()).fetch_optional(&mut *tx).await?;
                if let Some(existing) = existing {
                    if options.duplication_policy == Some(ReusePolicy::ReturnExisting) {
                        tx.rollback().await?;
                        return Ok(existing);
                    }
                    return Err(Error::QueueDeduplicated {
                        queue: queue.to_owned(),
                        deduplication_id: id.clone(),
                    });
                }
            }
            if let Some(timeout) = new.timeout_ms {
                let created: i64 = row.try_get("created_at")?;
                created.checked_add(timeout).ok_or_else(|| {
                    Error::InvalidConfig("workflow timeout is too large".to_owned())
                })?;
            }
            sqlx::query(self.sql(
                "INSERT INTO workflow_queue_entries (workflow_uuid, queue_name, start_timeout_ms, deduplication_id, priority) VALUES (?, ?, ?, ?, ?)",
            ))
            .bind(new.workflow_id).bind(queue).bind(new.timeout_ms).bind(options.deduplication_id.as_deref()).bind(options.priority.unwrap_or(0)).execute(&mut *tx).await?;
            if let Some(deadline) = new.deadline_epoch_ms {
                sqlx::query(self.sql(
                    "INSERT INTO workflow_timeouts (workflow_uuid, timeout_ms, deadline_epoch_ms) VALUES (?, NULL, ?)",
                ))
                .bind(new.workflow_id).bind(deadline).execute(&mut *tx).await?;
            }
            sqlx::query(self.sql("UPDATE workflow_status SET lease_token = NULL WHERE workflow_uuid = ? AND lease_token = ?"))
                .bind(new.workflow_id).bind(token).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(new.workflow_id.to_owned())
    }

    pub async fn claim_queue(
        &self,
        queue: &str,
        names: &[String],
        limit: u32,
        duration: Duration,
    ) -> Result<Vec<Claimed>, Error> {
        check_queue_name(queue)?;
        if names.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let request_token = new_token();
        let mut retry = false;
        self.attempt(|| {
            let replay = retry;
            retry = true;
            self.claim_queue_once(queue, names, limit, duration, &request_token, replay)
        })
        .await
    }

    async fn claim_queue_once(
        &self,
        queue: &str,
        names: &[String],
        limit: u32,
        duration: Duration,
        request_token: &str,
        retry: bool,
    ) -> Result<Vec<Claimed>, Error> {
        let mut tx = self.begin().await?;
        let concurrency = self.lock_queue(&mut tx, queue).await?;
        if retry
            && let Some(mut claimed) = self
                .replay_claim_request(&mut tx, request_token, duration)
                .await?
        {
            let order: Vec<String> = sqlx::query_scalar(self.sql(
                "SELECT workflow_queue_entries.workflow_uuid FROM workflow_queue_entries \
                 JOIN workflow_status ON workflow_status.workflow_uuid = workflow_queue_entries.workflow_uuid \
                 WHERE queue_name = ? AND lease_token = ? ORDER BY priority, sequence",
            )).bind(queue).bind(request_token).fetch_all(&mut *tx).await?;
            let positions: HashMap<_, _> = order
                .into_iter()
                .enumerate()
                .map(|(position, id)| (id, position))
                .collect();
            claimed.sort_by_key(|claim| {
                positions
                    .get(&claim.lease.workflow_id)
                    .copied()
                    .unwrap_or(usize::MAX)
            });
            tx.commit().await?;
            return Ok(claimed);
        }
        let cutoff: i64 = sqlx::query_scalar(self.sql("SELECT {NOW}"))
            .fetch_one(&mut *tx)
            .await?;
        // Revoke at one cutoff, then count every remaining token: a renewal cannot resurrect a freed slot.
        // MySQL scans retain unrelated row locks, so read candidates before locking primary keys.
        // Free rows are updated together. A row someone else holds is updated after those, so that
        // wait cannot leave the other rows expired.
        let expired_owners: Vec<String> = sqlx::query_scalar(self.sql(
            "SELECT workflow_uuid FROM workflow_status WHERE status = 'PENDING' AND lease_token IS NOT NULL \
             AND claim_after <= ? AND workflow_uuid IN (SELECT workflow_uuid FROM workflow_queue_entries WHERE queue_name = ?)",
        )).bind(cutoff).bind(queue).fetch_all(&mut *tx).await?;
        let revoke = "status = 'PENDING' AND lease_token IS NOT NULL AND claim_after <= ?";
        self.recheck_update(
            &mut tx,
            &expired_owners,
            "lease_token = NULL",
            revoke,
            revoke,
            Some(cutoff),
        )
        .await?;
        let expired_deadlines: Vec<String> = sqlx::query_scalar(self.sql(
            "SELECT workflow_uuid FROM workflow_status WHERE status = 'PENDING' \
             AND workflow_uuid IN (SELECT workflow_uuid FROM workflow_queue_entries WHERE queue_name = ?) \
             AND workflow_uuid IN (SELECT workflow_uuid FROM workflow_timeouts WHERE deadline_epoch_ms <= ?)",
        )).bind(queue).bind(cutoff).fetch_all(&mut *tx).await?;
        self.recheck_update(
            &mut tx,
            &expired_deadlines,
            "status = 'CANCELLED', lease_token = NULL, claim_after = NULL, completed_at = {NOW}, updated_at = {NOW}",
            "status = 'PENDING' AND EXISTS (SELECT 1 FROM workflow_timeouts WHERE workflow_timeouts.workflow_uuid = workflow_status.workflow_uuid AND deadline_epoch_ms <= ?)",
            "status = 'PENDING' AND workflow_uuid IN (SELECT workflow_uuid FROM workflow_timeouts WHERE deadline_epoch_ms <= ?)",
            Some(cutoff),
        )
        .await?;
        let occupied: i64 = sqlx::query_scalar(self.sql(
            "SELECT COUNT(*) FROM workflow_status WHERE status = 'PENDING' AND lease_token IS NOT NULL \
             AND workflow_uuid IN (SELECT workflow_uuid FROM workflow_queue_entries WHERE queue_name = ?)",
        )).bind(queue).fetch_one(&mut *tx).await?;
        let occupied = u32::try_from(occupied).map_err(|_| {
            Error::Corrupt(format!("queue owner count is out of range: {occupied}"))
        })?;
        let slots = concurrency.saturating_sub(occupied).min(limit);
        let mut claimed = Vec::new();
        let (compatible, binds) = self.compatible_clause();
        while claimed.len() < slots as usize {
            // The queue row lock already serializes claimers, so this read takes no row locks:
            // MySQL's ordered locking scan can keep unrelated live owners locked until commit.
            let read_sql = self.sql(&format!(
                    "SELECT workflow_queue_entries.workflow_uuid FROM workflow_queue_entries \
                     JOIN workflow_status ON workflow_status.workflow_uuid = workflow_queue_entries.workflow_uuid \
                     WHERE workflow_queue_entries.queue_name = ? AND workflow_status.status = 'PENDING' \
                     AND workflow_status.lease_token IS NULL AND workflow_status.claim_after <= ? \
                     AND workflow_status.serialization IN ({}) AND workflow_status.name IN ({}){compatible} \
                     ORDER BY workflow_queue_entries.priority, workflow_queue_entries.sequence LIMIT ?",
                    self.readable_formats(),
                    vec!["?"; names.len()].join(", "),
                ));
            let mut read = sqlx::query_scalar(read_sql).bind(queue).bind(cutoff);
            for name in names {
                read = read.bind(name.as_str());
            }
            for bind in &binds {
                read = read.bind(*bind);
            }
            let wanted = slots as usize - claimed.len();
            let found: Vec<String> = read.bind(wanted as i64).fetch_all(&mut *tx).await?;
            if found.is_empty() {
                break;
            }
            for workflow_id in found {
                let Some(row) = sqlx::query(self.sql(&format!(
                "SELECT workflow_uuid, name, inputs, recovery_attempts, max_recovery_attempts, serialization \
                 FROM workflow_status WHERE workflow_uuid = ? AND status = 'PENDING' \
                 AND lease_token IS NULL AND claim_after <= ? {}",
                self.dialect.lock_row(),
            )))
            .bind(workflow_id.as_str())
            .bind(cutoff)
            .fetch_optional(&mut *tx)
            .await? else {
                continue;
            };
                let timeout: Option<i64> = sqlx::query_scalar(self.sql(
                    "SELECT start_timeout_ms FROM workflow_queue_entries WHERE workflow_uuid = ?",
                ))
                .bind(workflow_id.as_str())
                .fetch_one(&mut *tx)
                .await?;
                if let Some(timeout) = timeout {
                    let now: i64 = sqlx::query_scalar(self.sql("SELECT {NOW}"))
                        .fetch_one(&mut *tx)
                        .await?;
                    // Enqueue checked this against an earlier clock. An error here would stop the queue for good.
                    let deadline = now.saturating_add(timeout);
                    sqlx::query(self.sql(&format!(
                    "INSERT INTO workflow_timeouts (workflow_uuid, timeout_ms, deadline_epoch_ms) VALUES (?, ?, ?) {}", self.dialect.insert_if_absent(),
                ))).bind(workflow_id.as_str()).bind(timeout).bind(deadline).execute(&mut *tx).await?;
                    sqlx::query(self.sql("UPDATE workflow_queue_entries SET start_timeout_ms = NULL WHERE workflow_uuid = ?"))
                    .bind(workflow_id.as_str()).execute(&mut *tx).await?;
                }
                if let Some(claim) = self
                    .claim_locked_row(&mut tx, &row, request_token, duration)
                    .await?
                {
                    claimed.push(claim);
                }
            }
        }
        tx.commit().await?;
        Ok(claimed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::PORTABLE_JSON;

    #[tokio::test]
    async fn committed_claim_retry_preserves_fifo_and_attempts() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::connect(&format!(
            "sqlite://{}",
            directory.path().join("test.sqlite").display()
        ))
        .await
        .unwrap();
        store.migrate().await.unwrap();
        store
            .ensure_queue("q", NonZeroU32::new(2).unwrap())
            .await
            .unwrap();
        for id in ["z", "a"] {
            store
                .enqueue_workflow(
                    "q",
                    &NewWorkflow {
                        workflow_id: id,
                        name: "wf",
                        input: "{}",
                        max_recovery_attempts: 0,
                        lease: Duration::ZERO,
                        parent_workflow_id: None,
                        serialization: PORTABLE_JSON,
                        timeout_ms: None,
                        deadline_epoch_ms: None,
                    },
                )
                .await
                .unwrap();
        }
        let request = new_token();
        let names = vec!["wf".to_owned()];
        let duration = Duration::from_secs(60);
        let committed = store
            .claim_queue_once("q", &names, 2, duration, &request, false)
            .await
            .unwrap();
        let replayed = store
            .claim_queue_once("q", &names, 2, duration, &request, true)
            .await
            .unwrap();
        assert_eq!(
            committed
                .iter()
                .map(|claim| claim.lease.workflow_id.as_str())
                .collect::<Vec<_>>(),
            ["z", "a"]
        );
        assert_eq!(replayed, committed);
        assert!(replayed.iter().all(|claim| claim.recovery_attempts == 1));
        assert!(
            store
                .claim_queue("q", &names, 2, duration)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
