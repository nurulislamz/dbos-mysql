use std::time::Duration;

use sqlx::any::AnyRow;
use sqlx::{Any, Row, Transaction};

use super::{Store, get_u32, millis, require_owner};
use crate::Error;
use crate::lease::LeaseKeeper;
use crate::workflow::{Claimed, Lease, PY_PICKLE, attempts_exhausted, new_token};

impl Store {
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

    pub async fn count_due_pickled(&self) -> Result<u64, Error> {
        self.attempt(|| async {
            let count: i64 = sqlx::query_scalar(self.sql(
                "SELECT COUNT(*) FROM workflow_status \
                 WHERE claim_after <= {NOW} AND status = 'PENDING' AND serialization = ?",
            ))
            .bind(PY_PICKLE)
            .fetch_one(&self.pool)
            .await?;
            u64::try_from(count).map_err(|_| Error::Corrupt(format!("count is negative: {count}")))
        })
        .await
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
        let request_token = new_token();
        let mut retry = false;
        self.attempt(|| {
            let replay = retry;
            retry = true;
            self.reclaim_once(names, limit, duration, &request_token, replay)
        })
        .await
    }

    async fn reclaim_once(
        &self,
        names: &[String],
        limit: u32,
        duration: Duration,
        request_token: &str,
        retry: bool,
    ) -> Result<Vec<Claimed>, Error> {
        let mut tx = self.begin().await?;
        if retry
            && let Some(claimed) = self
                .replay_claim_request(&mut tx, request_token, duration)
                .await?
        {
            tx.commit().await?;
            return Ok(claimed);
        }
        let select_sql = self.sql(&format!(
            "SELECT workflow_uuid, name, inputs, recovery_attempts, max_recovery_attempts, serialization, (SELECT deadline_epoch_ms FROM workflow_timeouts WHERE workflow_timeouts.workflow_uuid = workflow_status.workflow_uuid) AS workflow_deadline_epoch_ms, {{NOW}} AS now_ms FROM workflow_status \
             WHERE claim_after <= {{NOW}} AND status = 'PENDING' AND NOT EXISTS \
             (SELECT 1 FROM workflow_queue_entries WHERE workflow_queue_entries.workflow_uuid = workflow_status.workflow_uuid) \
             AND serialization IN ({}) AND name IN ({}) \
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
            // MySQL reads the NOT EXISTS above from a view older than this locked row, so a queue
            // member committed during the scan looks ordinary. Only a queue claim may admit it.
            let workflow_id: String = row.try_get("workflow_uuid")?;
            let queued = sqlx::query(
                self.sql("SELECT 1 FROM workflow_queue_entries WHERE workflow_uuid = ?"),
            )
            .bind(workflow_id.as_str())
            .fetch_optional(&mut *tx)
            .await?;
            if queued.is_some() {
                continue;
            }
            if let Some(claim) = self
                .claim_locked_row(&mut tx, &row, request_token, duration)
                .await?
            {
                claimed.push(claim);
            }
        }
        tx.commit().await?;
        Ok(claimed)
    }

    pub(super) async fn claim_locked_row(
        &self,
        tx: &mut Transaction<'static, Any>,
        row: &AnyRow,
        request_token: &str,
        duration: Duration,
    ) -> Result<Option<Claimed>, Error> {
        let workflow_id: String = row.try_get("workflow_uuid")?;
        // The caller holds the workflow lock before checking its current deadline.
        if self.cancel_if_expired_on(tx, &workflow_id).await? {
            return Ok(None);
        }
        let attempts = get_u32(row, "recovery_attempts")?.saturating_add(1);
        if attempts_exhausted(attempts, get_u32(row, "max_recovery_attempts")?) {
            sqlx::query(self.sql(
                "UPDATE workflow_status SET status = 'MAX_RECOVERY_ATTEMPTS_EXCEEDED', recovery_attempts = ?, \
                 lease_token = NULL, claim_after = NULL, completed_at = {NOW}, updated_at = {NOW} WHERE workflow_uuid = ?",
            ))
            .bind(i64::from(attempts))
            .bind(workflow_id.as_str())
            .execute(&mut **tx)
            .await?;
            return Ok(None);
        }
        let deadline_epoch_ms: Option<i64> = sqlx::query_scalar(
            self.sql("SELECT deadline_epoch_ms FROM workflow_timeouts WHERE workflow_uuid = ?"),
        )
        .bind(workflow_id.as_str())
        .fetch_optional(&mut **tx)
        .await?;
        sqlx::query(self.sql(
            "UPDATE workflow_status SET lease_token = ?, recovery_attempts = ?, \
             claim_after = {NOW} + ?, updated_at = {NOW} WHERE workflow_uuid = ?",
        ))
        .bind(request_token)
        .bind(i64::from(attempts))
        .bind(millis(duration))
        .bind(workflow_id.as_str())
        .execute(&mut **tx)
        .await?;
        Ok(Some(Claimed {
            lease: Lease {
                workflow_id,
                token: request_token.to_owned(),
            },
            name: row.try_get("name")?,
            input: row.try_get("inputs")?,
            recovery_attempts: attempts,
            serialization: row.try_get("serialization")?,
            deadline_epoch_ms,
        }))
    }

    pub(super) async fn replay_claim_request(
        &self,
        tx: &mut Transaction<'static, Any>,
        request_token: &str,
        duration: Duration,
    ) -> Result<Option<Vec<Claimed>>, Error> {
        let rows = sqlx::query(self.sql(&format!(
            "SELECT workflow_uuid, name, inputs, recovery_attempts, serialization, \
             (SELECT deadline_epoch_ms FROM workflow_timeouts WHERE workflow_timeouts.workflow_uuid = workflow_status.workflow_uuid) AS workflow_deadline_epoch_ms \
             FROM workflow_status WHERE lease_token = ? AND status = 'PENDING' ORDER BY workflow_uuid {}",
            self.dialect.lock_row(),
        )))
        .bind(request_token)
        .fetch_all(&mut **tx)
        .await?;
        if rows.is_empty() {
            return Ok(None);
        }
        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let workflow_id: String = row.try_get("workflow_uuid")?;
            if self.cancel_if_expired_on(tx, &workflow_id).await? {
                continue;
            }
            sqlx::query(self.sql(
                "UPDATE workflow_status SET claim_after = {NOW} + ?, updated_at = {NOW} \
                 WHERE workflow_uuid = ? AND lease_token = ? AND status = 'PENDING'",
            ))
            .bind(millis(duration))
            .bind(workflow_id.as_str())
            .bind(request_token)
            .execute(&mut **tx)
            .await?;
            claimed.push(Claimed {
                lease: Lease {
                    workflow_id,
                    token: request_token.to_owned(),
                },
                name: row.try_get("name")?,
                input: row.try_get("inputs")?,
                recovery_attempts: get_u32(&row, "recovery_attempts")?,
                serialization: row.try_get("serialization")?,
                deadline_epoch_ms: row.try_get("workflow_deadline_epoch_ms")?,
            });
        }
        Ok(Some(claimed))
    }

    pub fn keep_lease(&self, lease: Lease, duration: Duration, max_run: Duration) -> LeaseKeeper {
        let store = self.clone();
        LeaseKeeper::spawn(duration, max_run, move || {
            let (store, lease) = (store.clone(), lease.clone());
            async move { store.renew_lease(&lease, duration).await }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::{NewWorkflow, PORTABLE_JSON, Status};

    async fn pending(store: &Store, id: &str) {
        store
            .start_workflow(&NewWorkflow {
                workflow_id: id,
                name: "wf",
                input: "{}",
                max_recovery_attempts: 1,
                lease: Duration::ZERO,
                parent_workflow_id: None,
                serialization: PORTABLE_JSON,
                timeout_ms: None,
                deadline_epoch_ms: None,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn reclaim_retries_a_committed_request_without_spending_attempts() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::connect(&format!(
            "sqlite://{}",
            directory.path().join("test.sqlite").display()
        ))
        .await
        .unwrap();
        store.migrate().await.unwrap();
        for id in ["a", "b"] {
            pending(&store, id).await;
        }
        let names = vec!["wf".to_owned()];
        let request = new_token();
        let duration = Duration::from_secs(60);
        let committed = store
            .reclaim_once(&names, 2, duration, &request, false)
            .await
            .unwrap();
        assert_eq!(committed.len(), 2);
        // The caller lost the commit acknowledgement and retries the same request.
        let replayed = store
            .reclaim_once(&names, 2, duration, &request, true)
            .await
            .unwrap();
        assert_eq!(replayed, committed);
        for id in ["a", "b"] {
            let row = store.get_workflow(id).await.unwrap().unwrap();
            assert_eq!(row.recovery_attempts, 2);
            assert_eq!(row.status, Status::Pending);
        }
    }

    #[tokio::test]
    async fn retry_does_not_resurrect_replaced_request_ownership() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::connect(&format!(
            "sqlite://{}",
            directory.path().join("test.sqlite").display()
        ))
        .await
        .unwrap();
        store.migrate().await.unwrap();
        pending(&store, "a").await;
        let names = vec!["wf".to_owned()];
        let request = new_token();
        let committed = store
            .reclaim_once(&names, 1, Duration::ZERO, &request, false)
            .await
            .unwrap();
        store.release_lease(&committed[0].lease).await.unwrap();
        let current = store
            .reclaim(&names, 1, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(current.len(), 1);
        assert_ne!(current[0].lease.token, request);
        assert!(
            store
                .reclaim_once(&names, 1, Duration::from_secs(60), &request, true)
                .await
                .unwrap()
                .is_empty()
        );
        store
            .renew_lease(&current[0].lease, Duration::from_secs(60))
            .await
            .unwrap();
    }
}
