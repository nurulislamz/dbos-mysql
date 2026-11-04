use std::time::Duration;

use sqlx::Row;

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

    pub fn keep_lease(&self, lease: Lease, duration: Duration, max_run: Duration) -> LeaseKeeper {
        let store = self.clone();
        LeaseKeeper::spawn(duration, max_run, move || {
            let (store, lease) = (store.clone(), lease.clone());
            async move { store.renew_lease(&lease, duration).await }
        })
    }
}
