use std::collections::HashSet;

use sqlx::{Any, AnyConnection, Transaction};

use super::{Dialect, Store};
use crate::Error;

const RECHECK_CHUNK: usize = 200;

impl Store {
    /// Cancel every pending workflow whose stored deadline has passed, using the database clock.
    pub async fn cancel_expired(&self) -> Result<u64, Error> {
        self.attempt(|| async {
            let mut tx = self.begin().await?;
            let expired: Vec<String> = sqlx::query_scalar(self.sql(
                "SELECT workflow_uuid FROM workflow_status WHERE status = 'PENDING' AND workflow_uuid IN \
                 (SELECT workflow_uuid FROM workflow_timeouts WHERE deadline_epoch_ms <= {NOW})",
            ))
            .fetch_all(&mut *tx)
            .await?;
            let cancelled = self
                .recheck_update(
                    &mut tx,
                    &expired,
                    "status = 'CANCELLED', lease_token = NULL, claim_after = NULL, completed_at = {NOW}, updated_at = {NOW}",
                    "status = 'PENDING' AND EXISTS (SELECT 1 FROM workflow_timeouts WHERE workflow_timeouts.workflow_uuid = workflow_status.workflow_uuid AND deadline_epoch_ms <= {NOW})",
                    "status = 'PENDING' AND workflow_uuid IN (SELECT workflow_uuid FROM workflow_timeouts WHERE deadline_epoch_ms <= {NOW})",
                    None,
                )
                .await?;
            tx.commit().await?;
            Ok(cancelled)
        })
        .await
    }

    /// Update candidate primary keys. Rows this transaction can lock are updated together.
    /// A row someone else already holds is updated only after that, so the wait cannot sit
    /// in front of the free rows. `wait_predicate` is the recheck on that later update.
    pub(super) async fn recheck_update(
        &self,
        tx: &mut Transaction<'static, Any>,
        ids: &[String],
        assignment: &str,
        lock_predicate: &str,
        wait_predicate: &str,
        cutoff: Option<i64>,
    ) -> Result<u64, Error> {
        if ids.is_empty() {
            return Ok(0);
        }
        // SQLite has one writer and no row locks. One rechecked update per chunk is the whole sweep.
        if self.dialect == Dialect::Sqlite {
            let mut changed = 0;
            for chunk in ids.chunks(RECHECK_CHUNK) {
                changed += self
                    .update_where(tx, chunk, assignment, wait_predicate, cutoff)
                    .await?;
            }
            return Ok(changed);
        }
        let mut changed = 0;
        let mut skipped = Vec::new();
        for chunk in ids.chunks(RECHECK_CHUNK) {
            let free = self
                .select_where(tx, chunk, lock_predicate, cutoff, true)
                .await?;
            if !free.is_empty() {
                changed += self
                    .update_where(tx, &free, assignment, lock_predicate, cutoff)
                    .await?;
            }
            if free.len() != chunk.len() {
                let held: HashSet<&str> = free.iter().map(String::as_str).collect();
                skipped.extend(
                    chunk
                        .iter()
                        .filter(|id| !held.contains(id.as_str()))
                        .cloned(),
                );
            }
        }
        for chunk in skipped.chunks(RECHECK_CHUNK) {
            // Not locked with the free rows. Keep only ids that still match, then recheck each one.
            let waiting = self
                .select_where(tx, chunk, wait_predicate, cutoff, false)
                .await?;
            for id in waiting {
                changed += self
                    .update_where(
                        tx,
                        std::slice::from_ref(&id),
                        assignment,
                        wait_predicate,
                        cutoff,
                    )
                    .await?;
            }
        }
        Ok(changed)
    }

    async fn select_where(
        &self,
        tx: &mut Transaction<'static, Any>,
        ids: &[String],
        predicate: &str,
        cutoff: Option<i64>,
        skip_locked: bool,
    ) -> Result<Vec<String>, Error> {
        let lock = if skip_locked {
            self.dialect.lock_free_rows()
        } else {
            ""
        };
        let statement = self.sql(&format!(
            "SELECT workflow_uuid FROM workflow_status WHERE workflow_uuid IN ({}) AND {predicate} {lock}",
            vec!["?"; ids.len()].join(", ")
        ));
        let mut query = sqlx::query_scalar(statement);
        for id in ids {
            query = query.bind(id.as_str());
        }
        if let Some(cutoff) = cutoff {
            query = query.bind(cutoff);
        }
        Ok(query.fetch_all(&mut **tx).await?)
    }

    async fn update_where(
        &self,
        tx: &mut Transaction<'static, Any>,
        ids: &[String],
        assignment: &str,
        predicate: &str,
        cutoff: Option<i64>,
    ) -> Result<u64, Error> {
        let id_sql = if ids.len() == 1 {
            "workflow_uuid = ?".to_owned()
        } else {
            format!("workflow_uuid IN ({})", vec!["?"; ids.len()].join(", "))
        };
        let statement = self.sql(&format!(
            "UPDATE workflow_status SET {assignment} WHERE {id_sql} AND {predicate}"
        ));
        let mut query = sqlx::query(statement);
        for id in ids {
            query = query.bind(id.as_str());
        }
        if let Some(cutoff) = cutoff {
            query = query.bind(cutoff);
        }
        Ok(query.execute(&mut **tx).await?.rows_affected())
    }

    /// Cancel this workflow if it is still pending and its deadline has passed.
    pub async fn cancel_if_expired(&self, workflow_id: &str) -> Result<bool, Error> {
        self.attempt(|| async {
            let mut conn = self.pool.acquire().await?;
            self.cancel_if_expired_on(&mut conn, workflow_id).await
        })
        .await
    }

    pub(crate) async fn cancel_if_expired_on(
        &self,
        conn: &mut AnyConnection,
        workflow_id: &str,
    ) -> Result<bool, Error> {
        let result = sqlx::query(self.sql(
            "UPDATE workflow_status SET status = 'CANCELLED', lease_token = NULL, claim_after = NULL, \
             completed_at = {NOW}, updated_at = {NOW} \
             WHERE workflow_uuid = ? AND status = 'PENDING' AND workflow_uuid IN \
             (SELECT workflow_uuid FROM workflow_timeouts WHERE deadline_epoch_ms <= {NOW})",
        ))
        .bind(workflow_id)
        .execute(conn)
        .await?;
        Ok(result.rows_affected() > 0)
    }
}
