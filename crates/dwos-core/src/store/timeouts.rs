use sqlx::AnyConnection;

use super::Store;
use crate::Error;

impl Store {
    /// Cancel every pending workflow whose stored deadline has passed, using the database clock.
    pub async fn cancel_expired(&self) -> Result<u64, Error> {
        self.attempt(|| async {
            let result = sqlx::query(self.sql(
                "UPDATE workflow_status SET status = 'CANCELLED', lease_token = NULL, claim_after = NULL, \
                 completed_at = {NOW}, updated_at = {NOW} \
                 WHERE status = 'PENDING' AND workflow_uuid IN (SELECT workflow_uuid FROM workflow_timeouts WHERE deadline_epoch_ms <= {NOW})",
            ))
            .execute(&self.pool)
            .await?;
            Ok(result.rows_affected())
        })
        .await
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
