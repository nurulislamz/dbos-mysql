use super::{Store, get_u32};
use crate::{Error, Lease};
use sqlx::Row;

impl Store {
    pub async fn continue_as_new(
        &self,
        lease: &Lease,
        new_token: &str,
        input: &str,
        step_base: u32,
    ) -> Result<u32, Error> {
        self.check_size(&[Some(input)])?;
        self.attempt(|| async {
            let (tx, _) = match self.owned_transaction(lease).await {
                Ok(owned) => owned,
                Err(Error::LeaseLost(_)) => {
                    let row = sqlx::query(self.sql("SELECT lease_token, step_base FROM workflow_status WHERE workflow_uuid = ? AND status = 'PENDING'"))
                        .bind(lease.workflow_id.as_str()).fetch_optional(&self.pool).await?;
                    return match row {
                        Some(row) if row.try_get::<Option<String>, _>("lease_token")?.as_deref() == Some(new_token) => get_u32(&row, "step_base"),
                        _ => Err(Error::LeaseLost(lease.workflow_id.clone())),
                    };
                }
                Err(error) => return Err(error),
            };
            let mut tx = self.live_transaction(tx, lease).await?;
            let max_saved: Option<i64> = sqlx::query_scalar(self.sql("SELECT MAX(function_id) FROM operation_outputs WHERE workflow_uuid = ?"))
                .bind(lease.workflow_id.as_str()).fetch_one(&mut *tx).await?;
            let step_base = i64::from(step_base).max(max_saved.unwrap_or(0));
            let step_base = u32::try_from(step_base).ok().filter(|value| *value < u32::MAX)
                .ok_or_else(|| Error::InvalidConfig("workflow has exhausted its step IDs".into()))?;
            sqlx::query(self.sql("DELETE FROM operation_outputs WHERE workflow_uuid = ?"))
                .bind(lease.workflow_id.as_str()).execute(&mut *tx).await?;
            sqlx::query(self.sql("UPDATE workflow_status SET inputs = ?, step_base = ?, lease_token = ?, recovery_attempts = 0, updated_at = {NOW} WHERE workflow_uuid = ?"))
                .bind(input).bind(i64::from(step_base)).bind(new_token).bind(lease.workflow_id.as_str()).execute(&mut *tx).await?;
            tx.commit().await?;
            Ok(step_base)
        }).await
    }
}
