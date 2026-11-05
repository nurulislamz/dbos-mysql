use sqlx::{Any, AnyConnection, Row, Transaction};

use super::{Store, check_name, get_u32, record_from_row};
use crate::Error;
use crate::workflow::{
    Lease, RecordedStep, Status, StepRecord, check_recorded_step, check_step_name,
};

impl Store {
    pub(super) async fn owned_transaction(
        &self,
        lease: &Lease,
    ) -> Result<(Transaction<'static, Any>, String), Error> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(self.sql(&format!(
            "SELECT status, lease_token, serialization FROM workflow_status WHERE workflow_uuid = ? {}",
            self.dialect.lock_row(),
        )))
        .bind(lease.workflow_id.as_str())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| Error::WorkflowNotFound(lease.workflow_id.clone()))?;
        if self
            .cancel_if_expired_on(&mut tx, &lease.workflow_id)
            .await?
        {
            tx.commit().await?;
            return Err(Error::WorkflowCancelled(lease.workflow_id.clone()));
        }
        let status: String = row.try_get("status")?;
        if Status::parse(&status)? == Status::Cancelled {
            return Err(Error::WorkflowCancelled(lease.workflow_id.clone()));
        }
        let token: Option<String> = row.try_get("lease_token")?;
        if status != "PENDING" || token.as_deref() != Some(lease.token.as_str()) {
            return Err(Error::LeaseLost(lease.workflow_id.clone()));
        }
        let serialization = row.try_get("serialization")?;
        Ok((tx, serialization))
    }

    pub async fn load_step_owned(
        &self,
        lease: &Lease,
        step_id: u32,
        step_name: &str,
    ) -> Result<Option<StepRecord>, Error> {
        check_name("step name", step_name)?;
        self.attempt(|| async {
            let (mut tx, _) = self.owned_transaction(lease).await?;
            let saved = self.read_step(&mut tx, &lease.workflow_id, step_id).await?;
            let saved = saved
                .map(|(name, record)| -> Result<StepRecord, Error> {
                    check_step_name(&lease.workflow_id, step_id, step_name, name)?;
                    Ok(record)
                })
                .transpose()?;
            // Revoked, not only refused: an SDK reports this through the body, and `finish` must not store it as final.
            let revoked = saved.is_none()
                && sqlx::query(self.sql(
                    "UPDATE workflow_status SET lease_token = NULL, updated_at = {NOW} \
                     WHERE workflow_uuid = ? AND lease_token = ? AND claim_after <= {NOW}",
                ))
                .bind(lease.workflow_id.as_str())
                .bind(lease.token.as_str())
                .execute(&mut *tx)
                .await?
                .rows_affected()
                    > 0;
            tx.commit().await?;
            if revoked {
                return Err(Error::LeaseLost(lease.workflow_id.clone()));
            }
            Ok(saved)
        })
        .await
    }

    pub async fn load_step(
        &self,
        workflow_id: &str,
        step_id: u32,
        step_name: &str,
    ) -> Result<Option<StepRecord>, Error> {
        check_name("step name", step_name)?;
        self.attempt(|| async {
            let mut conn = self.pool.acquire().await?;
            let exists =
                sqlx::query(self.sql("SELECT 1 FROM workflow_status WHERE workflow_uuid = ?"))
                    .bind(workflow_id)
                    .fetch_optional(&mut *conn)
                    .await?;
            if exists.is_none() {
                return Err(Error::WorkflowNotFound(workflow_id.to_owned()));
            }
            let Some((recorded_name, record)) =
                self.read_step(&mut conn, workflow_id, step_id).await?
            else {
                return Ok(None);
            };
            check_step_name(workflow_id, step_id, step_name, recorded_name)?;
            Ok(Some(record))
        })
        .await
    }

    pub(super) async fn read_step(
        &self,
        conn: &mut AnyConnection,
        workflow_id: &str,
        step_id: u32,
    ) -> Result<Option<(String, StepRecord)>, Error> {
        let row = sqlx::query(self.sql(
            "SELECT function_name, output, error, child_workflow_id FROM operation_outputs WHERE workflow_uuid = ? AND function_id = ?",
        ))
        .bind(workflow_id)
        .bind(i64::from(step_id))
        .fetch_optional(conn)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some((
            row.try_get("function_name")?,
            record_from_row(&row)?,
        )))
    }

    pub async fn save_step(
        &self,
        lease: &Lease,
        step_id: u32,
        step_name: &str,
        record: &StepRecord,
    ) -> Result<(), Error> {
        check_name("step name", step_name)?;
        if let Some(child) = &record.child_workflow_id {
            check_name("child workflow ID", child)?;
        }
        self.check_size(&[record.output.as_deref(), record.error.as_deref()])?;
        self.attempt(|| self.save_step_once(lease, step_id, step_name, record))
            .await
    }

    async fn save_step_once(
        &self,
        lease: &Lease,
        step_id: u32,
        step_name: &str,
        record: &StepRecord,
    ) -> Result<(), Error> {
        let (mut tx, serialization) = self.owned_transaction(lease).await?;
        if let Some(recorded) = self.read_step(&mut tx, &lease.workflow_id, step_id).await? {
            return check_recorded_step(&lease.workflow_id, step_id, step_name, record, &recorded);
        }
        sqlx::query(self.sql(
            "INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, output, error, child_workflow_id, serialization, completed_at_epoch_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, {NOW})",
        ))
        .bind(lease.workflow_id.as_str())
        .bind(i64::from(step_id))
        .bind(step_name)
        .bind(record.output.as_deref())
        .bind(record.error.as_deref())
        .bind(record.child_workflow_id.as_deref())
        .bind(serialization.as_str())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn list_steps(&self, workflow_id: &str) -> Result<Vec<RecordedStep>, Error> {
        self.attempt(|| async {
            let rows = sqlx::query(self.sql(
                "SELECT function_id, function_name, output, error, child_workflow_id, serialization FROM operation_outputs WHERE workflow_uuid = ? ORDER BY function_id",
            ))
            .bind(workflow_id)
            .fetch_all(&self.pool)
            .await?;
            rows.iter()
                .map(|row| {
                    Ok(RecordedStep {
                        step_id: get_u32(row, "function_id")?,
                        step_name: row.try_get("function_name")?,
                        record: record_from_row(row)?,
                        serialization: row.try_get("serialization")?,
                    })
                })
                .collect()
        })
        .await
    }
}
