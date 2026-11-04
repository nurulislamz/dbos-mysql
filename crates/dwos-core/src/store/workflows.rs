use sqlx::Row;

use super::{SELECT_WORKFLOW, Store, check_name, millis, require_owner, workflow_from_row};
use crate::Error;
use crate::workflow::{
    Lease, NewWorkflow, Outcome, Started, Status, Workflow, check_workflow_name, new_token,
};

impl Store {
    pub async fn start_workflow(&self, new: &NewWorkflow<'_>) -> Result<Started, Error> {
        if new.timeout_ms.is_some_and(|timeout| timeout <= 0) {
            return Err(Error::InvalidConfig(
                "workflow timeout must be positive".to_owned(),
            ));
        }
        check_name("workflow ID", new.workflow_id)?;
        check_name("workflow name", new.name)?;
        self.check_size(&[Some(new.input)])?;
        let token = new_token();
        self.attempt(|| self.start_workflow_once(new, &token)).await
    }

    async fn start_workflow_once(
        &self,
        new: &NewWorkflow<'_>,
        token: &str,
    ) -> Result<Started, Error> {
        let mut tx = self.begin().await?;
        sqlx::query(self.sql(&format!(
            "INSERT INTO workflow_status (workflow_uuid, name, status, inputs, claim_after, lease_token, max_recovery_attempts, parent_workflow_id, serialization, created_at, updated_at) \
             VALUES (?, ?, 'PENDING', ?, {{NOW}} + ?, ?, ?, ?, ?, {{NOW}}, {{NOW}}) {}",
            self.dialect.insert_if_absent()
        )))
        .bind(new.workflow_id)
        .bind(new.name)
        .bind(new.input)
        .bind(millis(new.lease))
        .bind(token)
        .bind(i64::from(new.max_recovery_attempts))
        .bind(new.parent_workflow_id)
        .bind(new.serialization)
        .execute(&mut *tx)
        .await?;

        // Compare tokens: a duplicate insert reports 1 row on MySQL and 0 on the others.
        let row = sqlx::query(self.sql(SELECT_WORKFLOW))
            .bind(new.workflow_id)
            .fetch_one(&mut *tx)
            .await?;
        let owner: Option<String> = row.try_get("lease_token")?;
        if owner.as_deref() == Some(token) {
            let created_at_ms: i64 = row.try_get("created_at")?;
            let deadline = match new.timeout_ms {
                Some(timeout) => Some(created_at_ms.checked_add(timeout).ok_or_else(|| {
                    Error::InvalidConfig("workflow timeout is too large".to_owned())
                })?),
                None => new.deadline_epoch_ms,
            };
            if let Some(deadline) = deadline {
                sqlx::query(self.sql(&format!(
                    "INSERT INTO workflow_timeouts (workflow_uuid, timeout_ms, deadline_epoch_ms) \
                     VALUES (?, ?, ?) {}",
                    self.dialect.insert_if_absent(),
                )))
                .bind(new.workflow_id)
                .bind(new.timeout_ms)
                .bind(deadline)
                .execute(&mut *tx)
                .await?;
            }
            let deadline_epoch_ms: Option<i64> = sqlx::query_scalar(
                self.sql("SELECT deadline_epoch_ms FROM workflow_timeouts WHERE workflow_uuid = ?"),
            )
            .bind(new.workflow_id)
            .fetch_optional(&mut *tx)
            .await?;
            tx.commit().await?;
            return Ok(Started::Owned(
                Lease {
                    workflow_id: new.workflow_id.to_owned(),
                    token: token.to_owned(),
                },
                deadline_epoch_ms,
            ));
        }
        let existing = workflow_from_row(&row)?;
        check_workflow_name(new, &existing.name)?;
        tx.commit().await?;
        Ok(Started::Existing(existing))
    }

    pub async fn get_workflow(&self, workflow_id: &str) -> Result<Option<Workflow>, Error> {
        self.attempt(|| async {
            let row = sqlx::query(self.sql(SELECT_WORKFLOW))
                .bind(workflow_id)
                .fetch_optional(&self.pool)
                .await?;
            row.as_ref().map(workflow_from_row).transpose()
        })
        .await
    }

    pub async fn finish_workflow(&self, lease: &Lease, outcome: &Outcome) -> Result<(), Error> {
        let (status, output, error) = match outcome {
            Outcome::Success(value) => (Status::Success, Some(value.as_str()), None),
            Outcome::Error(value) => (Status::Error, None, Some(value.as_str())),
        };
        self.check_size(&[output, error])?;
        if self.cancel_if_expired(&lease.workflow_id).await? {
            return Err(Error::WorkflowCancelled(lease.workflow_id.clone()));
        }
        let changed = self
            .attempt(|| async {
                let result = sqlx::query(self.sql(
                    "UPDATE workflow_status SET status = ?, output = ?, error = ?, lease_token = NULL, claim_after = NULL, completed_at = {NOW}, updated_at = {NOW} WHERE workflow_uuid = ? AND lease_token = ? AND status = 'PENDING' AND NOT EXISTS \
                     (SELECT 1 FROM workflow_timeouts WHERE workflow_timeouts.workflow_uuid = workflow_status.workflow_uuid AND deadline_epoch_ms <= {NOW})",
                ))
                .bind(status.as_str())
                .bind(output)
                .bind(error)
                .bind(lease.workflow_id.as_str())
                .bind(lease.token.as_str())
                .execute(&self.pool)
                .await?;
                Ok(result.rows_affected())
            })
            .await?;
        if changed == 0 && self.cancel_if_expired(&lease.workflow_id).await? {
            return Err(Error::WorkflowCancelled(lease.workflow_id.clone()));
        }
        require_owner(changed, lease)
    }
}
