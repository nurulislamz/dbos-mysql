use sqlx::Row;

use super::{SELECT_WORKFLOW, Store, check_name, millis, require_owner, workflow_from_row};
use crate::Error;
use crate::workflow::{
    Lease, NewWorkflow, Outcome, Started, Status, Workflow, check_workflow_name, new_token,
};

impl Store {
    pub async fn start_workflow(&self, new: &NewWorkflow<'_>) -> Result<Started, Error> {
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
            tx.commit().await?;
            return Ok(Started::Owned(Lease {
                workflow_id: new.workflow_id.to_owned(),
                token: token.to_owned(),
            }));
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
        let changed = self
            .attempt(|| async {
                let result = sqlx::query(self.sql(
                    "UPDATE workflow_status SET status = ?, output = ?, error = ?, lease_token = NULL, claim_after = NULL, completed_at = {NOW}, updated_at = {NOW} WHERE workflow_uuid = ? AND lease_token = ? AND status = 'PENDING'",
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
        require_owner(changed, lease)
    }

    pub async fn list_workflows(&self) -> Result<Vec<Workflow>, Error> {
        self.attempt(|| async {
            let rows = sqlx::query(
                "SELECT workflow_uuid, name, status, inputs, output, error, recovery_attempts, max_recovery_attempts, parent_workflow_id, created_at, updated_at, completed_at, serialization \
                 FROM workflow_status ORDER BY created_at, workflow_uuid",
            )
            .fetch_all(&self.pool)
            .await?;
            rows.iter().map(workflow_from_row).collect()
        })
        .await
    }
}
