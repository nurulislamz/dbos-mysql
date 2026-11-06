use sqlx::Row;

use super::events::recorded_send;
use super::{SendFrom, SendOutcome, Store};
use crate::{Error, StepRecord};

const STEP: &str = "DBOS.updateWorkflowAttributes";

impl Store {
    pub async fn update_workflow_attributes(
        &self,
        workflow_id: &str,
        attributes: Option<&str>,
        merge: bool,
        from: SendFrom<'_>,
        serialization: &str,
    ) -> Result<SendOutcome, Error> {
        let value: Option<serde_json::Value> = attributes
            .map(serde_json::from_str)
            .transpose()
            .map_err(|error| {
                Error::InvalidConfig(format!("invalid workflow attributes: {error}"))
            })?;
        if value.as_ref().is_some_and(|value| !value.is_object()) || (merge && value.is_none()) {
            return Err(Error::InvalidConfig(
                "workflow attributes must be an object; merge requires an object".into(),
            ));
        }
        self.attempt(|| async {
            let mut tx = match from {
                SendFrom::Outside => self.begin().await?,
                SendFrom::Step(lease) | SendFrom::Workflow { lease, .. } => self.owned_transaction(lease).await?.0,
            };
            match from {
                SendFrom::Workflow { lease, step_id } => {
                    let (next, recorded) = self.read_for_write(tx, lease, step_id).await?;
                    tx = next;
                    if let Some(recorded) = recorded {
                        let outcome = recorded_send(&lease.workflow_id, step_id, STEP, &recorded)?;
                        tx.commit().await?;
                        return Ok(outcome);
                    }
                }
                SendFrom::Step(lease) => tx = self.live_transaction(tx, lease).await?,
                SendFrom::Outside => {}
            }
            let row = sqlx::query(self.sql(&format!("SELECT attributes FROM workflow_status WHERE workflow_uuid = ? {}", self.dialect.lock_row())))
                .bind(workflow_id).fetch_optional(&mut *tx).await?;
            if let Some(row) = row {
                let text = if merge {
                    let current: Option<String> = row.try_get("attributes")?;
                    let mut current = match current {
                        Some(text) => serde_json::from_str::<serde_json::Value>(&text).map_err(|error| Error::Corrupt(format!("invalid workflow attributes: {error}")))?,
                        None => serde_json::json!({}),
                    };
                    let object = current.as_object_mut().ok_or_else(|| Error::Corrupt("workflow attributes are not an object".into()))?;
                    object.extend(value.as_ref().unwrap().as_object().unwrap().clone());
                    Some(current.to_string())
                } else {
                    attributes.map(str::to_owned)
                };
                sqlx::query(self.sql("UPDATE workflow_status SET attributes = ?, updated_at = {NOW} WHERE workflow_uuid = ?"))
                    .bind(text.as_deref()).bind(workflow_id).execute(&mut *tx).await?;
            }
            if let SendFrom::Workflow { lease, step_id } = from {
                self.insert_step(&mut tx, &lease.workflow_id, step_id, STEP, &StepRecord::default(), serialization).await?;
            }
            tx.commit().await?;
            Ok(SendOutcome::Sent)
        }).await
    }
}
