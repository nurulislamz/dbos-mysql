use sqlx::Row;

use super::events::{JSON_NULL, check_wait_steps, recorded_value};
use super::{Poll, SendFrom, Store, StoredValue, check_name};
use crate::Error;
use crate::workflow::{Lease, PORTABLE_JSON, StepRecord, new_token};

pub const CREATE_AWAKEABLE_STEP: &str = "DBOS.createAwakeable";
pub const AWAIT_AWAKEABLE_STEP: &str = "DBOS.awaitAwakeable";
pub const COMPLETE_AWAKEABLE_STEP: &str = "DBOS.completeAwakeable";

fn null() -> StoredValue {
    StoredValue {
        text: JSON_NULL.to_owned(),
        serialization: PORTABLE_JSON.to_owned(),
    }
}

// An awakeable is one `workflow_awakeables` row: open while `completed_at_epoch_ms` is NULL, then
// settled once, by the first completion or by the wait's timeout (value NULL).
impl Store {
    /// Records a new open awakeable for this workflow. Replay returns the recorded ID.
    pub async fn create_awakeable(&self, lease: &Lease, step_id: u32) -> Result<String, Error> {
        let id = new_token();
        self.attempt(|| async {
            let (tx, _) = self.owned_transaction(lease).await?;
            let (mut tx, recorded) = self.read_for_write(tx, lease, step_id).await?;
            if let Some(recorded) = recorded {
                let saved = recorded_value(
                    &lease.workflow_id,
                    step_id,
                    CREATE_AWAKEABLE_STEP,
                    &recorded,
                )?;
                tx.commit().await?;
                return serde_json::from_str(&saved.text).map_err(|_| {
                    Error::Corrupt(format!(
                        "workflow {} step {step_id} has no awakeable ID",
                        lease.workflow_id
                    ))
                });
            }
            sqlx::query(self.sql(
                "INSERT INTO workflow_awakeables (awakeable_id, workflow_uuid) VALUES (?, ?)",
            ))
            .bind(id.as_str())
            .bind(lease.workflow_id.as_str())
            .execute(&mut *tx)
            .await?;
            let output = serde_json::Value::from(id.as_str()).to_string();
            self.insert_step(
                &mut tx,
                &lease.workflow_id,
                step_id,
                CREATE_AWAKEABLE_STEP,
                &StepRecord {
                    output: Some(output),
                    ..StepRecord::default()
                },
                PORTABLE_JSON,
            )
            .await?;
            tx.commit().await?;
            Ok(id.clone())
        })
        .await
    }

    /// The completion, or `null` once the deadline recorded at `timeout_step_id` passes. Both are
    /// recorded, so replay never waits again. A timeout settles the row in the same transaction,
    /// so every completion that `complete_awakeable` accepted is one a wait returns.
    pub async fn poll_awakeable(
        &self,
        lease: &Lease,
        step_id: u32,
        timeout_step_id: u32,
        awakeable_id: &str,
        timeout_ms: i64,
    ) -> Result<Poll, Error> {
        check_wait_steps(step_id, timeout_step_id)?;
        check_name("awakeable ID", awakeable_id)?;
        let timeout_ms = timeout_ms.max(0);
        self.attempt(|| async {
            let (tx, _) = self.owned_transaction(lease).await?;
            let (mut tx, recorded) = self.read_for_write(tx, lease, step_id).await?;
            if let Some(recorded) = recorded {
                let saved =
                    recorded_value(&lease.workflow_id, step_id, AWAIT_AWAKEABLE_STEP, &recorded)?;
                tx.commit().await?;
                return Ok(Poll::Ready(saved));
            }
            let row = sqlx::query(self.sql(&format!(
                "SELECT completed_at_epoch_ms, value, serialization FROM workflow_awakeables WHERE awakeable_id = ? {}",
                self.dialect.lock_row()
            )))
            .bind(awakeable_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| Error::AwakeableNotFound(awakeable_id.to_owned()))?;
            let (deadline_ms, _) = self
                .ensure_deadline(&mut tx, lease, timeout_step_id, timeout_ms)
                .await?;
            let now_ms: i64 = sqlx::query_scalar(self.sql("SELECT {NOW}"))
                .fetch_one(&mut *tx)
                .await?;
            let value = if row.try_get::<Option<i64>, _>("completed_at_epoch_ms")?.is_some() {
                match row.try_get::<Option<String>, _>("value")? {
                    Some(text) => StoredValue {
                        text,
                        serialization: row.try_get::<Option<String>, _>("serialization")?.ok_or_else(|| {
                            Error::Corrupt(format!("awakeable {awakeable_id} has no stored serialization"))
                        })?,
                    },
                    None => null(),
                }
            } else if now_ms >= deadline_ms {
                sqlx::query(self.sql(
                    "UPDATE workflow_awakeables SET completed_at_epoch_ms = {NOW} WHERE awakeable_id = ?",
                ))
                .bind(awakeable_id)
                .execute(&mut *tx)
                .await?;
                null()
            } else {
                tx.commit().await?;
                return Ok(Poll::Waiting {
                    remaining_ms: deadline_ms - now_ms,
                    deadline_ms,
                });
            };
            self.insert_step(
                &mut tx,
                &lease.workflow_id,
                step_id,
                AWAIT_AWAKEABLE_STEP,
                &StepRecord {
                    output: Some(value.text.clone()),
                    ..StepRecord::default()
                },
                &value.serialization,
            )
            .await?;
            tx.commit().await?;
            Ok(Poll::Ready(value))
        })
        .await
    }

    /// Stores the first completion and returns true. Returns false when the awakeable is already
    /// completed or timed out. From a workflow the answer is recorded, so replay returns it again.
    pub async fn complete_awakeable(
        &self,
        awakeable_id: &str,
        value: &str,
        serialization: &str,
        from: SendFrom<'_>,
    ) -> Result<bool, Error> {
        check_name("awakeable ID", awakeable_id)?;
        self.check_size(&[Some(value)])?;
        self.attempt(|| async {
            let mut tx = match from {
                SendFrom::Outside => self.begin().await?,
                SendFrom::Step(lease) | SendFrom::Workflow { lease, .. } => {
                    self.owned_transaction(lease).await?.0
                }
            };
            match from {
                SendFrom::Workflow { lease, step_id } => {
                    let (next, recorded) = self.read_for_write(tx, lease, step_id).await?;
                    tx = next;
                    if let Some(recorded) = recorded {
                        let saved = recorded_value(
                            &lease.workflow_id,
                            step_id,
                            COMPLETE_AWAKEABLE_STEP,
                            &recorded,
                        )?;
                        tx.commit().await?;
                        return Ok(saved.text == "true");
                    }
                }
                SendFrom::Step(lease) => tx = self.live_transaction(tx, lease).await?,
                SendFrom::Outside => {}
            }
            let completed_at: Option<i64> = sqlx::query_scalar(self.sql(&format!(
                "SELECT completed_at_epoch_ms FROM workflow_awakeables WHERE awakeable_id = ? {}",
                self.dialect.lock_row()
            )))
            .bind(awakeable_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| Error::AwakeableNotFound(awakeable_id.to_owned()))?;
            let stored = completed_at.is_none();
            if stored {
                sqlx::query(self.sql(
                    "UPDATE workflow_awakeables SET completed_at_epoch_ms = {NOW}, value = ?, serialization = ? WHERE awakeable_id = ?",
                ))
                .bind(value)
                .bind(serialization)
                .bind(awakeable_id)
                .execute(&mut *tx)
                .await?;
            }
            if let SendFrom::Workflow { lease, step_id } = from {
                self.insert_step(
                    &mut tx,
                    &lease.workflow_id,
                    step_id,
                    COMPLETE_AWAKEABLE_STEP,
                    &StepRecord {
                        output: Some(stored.to_string()),
                        ..StepRecord::default()
                    },
                    PORTABLE_JSON,
                )
                .await?;
            }
            tx.commit().await?;
            Ok(stored)
        })
        .await
    }
}
