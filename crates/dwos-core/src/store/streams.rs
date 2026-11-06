use sqlx::Row;

use super::events::{StoredValue, recorded_send};
use super::{SendOutcome, Store, check_name};
use crate::Error;
use crate::workflow::{Lease, Status, StepRecord};

pub const WRITE_STREAM_STEP: &str = "DBOS.writeStream";
pub const CLOSE_STREAM_STEP: &str = "DBOS.closeStream";

/// One offset of a stream, as seen now.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StreamRead {
    Value(StoredValue),
    /// Closed at or before this offset, or the workflow ended or does not exist.
    Closed,
    /// Nothing there yet and the workflow can still write. From a timed wait, the timeout passed.
    Waiting,
}

impl Store {
    /// Appends at the next offset, starting at 0. `value` `None` closes the stream.
    /// With `step_id` the write is recorded once; from a step it may repeat on retry.
    pub async fn write_stream(
        &self,
        lease: &Lease,
        step_id: Option<u32>,
        key: &str,
        value: Option<&str>,
        serialization: &str,
    ) -> Result<SendOutcome, Error> {
        check_name("stream key", key)?;
        self.check_size(&[value])?;
        let step_name = if value.is_some() {
            WRITE_STREAM_STEP
        } else {
            CLOSE_STREAM_STEP
        };
        self.attempt(|| async {
            let (mut tx, _) = self.owned_transaction(lease).await?;
            if let Some(step_id) = step_id {
                let (next, recorded) = self.read_for_write(tx, lease, step_id).await?;
                tx = next;
                if let Some(recorded) = recorded {
                    let outcome = recorded_send(&lease.workflow_id, step_id, step_name, &recorded)?;
                    tx.commit().await?;
                    return Ok(outcome);
                }
            } else {
                tx = self.live_transaction(tx, lease).await?;
            }
            // The workflow row is locked above, so offsets for one workflow are taken one at a time.
            let last: Option<i64> = sqlx::query_scalar(self.sql(
                "SELECT MAX(stream_offset) FROM workflow_streams WHERE workflow_uuid = ? AND stream_key = ?",
            ))
            .bind(lease.workflow_id.as_str())
            .bind(key)
            .fetch_one(&mut *tx)
            .await?;
            let offset = last.map_or(Some(0), |last| last.checked_add(1))
                .ok_or_else(|| Error::Corrupt("stream offset overflow".to_owned()))?;
            sqlx::query(self.sql(
                "INSERT INTO workflow_streams (workflow_uuid, stream_key, stream_offset, value, serialization) VALUES (?, ?, ?, ?, ?)",
            ))
            .bind(lease.workflow_id.as_str())
            .bind(key)
            .bind(offset)
            .bind(value)
            .bind(serialization)
            .execute(&mut *tx)
            .await?;
            if let Some(step_id) = step_id {
                self.insert_step(&mut tx, &lease.workflow_id, step_id, step_name, &StepRecord::default(), serialization)
                    .await?;
            }
            tx.commit().await?;
            Ok(SendOutcome::Sent)
        })
        .await
    }

    pub async fn read_stream(
        &self,
        workflow_id: &str,
        key: &str,
        offset: i64,
    ) -> Result<StreamRead, Error> {
        check_name("workflow ID", workflow_id)?;
        check_name("stream key", key)?;
        if offset < 0 {
            return Err(Error::InvalidConfig(
                "stream offset must not be negative".to_owned(),
            ));
        }
        self.attempt(|| async {
            // Status first: a write commits before the workflow ends, so an ended status read here
            // means the row read below sees every value.
            let status: Option<String> = sqlx::query_scalar(
                self.sql("SELECT status FROM workflow_status WHERE workflow_uuid = ?"),
            )
            .bind(workflow_id)
            .fetch_optional(&self.pool)
            .await?;
            let Some(status) = status else {
                return Ok(StreamRead::Closed);
            };
            let row = sqlx::query(self.sql(
                "SELECT stream_offset, value, serialization FROM workflow_streams \
                 WHERE workflow_uuid = ? AND stream_key = ? AND (stream_offset = ? OR (value IS NULL AND stream_offset < ?)) \
                 ORDER BY stream_offset LIMIT 1",
            ))
            .bind(workflow_id)
            .bind(key)
            .bind(offset)
            .bind(offset)
            .fetch_optional(&self.pool)
            .await?;
            if let Some(row) = row {
                return Ok(match row.try_get::<Option<String>, _>("value")? {
                    Some(text) => StreamRead::Value(StoredValue {
                        text,
                        serialization: row.try_get("serialization")?,
                    }),
                    None => StreamRead::Closed,
                });
            }
            Ok(match Status::parse(&status)? {
                Status::Pending | Status::Enqueued | Status::Delayed => StreamRead::Waiting,
                _ => StreamRead::Closed,
            })
        })
        .await
    }
}
