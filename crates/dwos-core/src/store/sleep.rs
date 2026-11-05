use std::time::Duration;

use super::Store;
use crate::Error;
use crate::workflow::{Lease, PORTABLE_JSON, StepRecord, check_step_name};

pub(super) const SLEEP_STEP_NAME: &str = "DBOS.sleep";

fn seconds_to_ms(seconds: f64) -> i64 {
    let ms = seconds * 1000.0;
    if !ms.is_finite() || ms >= i64::MAX as f64 {
        i64::MAX
    } else if ms <= 0.0 {
        0
    } else {
        ms.round() as i64
    }
}

fn remaining(wake_ms: i64, now_ms: i64, deadline_epoch_ms: Option<i64>) -> Duration {
    let mut left = wake_ms.saturating_sub(now_ms).max(0);
    if let Some(deadline) = deadline_epoch_ms {
        left = left.min(deadline.saturating_sub(now_ms).max(0));
    }
    Duration::from_millis(left as u64)
}

impl Store {
    pub async fn record_sleep(
        &self,
        lease: &Lease,
        step_id: u32,
        seconds: f64,
    ) -> Result<Duration, Error> {
        if !seconds.is_finite() {
            return Err(Error::InvalidConfig(
                "sleep duration must be finite".to_owned(),
            ));
        }
        if seconds <= 0.0 {
            return Ok(Duration::ZERO);
        }
        self.attempt(|| self.record_sleep_once(lease, step_id, seconds))
            .await
    }

    /// The same test as `renew_lease`, without the write.
    pub async fn holds_lease(&self, lease: &Lease) -> Result<bool, Error> {
        let row = sqlx::query(self.sql(
            "SELECT 1 FROM workflow_status \
             WHERE workflow_uuid = ? AND lease_token = ? AND status = 'PENDING'",
        ))
        .bind(lease.workflow_id.as_str())
        .bind(lease.token.as_str())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    async fn record_sleep_once(
        &self,
        lease: &Lease,
        step_id: u32,
        seconds: f64,
    ) -> Result<Duration, Error> {
        let (mut tx, _) = self.owned_transaction(lease).await?;
        let stored_deadline: Option<i64> = sqlx::query_scalar(
            self.sql("SELECT deadline_epoch_ms FROM workflow_timeouts WHERE workflow_uuid = ?"),
        )
        .bind(lease.workflow_id.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        let now_ms: i64 = sqlx::query_scalar(self.sql("SELECT {NOW}"))
            .fetch_one(&mut *tx)
            .await?;
        let deadline = stored_deadline;
        if let Some((recorded_name, record)) =
            self.read_step(&mut tx, &lease.workflow_id, step_id).await?
        {
            check_step_name(&lease.workflow_id, step_id, SLEEP_STEP_NAME, recorded_name)?;
            let wake_ms = parse_wake_ms(&lease.workflow_id, step_id, &record)?;
            tx.commit().await?;
            return Ok(remaining(wake_ms, now_ms, deadline));
        }

        tx = self.live_transaction(tx, lease).await?;
        let wake_ms = now_ms.saturating_add(seconds_to_ms(seconds));
        let output = wake_ms.to_string();
        sqlx::query(self.sql(
            "INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, output, error, child_workflow_id, serialization, completed_at_epoch_ms) \
             VALUES (?, ?, ?, ?, NULL, NULL, ?, ?)",
        ))
        .bind(lease.workflow_id.as_str())
        .bind(i64::from(step_id))
        .bind(SLEEP_STEP_NAME)
        .bind(output.as_str())
        .bind(PORTABLE_JSON)
        .bind(wake_ms)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(remaining(wake_ms, now_ms, deadline))
    }
}

pub(super) fn parse_wake_ms(
    workflow_id: &str,
    step_id: u32,
    record: &StepRecord,
) -> Result<i64, Error> {
    if record.error.is_some() || record.child_workflow_id.is_some() {
        return Err(Error::StepKindMismatch {
            workflow_id: workflow_id.to_owned(),
            step_id,
            expected: "sleep",
        });
    }
    let Some(output) = &record.output else {
        return Err(Error::StepKindMismatch {
            workflow_id: workflow_id.to_owned(),
            step_id,
            expected: "sleep",
        });
    };
    output.parse().map_err(|_| {
        Error::Corrupt(format!(
            "workflow {workflow_id} step {step_id} has a sleep wake time that is not an integer"
        ))
    })
}
