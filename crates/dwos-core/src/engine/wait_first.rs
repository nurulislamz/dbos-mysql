use std::collections::HashSet;
use std::time::Duration;

use super::{Engine, POLL_MAX, POLL_MIN, Status, WakeKey};
use crate::Error;

impl Engine {
    pub async fn await_first(
        &self,
        ids: &[String],
        interval: Option<Duration>,
    ) -> Result<String, Error> {
        if ids.is_empty() || ids.iter().collect::<HashSet<_>>().len() != ids.len() {
            return Err(Error::InvalidConfig(
                "workflow IDs must be nonempty and unique".into(),
            ));
        }
        let interval = interval.unwrap_or(POLL_MAX);
        if interval.is_zero() {
            return Err(Error::InvalidConfig(
                "polling_interval_sec must be positive".into(),
            ));
        }
        let waiter = self
            .wakeups
            .waiter(ids.iter().cloned().map(WakeKey::Workflow).collect());
        let mut delay = POLL_MIN.min(interval);
        loop {
            for id in ids {
                let status = match self.get_status(id).await {
                    Ok(workflow) => workflow.status,
                    Err(Error::WorkflowNotFound(_)) => continue,
                    Err(error) => return Err(error),
                };
                if !matches!(status, Status::Pending | Status::Enqueued | Status::Delayed) {
                    return Ok(id.clone());
                }
            }
            waiter.wait(delay).await;
            delay = (delay * 2).min(interval);
        }
    }
}
