use std::time::Duration;

use tokio::time::Instant;

use super::wakeup::WakeKey;
use super::{Engine, POLL_MAX, POLL_MIN, Run};
use crate::Error;
use crate::store::{SendOutcome, StreamRead};
use crate::workflow::PORTABLE_JSON;

impl Engine {
    /// `step_id` from a workflow records the write once. `None` from a step writes on every try.
    pub async fn write_stream(
        &self,
        run: &Run,
        step_id: Option<u32>,
        key: &str,
        value: &str,
        serialization: &str,
    ) -> Result<SendOutcome, Error> {
        self.append_stream(run, step_id, key, Some(value), serialization)
            .await
    }

    pub async fn close_stream(
        &self,
        run: &Run,
        step_id: Option<u32>,
        key: &str,
    ) -> Result<SendOutcome, Error> {
        self.append_stream(run, step_id, key, None, PORTABLE_JSON)
            .await
    }

    async fn append_stream(
        &self,
        run: &Run,
        step_id: Option<u32>,
        key: &str,
        value: Option<&str>,
        serialization: &str,
    ) -> Result<SendOutcome, Error> {
        let outcome = self
            .store
            .write_stream(&run.lease, step_id, key, value, serialization)
            .await?;
        self.wakeups.wake(&WakeKey::Stream(
            run.workflow_id().to_owned(),
            key.to_owned(),
        ));
        Ok(outcome)
    }

    /// Waits for the value at `offset`. Returns `Waiting` only when `timeout` passes first.
    /// A local write or the workflow's end wakes it; `interval` caps the poll for other processes.
    pub async fn read_stream(
        &self,
        workflow_id: &str,
        key: &str,
        offset: i64,
        timeout: Option<Duration>,
        interval: Option<Duration>,
    ) -> Result<StreamRead, Error> {
        let interval = interval.unwrap_or(POLL_MAX);
        let waiter = self.wakeups.waiter(vec![
            WakeKey::Stream(workflow_id.to_owned(), key.to_owned()),
            WakeKey::Workflow(workflow_id.to_owned()),
        ]);
        let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
        let mut delay = POLL_MIN.min(interval);
        loop {
            let read = self.store.read_stream(workflow_id, key, offset).await?;
            if read != StreamRead::Waiting {
                return Ok(read);
            }
            let mut wait = delay;
            if let Some(deadline) = deadline {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Ok(StreamRead::Waiting);
                }
                wait = wait.min(left);
            }
            if self.is_closing() {
                return Err(Error::ShuttingDown);
            }
            waiter.wait(wait).await;
            delay = (delay * 2).min(interval);
        }
    }
}
