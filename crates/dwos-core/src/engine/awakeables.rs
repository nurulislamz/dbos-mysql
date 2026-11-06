use super::events::send_from;
use super::wakeup::WakeKey;
use super::{Engine, POLL_MIN, Run};
use crate::Error;
use crate::store::{Poll, StoredValue};

impl Engine {
    pub async fn create_awakeable(&self, run: &Run, step_id: u32) -> Result<String, Error> {
        self.store.create_awakeable(&run.lease, step_id).await
    }

    /// `poll_awakeable` until a completion arrives or the recorded timeout passes, then `null`.
    pub async fn wait_awakeable(
        &self,
        run: &Run,
        step_id: u32,
        timeout_step_id: u32,
        awakeable_id: &str,
        timeout_ms: i64,
    ) -> Result<StoredValue, Error> {
        let waiter = self.wakeups.waiter(vec![
            WakeKey::Workflow(run.workflow_id().to_owned()),
            WakeKey::Awakeable(awakeable_id.to_owned()),
        ]);
        let mut delay = POLL_MIN;
        loop {
            match self
                .store
                .poll_awakeable(
                    &run.lease,
                    step_id,
                    timeout_step_id,
                    awakeable_id,
                    timeout_ms,
                )
                .await?
            {
                Poll::Ready(value) => return Ok(value),
                Poll::Waiting { remaining_ms, .. } => {
                    self.wait_turn(&waiter, &mut delay, remaining_ms).await?;
                }
            }
        }
    }

    /// No run from outside any workflow. A workflow passes `step_id` so replay returns the same answer.
    pub async fn complete_awakeable(
        &self,
        awakeable_id: &str,
        value: &str,
        serialization: &str,
        run: Option<&Run>,
        step_id: Option<u32>,
    ) -> Result<bool, Error> {
        let stored = self
            .store
            .complete_awakeable(awakeable_id, value, serialization, send_from(run, step_id)?)
            .await?;
        if stored {
            self.wakeups
                .wake(&WakeKey::Awakeable(awakeable_id.to_owned()));
        }
        Ok(stored)
    }
}
