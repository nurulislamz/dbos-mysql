use std::num::NonZeroU32;

use super::{Begin, Engine};
use crate::{Claimed, Error, NewWorkflow};

impl Engine {
    pub async fn ensure_queue(&self, name: &str, concurrency: NonZeroU32) -> Result<(), Error> {
        self.store.ensure_queue(name, concurrency).await
    }

    pub async fn enqueue(&self, queue: &str, begin: Begin<'_>) -> Result<String, Error> {
        let (timeout_ms, deadline_epoch_ms) = self.timeout_deadline(&begin)?;
        let workflow_id = self.workflow_id_for_begin(&begin).await?;
        self.store
            .enqueue_workflow(
                queue,
                &NewWorkflow {
                    workflow_id: &workflow_id,
                    name: begin.name,
                    input: begin.input,
                    max_recovery_attempts: begin.max_recovery_attempts,
                    lease: self.config.lease,
                    parent_workflow_id: begin.parent.map(|parent| parent.run.workflow_id()),
                    serialization: self.serialization(),
                    timeout_ms,
                    deadline_epoch_ms,
                },
            )
            .await
    }

    pub async fn claim_queue(&self, queue: &str, names: &[String]) -> Result<Vec<Claimed>, Error> {
        let free = usize::try_from(self.config.max_concurrent_recoveries)
            .unwrap_or(usize::MAX)
            .saturating_sub(self.active_runs());
        self.store
            .claim_queue(
                queue,
                names,
                u32::try_from(free).unwrap_or(u32::MAX),
                self.config.lease,
            )
            .await
    }
}
