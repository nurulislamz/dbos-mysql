use std::num::NonZeroU32;
use std::time::Duration;

use super::wakeup::WakeKey;
use super::{Begin, Engine};
use crate::{Claimed, Error, NewWorkflow, PartitionLimits, PartitionUpdate, WorkflowOptions};

impl Engine {
    pub async fn set_polling_interval(&self, name: &str, millis: i64) -> Result<(), Error> {
        self.store.set_polling_interval(name, millis).await
    }

    pub async fn get_polling_interval(&self, name: &str) -> Result<Option<i64>, Error> {
        self.store.get_polling_interval(name).await
    }
    pub async fn get_partition_limits(&self, name: &str) -> Result<PartitionLimits, Error> {
        self.store.get_partition_limits(name).await
    }

    pub async fn update_partition_limits(
        &self,
        name: &str,
        change: PartitionUpdate,
    ) -> Result<(), Error> {
        self.store.update_partition_limits(name, change).await
    }
    pub async fn set_limiter(
        &self,
        name: &str,
        limiter: Option<(NonZeroU32, i64)>,
    ) -> Result<(), Error> {
        self.store.set_limiter(name, limiter).await
    }

    pub async fn get_limiter(&self, name: &str) -> Result<Option<(u32, i64)>, Error> {
        self.store.get_limiter(name).await
    }
    pub async fn set_global_concurrency(
        &self,
        name: &str,
        limit: Option<NonZeroU32>,
    ) -> Result<(), Error> {
        self.store.set_global_concurrency(name, limit).await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn register_queue(
        &self,
        name: &str,
        global: Option<NonZeroU32>,
        worker: Option<NonZeroU32>,
        limiter: Option<(NonZeroU32, i64)>,
        partition: PartitionLimits,
        polling_ms: i64,
        update: bool,
    ) -> Result<(), Error> {
        self.store
            .register_queue(name, global, worker, limiter, partition, polling_ms, update)
            .await
    }

    pub async fn delete_queue(&self, name: &str) -> Result<(), Error> {
        self.store.delete_queue(name).await
    }

    pub async fn list_queues(&self) -> Result<Vec<(String, u32)>, Error> {
        self.store.list_queues().await
    }

    pub async fn list_queues_filtered(
        &self,
        applications: Option<&[String]>,
    ) -> Result<Vec<(String, u32)>, Error> {
        self.store.list_queues_filtered(applications).await
    }
    pub async fn set_worker_concurrency(
        &self,
        name: &str,
        limit: Option<NonZeroU32>,
    ) -> Result<(), Error> {
        self.store.set_worker_concurrency(name, limit).await
    }

    pub async fn get_worker_concurrency(&self, name: &str) -> Result<Option<u32>, Error> {
        self.store.get_worker_concurrency(name).await
    }
    pub async fn set_workflow_delay(
        &self,
        workflow_id: &str,
        delay_ms: Option<i64>,
        until_ms: Option<i64>,
    ) -> Result<(), Error> {
        self.store
            .set_workflow_delay(workflow_id, delay_ms, until_ms)
            .await
    }
    pub async fn get_queue(&self, name: &str) -> Result<Option<NonZeroU32>, Error> {
        self.store.get_queue(name).await
    }

    pub async fn ensure_queue(&self, name: &str, concurrency: NonZeroU32) -> Result<(), Error> {
        self.store.ensure_queue(name, concurrency).await
    }

    pub async fn ensure_queue_exists(&self, name: &str) -> Result<(), Error> {
        self.store.ensure_queue_exists(name).await
    }

    pub async fn enqueue(&self, queue: &str, begin: Begin<'_>) -> Result<String, Error> {
        self.enqueue_with(queue, begin, &WorkflowOptions::default())
            .await
    }

    pub async fn enqueue_with(
        &self,
        queue: &str,
        begin: Begin<'_>,
        options: &WorkflowOptions,
    ) -> Result<String, Error> {
        let (timeout_ms, deadline_epoch_ms) = self.timeout_deadline(&begin)?;
        let workflow_id = self.workflow_id_for_begin(&begin).await?;
        let enqueued = self
            .store
            .enqueue_workflow_with(
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
                options,
            )
            .await?;
        self.wake_queue(queue);
        Ok(enqueued)
    }

    /// For a freed slot, which only the SDK knows about, because it runs the workflow.
    pub fn wake_queue(&self, queue: &str) {
        self.wakeups.wake(&WakeKey::Queue(queue.to_owned()));
    }

    /// One claim. If it finds nothing, waits for a wake or for `interval` and returns nothing, so
    /// the SDK calls again with its current names. A failed read is logged and tried again.
    pub async fn wait_queue_claims(
        &self,
        queue: &str,
        names: &[String],
        interval: Duration,
    ) -> Vec<Claimed> {
        let waiter = self.wakeups.waiter(vec![WakeKey::Queue(queue.to_owned())]);
        match self.claim_queue(queue, names).await {
            Ok(claims) if !claims.is_empty() => return claims,
            Ok(_) => {}
            Err(error) => {
                log::error!("reading queue {queue} failed, and will be tried again: {error}");
            }
        }
        waiter.wait(interval).await;
        Vec::new()
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
