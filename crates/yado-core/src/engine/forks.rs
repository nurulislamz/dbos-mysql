use super::{Engine, Run};
use crate::Error;
use crate::store::{ForkOptions, SendOutcome};
use crate::workflow::{Claimed, new_token};

impl Engine {
    /// Starts a new workflow from `start_step` of `workflow_id`: steps before it replay, the rest run.
    /// Returns the fork's ID, and its claim when it runs here. With no `new_id` the ID is random.
    /// On a queue, admission starts it. Otherwise this process claims it if it knows the name,
    /// and recovery elsewhere takes it if not.
    pub async fn fork_workflow(
        &self,
        workflow_id: &str,
        start_step: u32,
        new_id: Option<&str>,
        names: &[String],
        options: &ForkOptions,
    ) -> Result<(String, Option<Claimed>), Error> {
        let fork_id = new_id
            .filter(|id| !id.is_empty())
            .map_or_else(new_token, str::to_owned);
        self.store
            .fork_workflow(workflow_id, &fork_id, start_step, options)
            .await?;
        let claim = self
            .run_again(&fork_id, names, options.queue_name.as_deref())
            .await?;
        Ok((fork_id, claim))
    }

    /// Runs a finished workflow again from `start_step`, default 1, under its own ID. See
    /// [`crate::Store::rewind_workflow`] for what is discarded.
    pub async fn rewind_workflow(
        &self,
        workflow_id: &str,
        start_step: Option<u32>,
        names: &[String],
        application_version: Option<&str>,
        queue_name: Option<&str>,
        queue_partition_key: Option<&str>,
    ) -> Result<Option<Claimed>, Error> {
        self.store
            .rewind_workflow(
                workflow_id,
                start_step.unwrap_or(1),
                application_version,
                queue_name,
                queue_partition_key,
            )
            .await?;
        self.run_again(workflow_id, names, queue_name).await
    }

    async fn run_again(
        &self,
        workflow_id: &str,
        names: &[String],
        queue: Option<&str>,
    ) -> Result<Option<Claimed>, Error> {
        if let Some(queue) = queue {
            self.wake_queue(queue);
            return Ok(None);
        }
        self.store
            .claim_one(workflow_id, names, self.config.lease)
            .await
    }

    /// `send_bulk`, with each message also sent to every workflow forked from its destination,
    /// and forks of those, as they are at send time. It is one step and one transaction.
    pub async fn send_bulk_to_forks(
        &self,
        messages: &[(String, Option<String>, String, Option<String>)],
        serialization: &str,
        run: Option<&Run>,
        step_id: Option<u32>,
    ) -> Result<SendOutcome, Error> {
        let mut all = Vec::with_capacity(messages.len());
        for message in messages {
            all.push(message.clone());
            for fork in self.store.fork_descendants(&message.0).await? {
                all.push((
                    fork,
                    message.1.clone(),
                    message.2.clone(),
                    message.3.clone(),
                ));
            }
        }
        self.send_bulk(&all, serialization, run, step_id).await
    }
}
