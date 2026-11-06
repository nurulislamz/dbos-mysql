use super::{Begin, Engine};
use crate::{Debounce, Error, NewWorkflow, WorkflowOptions};

impl Engine {
    /// DBOS's debounce: calls with the same key coalesce into one delayed workflow that runs with
    /// the last call's inputs, `period_ms` after the last call and never later than `timeout_ms`
    /// after the first. Inside a workflow the result is a saved step, so a replay returns it.
    pub async fn debounce(
        &self,
        queue: &str,
        begin: Begin<'_>,
        options: &WorkflowOptions,
        debounce_key: &str,
        period_ms: i64,
        timeout_ms: Option<i64>,
    ) -> Result<String, Error> {
        // A long debounce delay could outlast the caller's deadline, so the workflow does not inherit it.
        let begin = Begin {
            ignore_propagated_deadline: true,
            ..begin
        };
        let (workflow_timeout_ms, deadline_epoch_ms) = self.timeout_deadline(&begin)?;
        let workflow_id = match (begin.workflow_id.filter(|id| !id.is_empty()), begin.parent) {
            (Some(id), _) => id.to_owned(),
            (None, Some(parent)) => format!("{}-{}", parent.run.workflow_id(), parent.step_id),
            (None, None) => crate::workflow::new_token(),
        };
        let format = self.format_for(options)?;
        let key = format!("{}-{debounce_key}", begin.name);
        let debounced = self
            .store
            .debounce_workflow(
                queue,
                &NewWorkflow {
                    workflow_id: &workflow_id,
                    name: begin.name,
                    input: begin.input,
                    max_recovery_attempts: begin.max_recovery_attempts,
                    lease: self.config.lease,
                    parent_workflow_id: begin.parent.map(|parent| parent.run.workflow_id()),
                    serialization: &format,
                    timeout_ms: workflow_timeout_ms,
                    deadline_epoch_ms,
                },
                options,
                &Debounce {
                    key: &key,
                    period_ms,
                    timeout_ms,
                },
                begin
                    .parent
                    .map(|parent| (&parent.run.lease, parent.step_id)),
            )
            .await?;
        self.wake_queue(queue);
        Ok(debounced)
    }
}
