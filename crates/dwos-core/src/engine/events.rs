use super::{Engine, Run};
use crate::Error;
use crate::store::{EventRow, Poll, SendFrom, SendOutcome};

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub async fn send(
        &self,
        destination: &str,
        topic: Option<&str>,
        message: &str,
        serialization: &str,
        idempotency_key: Option<&str>,
        run: Option<&Run>,
        step_id: Option<u32>,
    ) -> Result<SendOutcome, Error> {
        let from = match (run, step_id) {
            (Some(run), Some(step_id)) => SendFrom::Workflow {
                lease: &run.lease,
                step_id,
            },
            (Some(run), None) => SendFrom::Step(&run.lease),
            (None, None) => SendFrom::Outside,
            (None, Some(_)) => {
                return Err(Error::InvalidConfig(
                    "a send step ID requires a workflow run".to_owned(),
                ));
            }
        };
        self.store
            .send(
                destination,
                topic,
                message,
                serialization,
                idempotency_key,
                from,
            )
            .await
    }

    pub async fn poll_recv(
        &self,
        run: &Run,
        step_id: u32,
        timeout_step_id: u32,
        topic: Option<&str>,
        timeout_ms: i64,
    ) -> Result<Poll, Error> {
        self.store
            .poll_recv(&run.lease, step_id, timeout_step_id, topic, timeout_ms)
            .await
    }

    pub async fn poll_event(
        &self,
        target: &str,
        key: &str,
        timeout_ms: i64,
        deadline_ms: Option<i64>,
        caller: Option<(&Run, u32, u32)>,
    ) -> Result<Poll, Error> {
        let caller =
            caller.map(|(run, step_id, timeout_step_id)| (&run.lease, step_id, timeout_step_id));
        self.store
            .poll_event(target, key, timeout_ms, deadline_ms, caller)
            .await
    }

    pub async fn set_event(
        &self,
        run: &Run,
        step_id: Option<u32>,
        key: &str,
        value: &str,
        serialization: &str,
    ) -> Result<SendOutcome, Error> {
        self.store
            .set_event(&run.lease, step_id, key, value, serialization)
            .await
    }

    pub async fn list_events(&self, workflow_id: &str) -> Result<Vec<EventRow>, Error> {
        self.store.list_events(workflow_id).await
    }
}
