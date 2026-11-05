use std::time::Duration;

use super::wakeup::{Waiter, WakeKey};
use super::{Engine, POLL_MAX, POLL_MIN, Run};
use crate::Error;
use crate::store::{EventRow, Poll, SendFrom, SendOutcome, StoredValue};

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
        let outcome = self
            .store
            .send(
                destination,
                topic,
                message,
                serialization,
                idempotency_key,
                from,
            )
            .await?;
        self.wakeups
            .wake(&WakeKey::Workflow(destination.to_owned()));
        Ok(outcome)
    }

    pub async fn send_bulk(
        &self,
        messages: &[(String, Option<String>, String, Option<String>)],
        serialization: &str,
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
                    "a send step ID requires a workflow run".into(),
                ));
            }
        };
        let outcome = self.store.send_bulk(messages, serialization, from).await?;
        for (destination, _, _, _) in messages {
            self.wakeups.wake(&WakeKey::Workflow(destination.clone()));
        }
        Ok(outcome)
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

    /// `poll_recv` until a message arrives or the recorded timeout passes.
    pub async fn wait_recv(
        &self,
        run: &Run,
        step_id: u32,
        timeout_step_id: u32,
        topic: Option<&str>,
        timeout_ms: i64,
    ) -> Result<StoredValue, Error> {
        let waiter = self
            .wakeups
            .waiter(vec![WakeKey::Workflow(run.workflow_id().to_owned())]);
        let mut delay = POLL_MIN;
        loop {
            match self
                .poll_recv(run, step_id, timeout_step_id, topic, timeout_ms)
                .await?
            {
                Poll::Ready(value) => return Ok(value),
                Poll::Waiting { remaining_ms, .. } => {
                    self.wait_turn(&waiter, &mut delay, remaining_ms).await?;
                }
            }
        }
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

    /// `poll_event` until the event arrives or the timeout passes.
    pub async fn wait_event(
        &self,
        target: &str,
        key: &str,
        timeout_ms: i64,
        caller: Option<(&Run, u32, u32)>,
    ) -> Result<StoredValue, Error> {
        let mut keys = vec![WakeKey::Event(target.to_owned(), key.to_owned())];
        if let Some((run, _, _)) = caller {
            keys.push(WakeKey::Workflow(run.workflow_id().to_owned()));
        }
        let waiter = self.wakeups.waiter(keys);
        let (mut delay, mut known_deadline_ms) = (POLL_MIN, None);
        loop {
            match self
                .poll_event(target, key, timeout_ms, known_deadline_ms, caller)
                .await?
            {
                Poll::Ready(value) => return Ok(value),
                Poll::Waiting {
                    remaining_ms,
                    deadline_ms,
                } => {
                    known_deadline_ms = Some(deadline_ms);
                    self.wait_turn(&waiter, &mut delay, remaining_ms).await?;
                }
            }
        }
    }

    // shortcut: each waiter on another process re-reads the database every 250ms, so very many waiters mean many reads; upgrade with a single per-engine watcher if that load shows up.
    async fn wait_turn(
        &self,
        waiter: &Waiter,
        delay: &mut Duration,
        remaining_ms: i64,
    ) -> Result<(), Error> {
        if self.is_closing() {
            return Err(Error::ShuttingDown);
        }
        let remaining = Duration::from_millis(u64::try_from(remaining_ms).unwrap_or(0));
        waiter.wait((*delay).min(remaining)).await;
        *delay = (*delay * 2).min(POLL_MAX);
        Ok(())
    }

    pub async fn set_event(
        &self,
        run: &Run,
        step_id: Option<u32>,
        key: &str,
        value: &str,
        serialization: &str,
    ) -> Result<SendOutcome, Error> {
        let outcome = self
            .store
            .set_event(&run.lease, step_id, key, value, serialization)
            .await?;
        self.wakeups.wake(&WakeKey::Event(
            run.workflow_id().to_owned(),
            key.to_owned(),
        ));
        Ok(outcome)
    }

    pub async fn list_events(&self, workflow_id: &str) -> Result<Vec<EventRow>, Error> {
        self.store.list_events(workflow_id).await
    }
}
