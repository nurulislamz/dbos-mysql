use sqlx::mysql::MySqlDatabaseError;
use sqlx::{Any, Row, Transaction};

use super::sleep::{SLEEP_STEP_NAME, parse_wake_ms};
use super::{Store, check_name};
use crate::Error;
use crate::workflow::{Lease, PORTABLE_JSON, StepRecord, check_step_name, new_token};

pub const NULL_TOPIC: &str = "__null__topic__";
pub const JSON_NULL: &str = "null";
pub const SEND_STEP: &str = "DBOS.send";
pub const RECV_STEP: &str = "DBOS.recv";
pub const SET_EVENT_STEP: &str = "DBOS.setEvent";
pub const GET_EVENT_STEP: &str = "DBOS.getEvent";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredValue {
    pub text: String,
    pub serialization: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventRow {
    pub key: String,
    pub value: StoredValue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageRow {
    pub destination: String,
    pub topic: String,
    pub message: StoredValue,
    pub message_uuid: String,
    pub consumed: bool,
    pub consumed_by_function_id: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Poll {
    Ready(StoredValue),
    Waiting { remaining_ms: i64, deadline_ms: i64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SendOutcome {
    Sent,
    Replayed,
    ReplayedError(String),
}

#[derive(Clone, Copy, Debug)]
pub enum SendFrom<'a> {
    Outside,
    Step(&'a Lease),
    Workflow { lease: &'a Lease, step_id: u32 },
}

fn topic_name(topic: Option<&str>) -> &str {
    topic.filter(|name| !name.is_empty()).unwrap_or(NULL_TOPIC)
}

fn is_fk_violation(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(database) = error else {
        return false;
    };
    if let Some(mysql) = database.try_downcast_ref::<MySqlDatabaseError>() {
        return matches!(mysql.number(), 1452 | 1216);
    }
    match database.code().as_deref() {
        Some("23503" | "787") => true,
        _ => {
            let message = database.message();
            message.contains("FOREIGN KEY") || message.contains("foreign key")
        }
    }
}

struct RecordedWrite {
    name: String,
    output: Option<String>,
    error: Option<String>,
    child_workflow_id: Option<String>,
    serialization: String,
}

fn recorded_send(
    workflow_id: &str,
    step_id: u32,
    expected: &'static str,
    step: &RecordedWrite,
) -> Result<SendOutcome, Error> {
    check_step_name(workflow_id, step_id, expected, step.name.clone())?;
    if step.child_workflow_id.is_some() {
        return Err(Error::StepKindMismatch {
            workflow_id: workflow_id.to_owned(),
            step_id,
            expected: "step",
        });
    }
    if let Some(error) = &step.error {
        return Ok(SendOutcome::ReplayedError(error.clone()));
    }
    Ok(SendOutcome::Replayed)
}

fn recorded_value(
    workflow_id: &str,
    step_id: u32,
    step_name: &str,
    step: &RecordedWrite,
) -> Result<StoredValue, Error> {
    check_step_name(workflow_id, step_id, step_name, step.name.clone())?;
    if step.child_workflow_id.is_some() {
        return Err(Error::StepKindMismatch {
            workflow_id: workflow_id.to_owned(),
            step_id,
            expected: "step",
        });
    }
    if let Some(error) = &step.error {
        return Err(Error::Corrupt(format!(
            "workflow {workflow_id} step {step_id} recorded an error during wait: {error}"
        )));
    }
    let text = step.output.clone().ok_or_else(|| {
        Error::Corrupt(format!(
            "workflow {workflow_id} step {step_id} has no stored output"
        ))
    })?;
    Ok(StoredValue {
        text,
        serialization: step.serialization.clone(),
    })
}

fn check_wait_steps(step_id: u32, timeout_step_id: u32) -> Result<(), Error> {
    if step_id == timeout_step_id {
        return Err(Error::InvalidConfig(
            "a wait and its timeout require different step IDs".to_owned(),
        ));
    }
    Ok(())
}

impl Store {
    pub async fn now_ms(&self) -> Result<i64, Error> {
        self.attempt(|| async {
            let value: i64 = sqlx::query_scalar(self.sql("SELECT {NOW}"))
                .fetch_one(&self.pool)
                .await?;
            Ok(value)
        })
        .await
    }

    pub async fn send(
        &self,
        destination: &str,
        topic: Option<&str>,
        message: &str,
        serialization: &str,
        idempotency_key: Option<&str>,
        from: SendFrom<'_>,
    ) -> Result<SendOutcome, Error> {
        check_name("destination workflow ID", destination)?;
        let topic = topic_name(topic);
        check_name("topic", topic)?;
        self.check_size(&[Some(message)])?;
        let message_uuid = match idempotency_key {
            Some(key) => {
                check_name("idempotency key", key)?;
                key.to_owned()
            }
            None => new_token(),
        };
        self.attempt(|| {
            self.send_once(
                destination,
                topic,
                message,
                serialization,
                &message_uuid,
                from,
            )
        })
        .await
    }

    async fn send_once(
        &self,
        destination: &str,
        topic: &str,
        message: &str,
        serialization: &str,
        message_uuid: &str,
        from: SendFrom<'_>,
    ) -> Result<SendOutcome, Error> {
        let mut tx = match from {
            SendFrom::Outside => self.begin().await?,
            SendFrom::Step(lease) | SendFrom::Workflow { lease, .. } => {
                self.owned_transaction(lease).await?.0
            }
        };
        match from {
            SendFrom::Workflow { lease, step_id } => {
                let (next, recorded) = self.read_for_write(tx, lease, step_id).await?;
                tx = next;
                if let Some(recorded) = recorded {
                    let outcome = recorded_send(&lease.workflow_id, step_id, SEND_STEP, &recorded)?;
                    tx.commit().await?;
                    return Ok(outcome);
                }
            }
            SendFrom::Step(lease) => {
                tx = self.live_transaction(tx, lease).await?;
            }
            SendFrom::Outside => {}
        }
        let inserted = sqlx::query(self.sql(&format!(
            "INSERT INTO notifications (destination_uuid, topic, message, created_at_epoch_ms, message_uuid, serialization, consumed) \
             VALUES (?, ?, ?, {{NOW}}, ?, ?, 0) {}",
            self.dialect.insert_message_if_absent()
        )))
        .bind(destination)
        .bind(topic)
        .bind(message)
        .bind(message_uuid)
        .bind(serialization)
        .execute(&mut *tx)
        .await;
        if let Err(error) = inserted {
            if is_fk_violation(&error) {
                return Err(Error::SendDestinationNotFound(destination.to_owned()));
            }
            return Err(error.into());
        }
        match from {
            SendFrom::Workflow { lease, step_id } => {
                self.insert_step(
                    &mut tx,
                    &lease.workflow_id,
                    step_id,
                    SEND_STEP,
                    &StepRecord::default(),
                    serialization,
                )
                .await?;
            }
            SendFrom::Step(_) | SendFrom::Outside => {}
        }
        tx.commit().await?;
        Ok(SendOutcome::Sent)
    }

    /// Reads messages recorded no later than the persisted deadline, including after recovery.
    pub async fn poll_recv(
        &self,
        lease: &Lease,
        step_id: u32,
        timeout_step_id: u32,
        topic: Option<&str>,
        timeout_ms: i64,
    ) -> Result<Poll, Error> {
        check_wait_steps(step_id, timeout_step_id)?;
        let topic = topic_name(topic);
        check_name("topic", topic)?;
        let timeout_ms = timeout_ms.max(0);
        self.attempt(|| self.poll_recv_once(lease, step_id, timeout_step_id, topic, timeout_ms))
            .await
    }

    async fn poll_recv_once(
        &self,
        lease: &Lease,
        step_id: u32,
        timeout_step_id: u32,
        topic: &str,
        timeout_ms: i64,
    ) -> Result<Poll, Error> {
        let (mut tx, _) = self.owned_transaction(lease).await?;
        let (next, recorded) = self.read_for_write(tx, lease, step_id).await?;
        tx = next;
        if let Some(recorded) = recorded {
            let saved = recorded_value(&lease.workflow_id, step_id, RECV_STEP, &recorded)?;
            tx.commit().await?;
            return Ok(Poll::Ready(saved));
        }
        let (deadline_ms, _) = self
            .ensure_deadline(&mut tx, lease, timeout_step_id, timeout_ms)
            .await?;
        let now_ms: i64 = sqlx::query_scalar(self.sql("SELECT {NOW}"))
            .fetch_one(&mut *tx)
            .await?;
        if let Some(message) = self
            .take_message(&mut tx, &lease.workflow_id, topic, step_id, deadline_ms)
            .await?
        {
            self.insert_step(
                &mut tx,
                &lease.workflow_id,
                step_id,
                RECV_STEP,
                &StepRecord {
                    output: Some(message.text.clone()),
                    ..StepRecord::default()
                },
                &message.serialization,
            )
            .await?;
            tx.commit().await?;
            return Ok(Poll::Ready(message));
        }
        if now_ms >= deadline_ms {
            self.insert_step(
                &mut tx,
                &lease.workflow_id,
                step_id,
                RECV_STEP,
                &StepRecord {
                    output: Some(JSON_NULL.to_owned()),
                    ..StepRecord::default()
                },
                PORTABLE_JSON,
            )
            .await?;
            tx.commit().await?;
            return Ok(Poll::Ready(StoredValue {
                text: JSON_NULL.to_owned(),
                serialization: PORTABLE_JSON.to_owned(),
            }));
        }
        tx.commit().await?;
        Ok(Poll::Waiting {
            remaining_ms: deadline_ms - now_ms,
            deadline_ms,
        })
    }

    /// Initial zero-timeout reads observe the latest value once; later polls use arrival history.
    /// Legacy values without history can only satisfy an initial read observed before its deadline.
    pub async fn poll_event(
        &self,
        target: &str,
        key: &str,
        timeout_ms: i64,
        deadline_ms: Option<i64>,
        caller: Option<(&Lease, u32, u32)>,
    ) -> Result<Poll, Error> {
        if let Some((_, step_id, timeout_step_id)) = caller {
            check_wait_steps(step_id, timeout_step_id)?;
        }
        check_name("workflow ID", target)?;
        check_name("event key", key)?;
        let timeout_ms = timeout_ms.max(0);
        self.attempt(|| self.poll_event_once(target, key, timeout_ms, deadline_ms, caller))
            .await
    }

    async fn poll_event_once(
        &self,
        target: &str,
        key: &str,
        timeout_ms: i64,
        deadline_ms: Option<i64>,
        caller: Option<(&Lease, u32, u32)>,
    ) -> Result<Poll, Error> {
        let mut tx = match caller {
            Some((lease, _, _)) => self.owned_transaction(lease).await?.0,
            None => self.begin().await?,
        };
        if let Some((lease, step_id, _)) = caller {
            let (next, recorded) = self.read_for_write(tx, lease, step_id).await?;
            tx = next;
            if let Some(recorded) = recorded {
                let saved = recorded_value(&lease.workflow_id, step_id, GET_EVENT_STEP, &recorded)?;
                tx.commit().await?;
                return Ok(Poll::Ready(saved));
            }
        }
        let (deadline_ms, initial) = if let Some((lease, _, timeout_step_id)) = caller {
            self.ensure_deadline(&mut tx, lease, timeout_step_id, timeout_ms)
                .await?
        } else if let Some(deadline_ms) = deadline_ms {
            (deadline_ms, false)
        } else {
            let now_ms: i64 = sqlx::query_scalar(self.sql("SELECT {NOW}"))
                .fetch_one(&mut *tx)
                .await?;
            (now_ms.saturating_add(timeout_ms), true)
        };
        let now_ms: i64 = sqlx::query_scalar(self.sql("SELECT {NOW}"))
            .fetch_one(&mut *tx)
            .await?;
        let event = if initial && timeout_ms == 0 {
            self.read_event(&mut tx, target, key).await?
        } else {
            let event = self
                .read_event_before(&mut tx, target, key, deadline_ms)
                .await?;
            if event.is_none() && initial {
                self.read_legacy_event_before(&mut tx, target, key, deadline_ms)
                    .await?
            } else {
                event
            }
        };
        if event.is_some() || now_ms >= deadline_ms {
            let value = event.unwrap_or(StoredValue {
                text: JSON_NULL.to_owned(),
                serialization: PORTABLE_JSON.to_owned(),
            });
            if let Some((lease, step_id, _)) = caller {
                self.insert_step(
                    &mut tx,
                    &lease.workflow_id,
                    step_id,
                    GET_EVENT_STEP,
                    &StepRecord {
                        output: Some(value.text.clone()),
                        ..StepRecord::default()
                    },
                    &value.serialization,
                )
                .await?;
            }
            tx.commit().await?;
            return Ok(Poll::Ready(value));
        }
        tx.commit().await?;
        Ok(Poll::Waiting {
            remaining_ms: deadline_ms - now_ms,
            deadline_ms,
        })
    }

    pub async fn set_event(
        &self,
        lease: &Lease,
        step_id: Option<u32>,
        key: &str,
        value: &str,
        serialization: &str,
    ) -> Result<SendOutcome, Error> {
        check_name("event key", key)?;
        self.check_size(&[Some(value)])?;
        self.attempt(|| self.set_event_once(lease, step_id, key, value, serialization))
            .await
    }

    async fn set_event_once(
        &self,
        lease: &Lease,
        step_id: Option<u32>,
        key: &str,
        value: &str,
        serialization: &str,
    ) -> Result<SendOutcome, Error> {
        let (mut tx, _) = self.owned_transaction(lease).await?;
        if let Some(step_id) = step_id {
            let (next, recorded) = self.read_for_write(tx, lease, step_id).await?;
            tx = next;
            if let Some(recorded) = recorded {
                let outcome =
                    recorded_send(&lease.workflow_id, step_id, SET_EVENT_STEP, &recorded)?;
                tx.commit().await?;
                return Ok(outcome);
            }
        } else {
            tx = self.live_transaction(tx, lease).await?;
        }
        sqlx::query(self.sql(&format!(
            "INSERT INTO workflow_events (workflow_uuid, {{KEY}}, value, serialization) VALUES (?, ?, ?, ?) {}",
            self.dialect.upsert_event()
        )))
        .bind(lease.workflow_id.as_str())
        .bind(key)
        .bind(value)
        .bind(serialization)
        .execute(&mut *tx)
        .await?;
        let revision: Option<i64> = sqlx::query_scalar(self.sql(
            "SELECT MAX(revision) FROM workflow_event_history WHERE workflow_uuid = ? AND event_key = ?",
        ))
        .bind(lease.workflow_id.as_str())
        .bind(key)
        .fetch_one(&mut *tx)
        .await?;
        let revision = revision
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| Error::Corrupt("event revision overflow".to_owned()))?;
        sqlx::query(self.sql(
            "INSERT INTO workflow_event_history (workflow_uuid, event_key, revision, created_at_epoch_ms, value, serialization) \
             VALUES (?, ?, ?, {NOW}, ?, ?)",
        ))
        .bind(lease.workflow_id.as_str())
        .bind(key)
        .bind(revision)
        .bind(value)
        .bind(serialization)
        .execute(&mut *tx)
        .await?;
        if let Some(step_id) = step_id {
            self.insert_step(
                &mut tx,
                &lease.workflow_id,
                step_id,
                SET_EVENT_STEP,
                &StepRecord::default(),
                serialization,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(SendOutcome::Sent)
    }

    pub async fn list_events(&self, workflow_id: &str) -> Result<Vec<EventRow>, Error> {
        self.attempt(|| async {
            let rows = sqlx::query(self.sql(
                "SELECT {KEY} AS event_key, value, serialization FROM workflow_events WHERE workflow_uuid = ? ORDER BY {KEY}",
            ))
            .bind(workflow_id)
            .fetch_all(&self.pool)
            .await?;
            rows.iter()
                .map(|row| {
                    Ok(EventRow {
                        key: row.try_get("event_key")?,
                        value: StoredValue {
                            text: row.try_get("value")?,
                            serialization: row.try_get("serialization")?,
                        },
                    })
                })
                .collect()
        })
        .await
    }

    pub async fn list_messages(&self, destination: &str) -> Result<Vec<MessageRow>, Error> {
        self.attempt(|| async {
            let rows = sqlx::query(self.sql(
                "SELECT destination_uuid, topic, message, message_uuid, serialization, consumed, consumed_by_function_id \
                 FROM notifications WHERE destination_uuid = ? ORDER BY message_id",
            ))
            .bind(destination)
            .fetch_all(&self.pool)
            .await?;
            rows.iter()
                .map(|row| {
                    let consumed: i64 = row.try_get("consumed")?;
                    Ok(MessageRow {
                        destination: row.try_get("destination_uuid")?,
                        topic: row.try_get("topic")?,
                        message: StoredValue {
                            text: row.try_get("message")?,
                            serialization: row.try_get("serialization")?,
                        },
                        message_uuid: row.try_get("message_uuid")?,
                        consumed: consumed != 0,
                        consumed_by_function_id: match row.try_get::<Option<i64>, _>("consumed_by_function_id")? {
                            Some(value) => Some(u32::try_from(value).map_err(|_| {
                                Error::Corrupt(format!("consumed_by_function_id is out of range: {value}"))
                            })?),
                            None => None,
                        },
                    })
                })
                .collect()
        })
        .await
    }

    /// Replay and the claim window in one read. A recorded step is returned without revoking
    /// an expired lease. A new write revokes that lease first, same as `live_transaction`.
    async fn read_for_write(
        &self,
        mut tx: Transaction<'static, Any>,
        lease: &Lease,
        step_id: u32,
    ) -> Result<(Transaction<'static, Any>, Option<RecordedWrite>), Error> {
        let row = sqlx::query(self.sql(
            "SELECT workflow_status.claim_after AS claim_after, {NOW} AS now_ms, \
             operation_outputs.function_name AS function_name, operation_outputs.output AS output, \
             operation_outputs.error AS error, operation_outputs.child_workflow_id AS child_workflow_id, \
             operation_outputs.serialization AS serialization \
             FROM operation_outputs \
             RIGHT JOIN workflow_status ON operation_outputs.workflow_uuid = workflow_status.workflow_uuid \
             AND operation_outputs.function_id = ? \
             WHERE workflow_status.workflow_uuid = ? AND workflow_status.lease_token = ?",
        ))
        .bind(i64::from(step_id))
        .bind(lease.workflow_id.as_str())
        .bind(lease.token.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            return Err(Error::LeaseLost(lease.workflow_id.clone()));
        };
        if let Some(name) = row.try_get::<Option<String>, _>("function_name")? {
            return Ok((
                tx,
                Some(RecordedWrite {
                    name,
                    output: row.try_get("output")?,
                    error: row.try_get("error")?,
                    child_workflow_id: row.try_get("child_workflow_id")?,
                    serialization: row
                        .try_get::<Option<String>, _>("serialization")?
                        .ok_or_else(|| {
                            Error::Corrupt(format!(
                                "workflow {} step {step_id} has no stored serialization",
                                lease.workflow_id
                            ))
                        })?,
                }),
            ));
        }
        let claim_after: Option<i64> = row.try_get("claim_after")?;
        let now_ms: i64 = row.try_get("now_ms")?;
        if claim_after.is_some_and(|claim_after| claim_after <= now_ms) {
            tx = self.live_transaction(tx, lease).await?;
        }
        Ok((tx, None))
    }

    async fn ensure_deadline(
        &self,
        tx: &mut Transaction<'static, Any>,
        lease: &Lease,
        timeout_step_id: u32,
        timeout_ms: i64,
    ) -> Result<(i64, bool), Error> {
        if let Some((recorded_name, record)) = self
            .read_step(tx, &lease.workflow_id, timeout_step_id)
            .await?
        {
            check_step_name(
                &lease.workflow_id,
                timeout_step_id,
                SLEEP_STEP_NAME,
                recorded_name,
            )?;
            return Ok((
                parse_wake_ms(&lease.workflow_id, timeout_step_id, &record)?,
                false,
            ));
        }
        let now_ms: i64 = sqlx::query_scalar(self.sql("SELECT {NOW}"))
            .fetch_one(&mut **tx)
            .await?;
        let deadline_ms = now_ms.saturating_add(timeout_ms);
        self.insert_step(
            tx,
            &lease.workflow_id,
            timeout_step_id,
            SLEEP_STEP_NAME,
            &StepRecord {
                output: Some(deadline_ms.to_string()),
                ..StepRecord::default()
            },
            PORTABLE_JSON,
        )
        .await?;
        Ok((deadline_ms, true))
    }

    async fn take_message(
        &self,
        tx: &mut Transaction<'static, Any>,
        destination: &str,
        topic: &str,
        step_id: u32,
        deadline_ms: i64,
    ) -> Result<Option<StoredValue>, Error> {
        let row = sqlx::query(self.sql(&format!(
            "SELECT message_uuid, message, serialization FROM notifications \
             WHERE destination_uuid = ? AND topic = ? AND consumed = 0 AND created_at_epoch_ms <= ? \
             ORDER BY message_id LIMIT 1 {}",
            self.dialect.lock_row()
        )))
        .bind(destination)
        .bind(topic)
        .bind(deadline_ms)
        .fetch_optional(&mut **tx)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let message_uuid: String = row.try_get("message_uuid")?;
        sqlx::query(self.sql(
            "UPDATE notifications SET consumed = 1, consumed_by_function_id = ? WHERE destination_uuid = ? AND message_uuid = ?",
        ))
        .bind(i64::from(step_id))
        .bind(destination)
        .bind(message_uuid.as_str())
        .execute(&mut **tx)
        .await?;
        Ok(Some(StoredValue {
            text: row.try_get("message")?,
            serialization: row.try_get("serialization")?,
        }))
    }

    async fn read_event_before(
        &self,
        tx: &mut Transaction<'static, Any>,
        workflow_id: &str,
        key: &str,
        deadline_ms: i64,
    ) -> Result<Option<StoredValue>, Error> {
        let row = sqlx::query(self.sql(
            "SELECT value, serialization FROM workflow_event_history \
             WHERE workflow_uuid = ? AND event_key = ? AND created_at_epoch_ms <= ? \
             ORDER BY revision DESC LIMIT 1",
        ))
        .bind(workflow_id)
        .bind(key)
        .bind(deadline_ms)
        .fetch_optional(&mut **tx)
        .await?;
        row.map(|row| {
            Ok(StoredValue {
                text: row.try_get("value")?,
                serialization: row.try_get("serialization")?,
            })
        })
        .transpose()
    }

    async fn read_legacy_event_before(
        &self,
        tx: &mut Transaction<'static, Any>,
        workflow_id: &str,
        key: &str,
        deadline_ms: i64,
    ) -> Result<Option<StoredValue>, Error> {
        let row = sqlx::query(self.sql(
            "SELECT value, serialization FROM workflow_events \
             WHERE workflow_uuid = ? AND {KEY} = ? AND {NOW} <= ? AND NOT EXISTS \
             (SELECT 1 FROM workflow_event_history WHERE workflow_event_history.workflow_uuid = workflow_events.workflow_uuid \
              AND workflow_event_history.event_key = workflow_events.{KEY})",
        ))
        .bind(workflow_id)
        .bind(key)
        .bind(deadline_ms)
        .fetch_optional(&mut **tx)
        .await?;
        row.map(|row| {
            Ok(StoredValue {
                text: row.try_get("value")?,
                serialization: row.try_get("serialization")?,
            })
        })
        .transpose()
    }

    async fn read_event(
        &self,
        tx: &mut Transaction<'static, Any>,
        workflow_id: &str,
        key: &str,
    ) -> Result<Option<StoredValue>, Error> {
        let row = sqlx::query(self.sql(
            "SELECT value, serialization FROM workflow_events WHERE workflow_uuid = ? AND {KEY} = ?",
        ))
        .bind(workflow_id)
        .bind(key)
        .fetch_optional(&mut **tx)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(StoredValue {
            text: row.try_get("value")?,
            serialization: row.try_get("serialization")?,
        }))
    }

    async fn insert_step(
        &self,
        tx: &mut Transaction<'static, Any>,
        workflow_id: &str,
        step_id: u32,
        step_name: &str,
        record: &StepRecord,
        serialization: &str,
    ) -> Result<(), Error> {
        sqlx::query(self.sql(
            "INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, output, error, child_workflow_id, serialization, completed_at_epoch_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, {NOW})",
        ))
        .bind(workflow_id)
        .bind(i64::from(step_id))
        .bind(step_name)
        .bind(record.output.as_deref())
        .bind(record.error.as_deref())
        .bind(record.child_workflow_id.as_deref())
        .bind(serialization)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}
