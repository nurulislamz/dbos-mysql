use sqlx::{Any, Row, Transaction};

use super::{Store, check_name, get_u32};
use crate::Error;
use crate::cron::Cron;
use crate::workflow::{PORTABLE_JSON, PY_PICKLE, new_token};

/// What `ensure_schedule` stores. An existing name is updated and keeps its last fire time.
#[derive(Clone, Copy, Debug)]
pub struct NewSchedule<'a> {
    pub name: &'a str,
    pub workflow_name: &'a str,
    pub cron: &'a str,
    pub queue: &'a str,
    pub inputs: &'a str,
    pub serialization: &'a str,
    pub max_recovery_attempts: u32,
}

/// One cron tick. `started` is false when a previous run of this schedule is still pending:
/// the tick is recorded and no second run is enqueued.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleFire {
    pub schedule_name: String,
    pub queue_name: String,
    pub workflow_id: String,
    pub scheduled_at_ms: i64,
    pub started: bool,
}

impl Store {
    pub async fn ensure_schedule(&self, schedule: NewSchedule<'_>) -> Result<(), Error> {
        required_name("schedule name", schedule.name)?;
        required_name("workflow name", schedule.workflow_name)?;
        required_name("queue name", schedule.queue)?;
        check_name("cron", schedule.cron)?;
        if schedule.serialization != PORTABLE_JSON && schedule.serialization != PY_PICKLE {
            return Err(Error::InvalidConfig(format!(
                "schedule serialization {:?} is unknown",
                schedule.serialization
            )));
        }
        // Reject an expression that can never fire before writing it.
        Cron::parse(schedule.cron)?;
        self.check_size(&[Some(schedule.inputs)])?;
        let statement = match self.dialect {
            super::Dialect::MySql => {
                "INSERT INTO workflow_schedules \
                 (schedule_name, workflow_name, cron, queue_name, inputs, serialization, max_recovery_attempts) \
                 VALUES (?, ?, ?, ?, ?, ?, ?) AS incoming ON DUPLICATE KEY UPDATE \
                 workflow_name = incoming.workflow_name, cron = incoming.cron, queue_name = incoming.queue_name, \
                 inputs = incoming.inputs, serialization = incoming.serialization, \
                 max_recovery_attempts = incoming.max_recovery_attempts"
            }
            super::Dialect::Postgres | super::Dialect::Sqlite => {
                "INSERT INTO workflow_schedules \
                 (schedule_name, workflow_name, cron, queue_name, inputs, serialization, max_recovery_attempts) \
                 VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (schedule_name) DO UPDATE SET \
                 workflow_name = excluded.workflow_name, cron = excluded.cron, queue_name = excluded.queue_name, \
                 inputs = excluded.inputs, serialization = excluded.serialization, \
                 max_recovery_attempts = excluded.max_recovery_attempts"
            }
        };
        self.attempt(|| async {
            sqlx::query(self.sql(statement))
                .bind(schedule.name)
                .bind(schedule.workflow_name)
                .bind(schedule.cron)
                .bind(schedule.queue)
                .bind(schedule.inputs)
                .bind(schedule.serialization)
                .bind(i64::from(schedule.max_recovery_attempts))
                .execute(&self.pool)
                .await
                .map_err(Error::from)
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn list_schedule_names(&self) -> Result<Vec<String>, Error> {
        self.attempt(|| async {
            sqlx::query_scalar(
                self.sql("SELECT schedule_name FROM workflow_schedules ORDER BY schedule_name"),
            )
            .fetch_all(&self.pool)
            .await
            .map_err(Error::from)
        })
        .await
    }

    /// Arms a new schedule at the database clock, or enqueues the latest missed tick.
    /// A tick is skipped, and still consumed, while any run of this schedule is `PENDING`.
    pub(crate) async fn fire_schedule(&self, name: &str) -> Result<Option<ScheduleFire>, Error> {
        self.attempt(|| self.fire_schedule_once(name)).await
    }

    async fn fire_schedule_once(&self, name: &str) -> Result<Option<ScheduleFire>, Error> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(self.sql(&format!(
            "SELECT workflow_name, cron, queue_name, inputs, serialization, max_recovery_attempts, last_fired_epoch_ms \
             FROM workflow_schedules WHERE schedule_name = ? {}",
            self.dialect.lock_row(),
        )))
        .bind(name)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(None);
        };
        let workflow_name: String = row.try_get("workflow_name")?;
        let cron_text: String = row.try_get("cron")?;
        let queue: String = row.try_get("queue_name")?;
        let inputs: String = row.try_get("inputs")?;
        let serialization: String = row.try_get("serialization")?;
        let max_recovery_attempts = get_u32(&row, "max_recovery_attempts")?;
        let last_fired: Option<i64> = row.try_get("last_fired_epoch_ms")?;
        let now: i64 = sqlx::query_scalar(self.sql("SELECT {NOW}"))
            .fetch_one(&mut *tx)
            .await?;
        let Some(last_fired) = last_fired else {
            sqlx::query(self.sql(
                "UPDATE workflow_schedules SET last_fired_epoch_ms = ? \
                 WHERE schedule_name = ? AND last_fired_epoch_ms IS NULL",
            ))
            .bind(now)
            .bind(name)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            return Ok(None);
        };
        let cron = match Cron::parse(&cron_text) {
            Ok(cron) => cron,
            Err(error) => {
                log::error!("schedule {name} has a cron that cannot fire: {error}");
                tx.commit().await?;
                return Ok(None);
            }
        };
        let Some(tick) = cron.latest_due(last_fired, now) else {
            tx.commit().await?;
            return Ok(None);
        };
        let workflow_id = format!("sched-{name}-{tick}");
        check_name("workflow ID", &workflow_id)?;
        let queue_exists: i64 =
            sqlx::query_scalar(self.sql("SELECT COUNT(*) FROM workflow_queues WHERE name = ?"))
                .bind(queue.as_str())
                .fetch_one(&mut *tx)
                .await?;
        if queue_exists == 0 {
            log::warn!("schedule {name} is due, but queue {queue} does not exist");
            tx.commit().await?;
            return Ok(None);
        }
        let pending: i64 = sqlx::query_scalar(self.sql(
            "SELECT COUNT(*) FROM workflow_status WHERE schedule_name = ? AND status = 'PENDING'",
        ))
        .bind(name)
        .fetch_one(&mut *tx)
        .await?;
        let (mut tx, started) = if pending > 0 {
            (tx, false)
        } else {
            self.insert_scheduled(
                tx,
                DueRun {
                    workflow_id: &workflow_id,
                    workflow_name: &workflow_name,
                    inputs: &inputs,
                    serialization: &serialization,
                    queue: &queue,
                    schedule_name: name,
                    max_recovery_attempts,
                },
            )
            .await?
        };
        sqlx::query(
            self.sql(
                "UPDATE workflow_schedules SET last_fired_epoch_ms = ? WHERE schedule_name = ?",
            ),
        )
        .bind(tick)
        .bind(name)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some(ScheduleFire {
            schedule_name: name.to_owned(),
            queue_name: queue,
            workflow_id,
            scheduled_at_ms: tick,
            started,
        }))
    }

    async fn insert_scheduled(
        &self,
        mut tx: Transaction<'static, Any>,
        due: DueRun<'_>,
    ) -> Result<(Transaction<'static, Any>, bool), Error> {
        let token = new_token();
        sqlx::query(self.sql(&format!(
            "INSERT INTO workflow_status \
             (workflow_uuid, name, status, inputs, claim_after, lease_token, recovery_attempts, max_recovery_attempts, serialization, schedule_name, created_at, updated_at) \
             VALUES (?, ?, 'PENDING', ?, {{NOW}}, ?, 0, ?, ?, ?, {{NOW}}, {{NOW}}) {}",
            self.dialect.insert_if_absent(),
        )))
        .bind(due.workflow_id)
        .bind(due.workflow_name)
        .bind(due.inputs)
        .bind(&token)
        .bind(i64::from(due.max_recovery_attempts))
        .bind(due.serialization)
        .bind(due.schedule_name)
        .execute(&mut *tx)
        .await?;
        let owner: Option<String> = sqlx::query_scalar(
            self.sql("SELECT lease_token FROM workflow_status WHERE workflow_uuid = ?"),
        )
        .bind(due.workflow_id)
        .fetch_one(&mut *tx)
        .await?;
        if owner.as_deref() != Some(token.as_str()) {
            return Ok((tx, false));
        }
        sqlx::query(self.sql(
            "INSERT INTO workflow_queue_entries (workflow_uuid, queue_name, start_timeout_ms) VALUES (?, ?, NULL)",
        ))
        .bind(due.workflow_id)
        .bind(due.queue)
        .execute(&mut *tx)
        .await?;
        sqlx::query(self.sql(
            "UPDATE workflow_status SET lease_token = NULL WHERE workflow_uuid = ? AND lease_token = ?",
        ))
        .bind(due.workflow_id)
        .bind(&token)
        .execute(&mut *tx)
        .await?;
        Ok((tx, true))
    }
}

struct DueRun<'a> {
    workflow_id: &'a str,
    workflow_name: &'a str,
    inputs: &'a str,
    serialization: &'a str,
    queue: &'a str,
    schedule_name: &'a str,
    max_recovery_attempts: u32,
}

fn required_name(field: &'static str, value: &str) -> Result<(), Error> {
    if value.is_empty() {
        return Err(Error::InvalidConfig(format!("{field} must not be empty")));
    }
    check_name(field, value)
}
