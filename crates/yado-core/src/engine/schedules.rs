use super::Engine;
use crate::Error;
use crate::store::{NewSchedule, ScheduleFire};

impl Engine {
    pub async fn ensure_schedule(&self, schedule: NewSchedule<'_>) -> Result<(), Error> {
        self.store.ensure_schedule(schedule).await
    }

    pub async fn delete_schedule(&self, name: &str) -> Result<(), Error> {
        self.store.delete_schedule(name).await
    }

    /// Fires every schedule once. A new schedule is armed at the database clock and does not
    /// run a tick that has already passed. After that, only the latest due tick is enqueued,
    /// and it is skipped while an earlier run of that schedule is still pending.
    pub async fn fire_schedules(&self) -> Result<Vec<ScheduleFire>, Error> {
        if self.is_closing() {
            return Ok(Vec::new());
        }
        let names = self.store.list_schedule_names().await?;
        let mut fired = Vec::new();
        for name in names {
            match self.store.fire_schedule(&name).await {
                Ok(Some(fire)) => {
                    if fire.started {
                        self.wake_queue(&fire.queue_name);
                    }
                    fired.push(fire);
                }
                Ok(None) => {}
                Err(error) => {
                    log::error!("firing schedule {name} failed, and will be tried again: {error}");
                }
            }
        }
        Ok(fired)
    }
}
