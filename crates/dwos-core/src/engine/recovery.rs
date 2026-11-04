use std::time::Duration;

use super::Engine;
use crate::Error;
use crate::workflow::Claimed;

fn jittered(interval: Duration) -> Duration {
    let random = f64::from(uuid::Uuid::new_v4().as_bytes()[0]) / 255.0;
    interval.mul_f64(0.5 + random)
}

impl Engine {
    pub async fn recover_now(&self, names: &[String]) -> Result<Vec<Claimed>, Error> {
        self.store
            .reclaim(
                names,
                self.config.max_concurrent_recoveries,
                self.config.lease,
            )
            .await
    }

    pub async fn recovery_tick(&self, names: &[String]) -> Vec<Claimed> {
        tokio::time::sleep(jittered(self.config.recovery_interval)).await;
        let free = usize::try_from(self.config.max_concurrent_recoveries)
            .unwrap_or(usize::MAX)
            .saturating_sub(self.active_runs());
        let limit = u32::try_from(free).unwrap_or(u32::MAX);
        if limit == 0 {
            return Vec::new();
        }
        match self.store.reclaim(names, limit, self.config.lease).await {
            Ok(claims) => claims,
            Err(error) => {
                log::error!("workflow recovery failed, and will be tried again: {error}");
                Vec::new()
            }
        }
    }
}
