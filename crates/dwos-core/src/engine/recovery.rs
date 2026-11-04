use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::Engine;
use crate::Error;
use crate::workflow::Claimed;

fn jittered(interval: Duration) -> Duration {
    let random = f64::from(uuid::Uuid::new_v4().as_bytes()[0]) / 255.0;
    interval.mul_f64(0.5 + random)
}

const UNREADABLE_WARN_INTERVAL_MS: u64 = 60_000;

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn unreadable_warning_due(last_ms: u64, now: u64, interval_ms: u64) -> bool {
    last_ms == 0 || now.saturating_sub(last_ms) >= interval_ms
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
        self.warn_unreadable_pickled().await;
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

    async fn warn_unreadable_pickled(&self) {
        if self.config.pickle {
            return;
        }
        let now = now_ms();
        let last = self.last_unreadable_warn_ms.load(Ordering::Relaxed);
        if !unreadable_warning_due(last, now, UNREADABLE_WARN_INTERVAL_MS) {
            return;
        }
        match self.store.count_due_pickled().await {
            Ok(0) | Err(_) => {}
            Ok(count) => {
                self.last_unreadable_warn_ms.store(now, Ordering::Relaxed);
                log::warn!(
                    "{count} pickled workflow(s) are due, but this process cannot read pickle, so they were left unclaimed"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unreadable_warning_is_due_once_per_interval() {
        assert!(unreadable_warning_due(0, 1, 60_000));
        assert!(!unreadable_warning_due(1_000, 2_000, 60_000));
        assert!(!unreadable_warning_due(1_000, 60_999, 60_000));
        assert!(unreadable_warning_due(1_000, 61_000, 60_000));
    }
}
