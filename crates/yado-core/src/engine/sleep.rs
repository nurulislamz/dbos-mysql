use std::time::Duration;

use tokio::time::{Instant, timeout_at};

use super::{ActiveRun, Engine, POLL_MAX, Run};
use crate::Error;
use crate::lease::LeaseLoss;

impl Engine {
    pub async fn sleep(&self, run: &Run, step_id: u32, seconds: f64) -> Result<Duration, Error> {
        self.store.record_sleep(&run.lease, step_id, seconds).await
    }

    /// Waits out what `sleep` returned, or stops early with the reason the run was lost.
    /// A cancel in this engine pokes the keeper. One from elsewhere is seen by the read below.
    pub async fn wait_sleep(
        &self,
        run: &Run,
        active: &ActiveRun,
        remaining: Duration,
    ) -> Option<LeaseLoss> {
        let wake = Instant::now().checked_add(remaining);
        loop {
            if let Ok(loss) = timeout_at(next_poll(wake), active.lost()).await {
                return Some(loss);
            }
            if wake.is_some_and(|wake| Instant::now() >= wake) {
                return None;
            }
            if self.is_closing() {
                continue;
            }
            // The keeper still decides, so a loss has one path. A failed read proves nothing,
            // and an outage reaches the keeper by itself.
            if let Ok(Ok(false)) =
                timeout_at(next_poll(wake), self.store.holds_lease(&run.lease)).await
            {
                active.keeper.poke();
            }
        }
    }
}

fn next_poll(wake: Option<Instant>) -> Instant {
    let poll = Instant::now() + POLL_MAX;
    wake.map_or(poll, |wake| wake.min(poll))
}
