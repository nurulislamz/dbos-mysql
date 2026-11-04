use std::time::Duration;

use super::{Engine, Run};
use crate::Error;

impl Engine {
    pub async fn sleep(&self, run: &Run, step_id: u32, seconds: f64) -> Result<Duration, Error> {
        self.store.record_sleep(&run.lease, step_id, seconds).await
    }
}
