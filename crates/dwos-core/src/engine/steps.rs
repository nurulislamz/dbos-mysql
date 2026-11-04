use std::time::Duration;

use super::{Engine, Run};
use crate::Error;
use crate::workflow::StepRecord;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StepDecision {
    Run,
    ReplayOutput(String),
    ReplayError(String),
}

#[derive(Clone, Copy, Debug)]
pub struct StepPolicy {
    retries_allowed: bool,
    interval_seconds: f64,
    max_attempts: u32,
    backoff_rate: f64,
}

impl StepPolicy {
    pub fn new(
        retries_allowed: bool,
        interval_seconds: f64,
        max_attempts: u32,
        backoff_rate: f64,
    ) -> Result<Self, Error> {
        if !interval_seconds.is_finite() || interval_seconds < 0.0 {
            return Err(Error::InvalidConfig(
                "interval_seconds must be zero or more".to_owned(),
            ));
        }
        if !backoff_rate.is_finite() || backoff_rate < 0.0 {
            return Err(Error::InvalidConfig(
                "backoff_rate must be zero or more".to_owned(),
            ));
        }
        if max_attempts == 0 {
            return Err(Error::InvalidConfig(
                "max_attempts must be at least 1".to_owned(),
            ));
        }
        Ok(Self {
            retries_allowed,
            interval_seconds,
            max_attempts,
            backoff_rate,
        })
    }

    pub fn attempts(self) -> StepAttempts {
        StepAttempts {
            policy: self,
            attempt: 1,
        }
    }

    /// A step that retries reports all its failures together. One that does not raises its own error.
    pub fn wraps_failures(&self) -> bool {
        self.retries_allowed
    }

    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    pub fn status(&self, attempt: u32) -> (Option<u32>, Option<u32>) {
        if self.retries_allowed {
            (Some(attempt.saturating_sub(1)), Some(self.max_attempts))
        } else {
            (None, None)
        }
    }

    pub fn retry_delay(&self, failed_attempt: u32) -> Option<Duration> {
        if !self.retries_allowed || failed_attempt >= self.max_attempts {
            return None;
        }
        let exponent = i32::try_from(failed_attempt.saturating_sub(1)).unwrap_or(i32::MAX);
        let seconds = self.interval_seconds * self.backoff_rate.powi(exponent);
        Some(Duration::try_from_secs_f64(seconds).unwrap_or(Duration::MAX))
    }
}

/// Counts one step call's attempts, and says what to do after each failure.
#[derive(Debug)]
pub struct StepAttempts {
    policy: StepPolicy,
    attempt: u32,
}

impl StepAttempts {
    pub fn status(&self) -> (Option<u32>, Option<u32>) {
        self.policy.status(self.attempt)
    }

    /// The delay before trying again, or `None` when the step has failed for good.
    pub fn failed(&mut self) -> Option<Duration> {
        let delay = self.policy.retry_delay(self.attempt)?;
        self.attempt += 1;
        Some(delay)
    }
}

impl Engine {
    pub async fn check_step(
        &self,
        run: &Run,
        step_id: u32,
        name: &str,
    ) -> Result<StepDecision, Error> {
        let Some(saved) = self
            .store
            .load_step_owned(&run.lease, step_id, name)
            .await?
        else {
            return Ok(StepDecision::Run);
        };
        if let Some(error) = saved.error {
            return Ok(StepDecision::ReplayError(error));
        }
        saved
            .output
            .map(StepDecision::ReplayOutput)
            .ok_or_else(|| Error::StepKindMismatch {
                workflow_id: run.workflow_id().to_owned(),
                step_id,
                expected: "step",
            })
    }

    pub async fn save_step_output(
        &self,
        run: &Run,
        step_id: u32,
        name: &str,
        output: String,
    ) -> Result<(), Error> {
        let record = StepRecord {
            output: Some(output),
            ..StepRecord::default()
        };
        self.store
            .save_step(&run.lease, step_id, name, &record)
            .await
    }

    pub async fn save_step_error(
        &self,
        run: &Run,
        step_id: u32,
        name: &str,
        error: String,
    ) -> Result<(), Error> {
        if self.is_closing() {
            return Err(Error::ShuttingDown);
        }
        let record = StepRecord {
            error: Some(error),
            ..StepRecord::default()
        };
        self.store
            .save_step(&run.lease, step_id, name, &record)
            .await
    }
}
