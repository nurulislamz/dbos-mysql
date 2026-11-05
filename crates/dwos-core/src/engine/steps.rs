use std::time::Duration;

use tokio::time::Instant;

use super::{Engine, Run};
use crate::Error;
use crate::lease::StepWatch;
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
    timeout: Option<Duration>,
    heartbeat_timeout: Option<Duration>,
}

fn attempt_limit(name: &str, seconds: f64) -> Result<Duration, Error> {
    Duration::try_from_secs_f64(seconds)
        .ok()
        .filter(|limit| !limit.is_zero() && Instant::now().checked_add(*limit).is_some())
        .ok_or_else(|| Error::InvalidConfig(format!("{name} must be a positive number of seconds")))
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
            timeout: None,
            heartbeat_timeout: None,
        })
    }

    /// Limits on each attempt. An attempt over a limit fails like any other: see [`StepAttempts::failed`].
    pub fn with_timeouts(
        mut self,
        timeout_seconds: Option<f64>,
        heartbeat_timeout_seconds: Option<f64>,
    ) -> Result<Self, Error> {
        self.timeout = timeout_seconds
            .map(|seconds| attempt_limit("timeout_seconds", seconds))
            .transpose()?;
        self.heartbeat_timeout = heartbeat_timeout_seconds
            .map(|seconds| attempt_limit("heartbeat_timeout_seconds", seconds))
            .transpose()?;
        Ok(self)
    }

    /// Starts one attempt's timers, or returns `None` for a step with no limits.
    pub fn watch(&self) -> Option<StepWatch> {
        (self.timeout.is_some() || self.heartbeat_timeout.is_some())
            .then(|| StepWatch::start(self.timeout, self.heartbeat_timeout))
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
        if self.interval_seconds == 0.0 {
            return Some(Duration::ZERO);
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

    pub fn wraps_failures(&self) -> bool {
        self.policy.wraps_failures()
    }

    /// The delay before trying again, or `None` when the step has failed for good.
    pub fn failed(&mut self, retryable: bool) -> Option<Duration> {
        self.policy.retries_allowed &= retryable;
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

impl Run {
    pub fn last_step_id(&self) -> u32 {
        self.last_step_id.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn step_id_for_call(&self, in_step: bool, reserved: Option<u32>) -> Option<u32> {
        (!in_step).then(|| reserved.unwrap_or_else(|| self.next_step_id()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_non_retryable_failure_stops_without_wrapping_the_error() {
        for failures_before_fatal in [0, 2] {
            let mut attempts = StepPolicy::new(true, 0.0, 3, 2.0).unwrap().attempts();
            for _ in 0..failures_before_fatal {
                assert_eq!(attempts.failed(true), Some(Duration::ZERO));
            }
            assert_eq!(attempts.failed(false), None);
            assert!(!attempts.wraps_failures());
            assert_eq!(attempts.failed(true), None);
        }
    }

    #[test]
    fn exhausted_retryable_failures_keep_the_wrapper() {
        let mut attempts = StepPolicy::new(true, 0.0, 1, 2.0).unwrap().attempts();
        assert_eq!(attempts.failed(true), None);
        assert!(attempts.wraps_failures());
    }

    #[test]
    fn invalid_attempt_limits_are_rejected() {
        let policy = StepPolicy::new(true, 0.0, 3, 2.0).unwrap();
        for bad in [
            0.0,
            -0.0,
            -1.0,
            1.0e-12,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MAX,
            1.0e19,
        ] {
            for (timeout, heartbeat) in
                [(Some(bad), None), (None, Some(bad)), (Some(1.0), Some(bad))]
            {
                assert!(
                    matches!(
                        policy.with_timeouts(timeout, heartbeat),
                        Err(Error::InvalidConfig(_))
                    ),
                    "{timeout:?} {heartbeat:?}"
                );
            }
        }
    }

    #[test]
    fn only_a_step_with_a_limit_gets_a_watch() {
        let policy = StepPolicy::new(true, 0.0, 3, 2.0).unwrap();
        assert!(policy.watch().is_none());
        assert!(policy.with_timeouts(None, None).unwrap().watch().is_none());
        assert!(
            policy
                .with_timeouts(Some(1.0), None)
                .unwrap()
                .watch()
                .is_some()
        );
        assert!(
            policy
                .with_timeouts(None, Some(1.0))
                .unwrap()
                .watch()
                .is_some()
        );
    }

    #[test]
    fn zero_retry_interval_stays_zero_when_backoff_overflows() {
        let policy = StepPolicy::new(true, 0.0, 10, 1.0e300).unwrap();
        assert_eq!(policy.retry_delay(3), Some(Duration::ZERO));
        assert_eq!(policy.retry_delay(10), None);
    }
}
