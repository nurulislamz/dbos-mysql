use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use crate::lease::{LeaseKeeper, LeaseLoss};
use crate::workflow::{
    Claimed, Lease, NewWorkflow, Outcome, RecordedStep, Started, Status, StepRecord, Workflow,
    new_token,
};
use crate::{Error, Store};

const POLL_MIN: Duration = Duration::from_millis(10);
const POLL_MAX: Duration = Duration::from_secs(1);

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub lease: Duration,
    pub max_run: Duration,
    pub recovery_interval: Duration,
    pub max_concurrent_recoveries: u32,
    pub run_migrations: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            lease: Duration::from_secs(30),
            max_run: Duration::from_secs(3600),
            recovery_interval: Duration::from_secs(1),
            max_concurrent_recoveries: 50,
            run_migrations: true,
        }
    }
}

#[derive(Debug)]
pub struct Run {
    lease: Lease,
    last_step_id: AtomicU32,
}

impl Run {
    pub fn new(lease: Lease) -> Self {
        Self {
            lease,
            last_step_id: AtomicU32::new(0),
        }
    }

    pub fn workflow_id(&self) -> &str {
        &self.lease.workflow_id
    }

    pub fn token(&self) -> &str {
        &self.lease.token
    }

    pub fn next_step_id(&self) -> u32 {
        self.last_step_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// A child started inside a step belongs to that step's result, so it takes no step ID.
    pub fn child_step_id(&self, in_step: bool) -> Option<u32> {
        (!in_step).then(|| self.next_step_id())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ParentStep<'a> {
    pub run: &'a Run,
    pub step_id: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct Begin<'a> {
    pub name: &'a str,
    pub input: &'a str,
    pub max_recovery_attempts: u32,
    pub workflow_id: Option<&'a str>,
    pub parent: Option<ParentStep<'a>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FinalOutcome {
    Success(String),
    Error(String),
    MaxRecoveryAttemptsExceeded(u32),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowResult {
    pub workflow_id: String,
    pub name: String,
    pub outcome: FinalOutcome,
}

#[derive(Debug)]
pub enum Begun {
    Run(Run),
    Stored(WorkflowResult),
    Attach(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StepDecision {
    Run,
    ReplayOutput(String),
    ReplayError(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Finished {
    Accepted,
    Superseded(WorkflowResult),
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

    pub fn retries_allowed(&self) -> bool {
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

#[derive(Debug)]
pub struct ActiveRun {
    keeper: LeaseKeeper,
    active: Arc<AtomicUsize>,
    stopped: AtomicBool,
}

impl ActiveRun {
    pub async fn lost(&self) -> LeaseLoss {
        self.keeper.lost().await
    }

    pub fn stop(&self) {
        self.keeper.stop();
        if !self.stopped.swap(true, Ordering::SeqCst) {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl Drop for ActiveRun {
    fn drop(&mut self) {
        self.stop();
    }
}

fn final_result(workflow: &Workflow) -> Result<Option<WorkflowResult>, Error> {
    let missing = |field: &str| {
        Error::Corrupt(format!(
            "workflow {} has no stored {field}",
            workflow.workflow_id
        ))
    };
    let outcome = match workflow.status {
        Status::Pending => return Ok(None),
        Status::Success => {
            FinalOutcome::Success(workflow.output.clone().ok_or_else(|| missing("output"))?)
        }
        Status::Error => {
            FinalOutcome::Error(workflow.error.clone().ok_or_else(|| missing("error"))?)
        }
        Status::MaxRecoveryAttemptsExceeded => {
            FinalOutcome::MaxRecoveryAttemptsExceeded(workflow.max_recovery_attempts)
        }
    };
    Ok(Some(WorkflowResult {
        workflow_id: workflow.workflow_id.clone(),
        name: workflow.name.clone(),
        outcome,
    }))
}

fn jittered(interval: Duration) -> Duration {
    let random = f64::from(uuid::Uuid::new_v4().as_bytes()[0]) / 255.0;
    interval.mul_f64(0.5 + random)
}

#[derive(Clone, Debug)]
pub struct Engine {
    store: Store,
    config: EngineConfig,
    active: Arc<AtomicUsize>,
    closing: Arc<AtomicBool>,
}

impl Engine {
    pub async fn connect(url: &str, config: EngineConfig) -> Result<Self, Error> {
        let store = Store::connect(url).await?;
        if config.run_migrations {
            store.migrate().await?;
        }
        Ok(Self {
            store,
            config,
            active: Arc::new(AtomicUsize::new(0)),
            closing: Arc::new(AtomicBool::new(false)),
        })
    }

    /// After this, a failure may just be the shutdown, so failures are no longer stored as final.
    pub fn close(&self) {
        self.closing.store(true, Ordering::SeqCst);
    }

    fn is_closing(&self) -> bool {
        self.closing.load(Ordering::SeqCst)
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn active_runs(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    pub async fn begin(&self, begin: Begin<'_>) -> Result<Begun, Error> {
        let workflow_id = match begin.parent {
            Some(parent) => {
                self.child_workflow_id(parent, begin.name, begin.workflow_id)
                    .await?
            }
            None => begin
                .workflow_id
                .filter(|id| !id.is_empty())
                .map_or_else(new_token, str::to_owned),
        };
        let new = NewWorkflow {
            workflow_id: &workflow_id,
            name: begin.name,
            input: begin.input,
            max_recovery_attempts: begin.max_recovery_attempts,
            lease: self.config.lease,
            parent_workflow_id: begin.parent.map(|parent| parent.run.workflow_id()),
        };
        match self.store.start_workflow(&new).await? {
            Started::Owned(lease) => Ok(Begun::Run(Run::new(lease))),
            Started::Existing(existing) => Ok(match final_result(&existing)? {
                Some(result) => Begun::Stored(result),
                None => Begun::Attach(workflow_id),
            }),
        }
    }

    async fn child_workflow_id(
        &self,
        parent: ParentStep<'_>,
        name: &str,
        explicit: Option<&str>,
    ) -> Result<String, Error> {
        let parent_id = parent.run.workflow_id();
        if let Some(saved) = self
            .store
            .load_step(parent_id, parent.step_id, name)
            .await?
        {
            return saved
                .child_workflow_id
                .ok_or_else(|| Error::StepKindMismatch {
                    workflow_id: parent_id.to_owned(),
                    step_id: parent.step_id,
                    expected: "child workflow",
                });
        }
        let child_id = explicit
            .filter(|id| !id.is_empty())
            .map_or_else(|| format!("{parent_id}-{}", parent.step_id), str::to_owned);
        let record = StepRecord {
            child_workflow_id: Some(child_id.clone()),
            ..StepRecord::default()
        };
        self.store
            .save_step(&parent.run.lease, parent.step_id, name, &record)
            .await?;
        Ok(child_id)
    }

    pub async fn check_step(
        &self,
        run: &Run,
        step_id: u32,
        name: &str,
    ) -> Result<StepDecision, Error> {
        let Some(saved) = self
            .store
            .load_step(run.workflow_id(), step_id, name)
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

    pub async fn finish(&self, run: &Run, outcome: Outcome) -> Result<Finished, Error> {
        if matches!(outcome, Outcome::Error(_)) && self.is_closing() {
            self.release(run).await?;
            return Err(Error::ShuttingDown);
        }
        match self.store.finish_workflow(&run.lease, &outcome).await {
            Ok(()) => Ok(Finished::Accepted),
            Err(Error::LeaseLost(_)) => {
                log::warn!(
                    "workflow {} finished after losing its lease, so its result was discarded",
                    run.workflow_id()
                );
                Ok(Finished::Superseded(
                    self.await_result(run.workflow_id()).await?,
                ))
            }
            Err(error) => Err(error),
        }
    }

    pub async fn release(&self, run: &Run) -> Result<(), Error> {
        match self.store.release_lease(&run.lease).await {
            Err(Error::LeaseLost(_)) => Ok(()),
            other => other,
        }
    }

    pub fn keep_lease(&self, run: &Run) -> ActiveRun {
        self.active.fetch_add(1, Ordering::SeqCst);
        ActiveRun {
            keeper: self.store.keep_lease(
                run.lease.clone(),
                self.config.lease,
                self.config.max_run,
            ),
            active: self.active.clone(),
            stopped: AtomicBool::new(false),
        }
    }

    pub async fn await_result(&self, workflow_id: &str) -> Result<WorkflowResult, Error> {
        let mut delay = POLL_MIN;
        loop {
            let workflow = self.get_status(workflow_id).await?;
            if let Some(result) = final_result(&workflow)? {
                return Ok(result);
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(POLL_MAX);
        }
    }

    pub async fn get_status(&self, workflow_id: &str) -> Result<Workflow, Error> {
        self.store
            .get_workflow(workflow_id)
            .await?
            .ok_or_else(|| Error::WorkflowNotFound(workflow_id.to_owned()))
    }

    pub async fn list_workflows(&self) -> Result<Vec<Workflow>, Error> {
        self.store.list_workflows().await
    }

    pub async fn list_steps(&self, workflow_id: &str) -> Result<Vec<RecordedStep>, Error> {
        self.store.list_steps(workflow_id).await
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(retries_allowed: bool) -> StepPolicy {
        StepPolicy::new(retries_allowed, 1.0, 3, 2.0).unwrap()
    }

    #[test]
    fn delays_back_off_with_no_delay_after_the_final_attempt() {
        let retried = policy(true);
        assert_eq!(retried.retry_delay(1), Some(Duration::from_secs(1)));
        assert_eq!(retried.retry_delay(2), Some(Duration::from_secs(2)));
        assert_eq!(retried.retry_delay(3), None);
    }

    #[test]
    fn steps_without_retries_never_wait() {
        assert_eq!(policy(false).retry_delay(1), None);
    }

    #[test]
    fn status_reports_zero_based_attempts_only_when_retrying() {
        assert_eq!(policy(true).status(1), (Some(0), Some(3)));
        assert_eq!(policy(true).status(3), (Some(2), Some(3)));
        assert_eq!(policy(false).status(1), (None, None));
    }

    #[test]
    fn enormous_backoff_does_not_panic() {
        let huge = StepPolicy::new(true, 1.0e300, u32::MAX, 1.0e300).unwrap();
        assert_eq!(huge.retry_delay(5), Some(Duration::MAX));
    }

    #[test]
    fn invalid_policies_are_rejected() {
        assert!(StepPolicy::new(true, -1.0, 3, 2.0).is_err());
        assert!(StepPolicy::new(true, f64::NAN, 3, 2.0).is_err());
        assert!(StepPolicy::new(true, 1.0, 3, -2.0).is_err());
        assert!(StepPolicy::new(true, 1.0, 0, 2.0).is_err());
    }

    #[test]
    fn a_child_takes_a_step_id_only_outside_a_step() {
        let run = Run::new(Lease {
            workflow_id: "w".to_owned(),
            token: "t".to_owned(),
        });
        assert_eq!(run.child_step_id(false), Some(1));
        assert_eq!(run.child_step_id(true), None);
        assert_eq!(run.child_step_id(false), Some(2));
    }

    #[test]
    fn step_ids_start_at_one_and_never_repeat() {
        let run = Arc::new(Run::new(Lease {
            workflow_id: "a".to_owned(),
            token: "t".to_owned(),
        }));
        assert_eq!(run.next_step_id(), 1);
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let run = run.clone();
                std::thread::spawn(move || (0..100).map(|_| run.next_step_id()).collect::<Vec<_>>())
            })
            .collect();
        let mut seen: Vec<u32> = threads
            .into_iter()
            .flat_map(|thread| thread.join().unwrap())
            .collect();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), 800);
        assert_eq!(seen[0], 2);
    }
}
