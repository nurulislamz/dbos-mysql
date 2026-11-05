use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::Notify;

use crate::lease::{LeaseKeeper, LeaseLoss};
use crate::workflow::{
    Lease, NewWorkflow, Outcome, PORTABLE_JSON, PY_PICKLE, RecordedStep, Started, Status,
    StepRecord, Workflow, WorkflowFilter, WorkflowOptions, new_token,
};
use crate::{Error, Store};

mod attributes;
mod events;
mod queues;
mod recovery;
mod retention;
mod schedules;
mod sleep;
mod steps;
mod timeouts;
mod wait_first;
mod wakeup;
mod workflow_management;

pub use crate::store::{EventRow, MessageRow, Poll, SendOutcome, StoredValue};
pub use steps::{StepAttempts, StepDecision, StepPolicy};
use wakeup::{WakeHub, WakeKey};

const POLL_MIN: Duration = Duration::from_millis(10);
// SQLite and MySQL have no LISTEN/NOTIFY, so this bounds how late a change from another process is seen.
const POLL_MAX: Duration = Duration::from_millis(250);

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub lease: Duration,
    pub max_run: Duration,
    pub recovery_interval: Duration,
    pub max_concurrent_recoveries: u32,
    pub run_migrations: bool,
    pub pickle: bool,
    pub read_only: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            lease: Duration::from_secs(30),
            max_run: Duration::from_secs(3600),
            recovery_interval: Duration::from_secs(1),
            max_concurrent_recoveries: 50,
            run_migrations: true,
            pickle: true,
            read_only: false,
        }
    }
}

#[derive(Debug)]
pub struct Run {
    lease: Lease,
    last_step_id: AtomicU32,
    /// Step IDs drawn by `reserve_step_id` and not yet claimed by a run.
    reserved_step_ids: Mutex<HashSet<u32>>,
    serialization: String,
    deadline_epoch_ms: Option<i64>,
}

impl Run {
    pub fn new(lease: Lease, serialization: String, deadline_epoch_ms: Option<i64>) -> Self {
        Self {
            lease,
            last_step_id: AtomicU32::new(0),
            reserved_step_ids: Mutex::new(HashSet::new()),
            serialization,
            deadline_epoch_ms,
        }
    }

    pub fn workflow_id(&self) -> &str {
        &self.lease.workflow_id
    }

    pub fn token(&self) -> &str {
        &self.lease.token
    }

    /// A workflow's values all use the format it started with, even after a config change.
    pub fn serialization(&self) -> &str {
        &self.serialization
    }

    pub fn deadline_epoch_ms(&self) -> Option<i64> {
        self.deadline_epoch_ms
    }

    pub fn next_step_id(&self) -> u32 {
        self.last_step_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Work done inside a step belongs to that step's result, so it takes no step ID.
    pub fn step_id_unless_in_step(&self, in_step: bool) -> Option<u32> {
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
    pub timeout_ms: Option<i64>,
    pub ignore_propagated_deadline: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FinalOutcome {
    Success(String),
    Error(String),
    MaxRecoveryAttemptsExceeded(u32),
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowResult {
    pub workflow_id: String,
    pub name: String,
    pub outcome: FinalOutcome,
    pub serialization: String,
}

#[derive(Debug)]
pub enum Begun {
    Run(Run),
    Stored(WorkflowResult),
    Attach(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Ended {
    Success(String),
    Error(String),
    Interrupted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Finished {
    Accepted,
    Superseded(WorkflowResult),
}

/// This engine's running lease keepers. A workflow can have two, when a run has not yet
/// noticed that it was replaced.
#[derive(Debug, Default)]
struct LocalRuns(Mutex<HashMap<String, Vec<Arc<Notify>>>>);

impl LocalRuns {
    fn lock(&self) -> MutexGuard<'_, HashMap<String, Vec<Arc<Notify>>>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn poke(&self, workflow_id: &str) {
        if let Some(pokes) = self.lock().get(workflow_id) {
            pokes.iter().for_each(|poke| poke.notify_one());
        }
    }

    fn remove(&self, workflow_id: &str, poke: &Arc<Notify>) {
        let mut runs = self.lock();
        if let Some(pokes) = runs.get_mut(workflow_id) {
            pokes.retain(|known| !Arc::ptr_eq(known, poke));
            if pokes.is_empty() {
                runs.remove(workflow_id);
            }
        }
    }
}

#[derive(Debug)]
pub struct ActiveRun {
    keeper: LeaseKeeper,
    active: Arc<AtomicUsize>,
    stopped: AtomicBool,
    local_runs: Arc<LocalRuns>,
    workflow_id: String,
}

impl ActiveRun {
    pub async fn lost(&self) -> LeaseLoss {
        self.keeper.lost().await
    }

    pub fn stop(&self) {
        self.keeper.stop();
        if !self.stopped.swap(true, Ordering::SeqCst) {
            self.active.fetch_sub(1, Ordering::SeqCst);
            self.local_runs
                .remove(&self.workflow_id, &self.keeper.poker());
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
        Status::Pending | Status::Enqueued | Status::Delayed => return Ok(None),
        Status::Success => {
            FinalOutcome::Success(workflow.output.clone().ok_or_else(|| missing("output"))?)
        }
        Status::Error => {
            FinalOutcome::Error(workflow.error.clone().ok_or_else(|| missing("error"))?)
        }
        Status::MaxRecoveryAttemptsExceeded => {
            FinalOutcome::MaxRecoveryAttemptsExceeded(workflow.max_recovery_attempts)
        }
        Status::Cancelled => FinalOutcome::Cancelled,
    };
    Ok(Some(WorkflowResult {
        workflow_id: workflow.workflow_id.clone(),
        name: workflow.name.clone(),
        outcome,
        serialization: workflow.serialization.clone(),
    }))
}

#[derive(Clone, Debug)]
pub struct Engine {
    store: Store,
    config: EngineConfig,
    active: Arc<AtomicUsize>,
    closing: Arc<AtomicBool>,
    last_unreadable_warn_ms: Arc<AtomicU64>,
    wakeups: Arc<WakeHub>,
    local_runs: Arc<LocalRuns>,
}

impl Engine {
    pub async fn connect(url: &str, config: EngineConfig) -> Result<Self, Error> {
        let store = if config.read_only {
            Store::connect_read_only(url).await?
        } else {
            Store::connect(url).await?
        };
        let store = if config.pickle {
            store.with_pickle()
        } else {
            store
        };
        let store = store.with_identity(crate::Identity {
            executor_id: Some(new_token()),
            ..Default::default()
        });
        // Store::connect_read_only has already checked the schema.
        if !config.read_only {
            if config.run_migrations {
                store.migrate().await?;
            } else {
                store.check_schema().await?;
            }
        }
        Ok(Self {
            store,
            config,
            active: Arc::new(AtomicUsize::new(0)),
            closing: Arc::new(AtomicBool::new(false)),
            last_unreadable_warn_ms: Arc::new(AtomicU64::new(0)),
            wakeups: Arc::default(),
            local_runs: Arc::default(),
        })
    }

    /// After this, a failure may just be the shutdown, so failures are no longer stored as final.
    pub fn close(&self) {
        self.closing.store(true, Ordering::SeqCst);
        self.wakeups.wake_all();
    }

    fn is_closing(&self) -> bool {
        self.closing.load(Ordering::SeqCst)
    }

    pub fn serialization(&self) -> &'static str {
        if self.config.pickle {
            PY_PICKLE
        } else {
            PORTABLE_JSON
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn active_runs(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    pub async fn begin(&self, begin: Begin<'_>) -> Result<Begun, Error> {
        self.begin_with(begin, &WorkflowOptions::default()).await
    }

    pub async fn begin_with(
        &self,
        begin: Begin<'_>,
        options: &WorkflowOptions,
    ) -> Result<Begun, Error> {
        let (timeout_ms, deadline_epoch_ms) = self.timeout_deadline(&begin)?;
        let workflow_id = self.workflow_id_for_begin(&begin).await?;
        let new = NewWorkflow {
            workflow_id: &workflow_id,
            name: begin.name,
            input: begin.input,
            max_recovery_attempts: begin.max_recovery_attempts,
            lease: self.config.lease,
            parent_workflow_id: begin.parent.map(|parent| parent.run.workflow_id()),
            serialization: self.serialization(),
            timeout_ms,
            deadline_epoch_ms,
        };
        match self.store.start_workflow_with(&new, options).await? {
            Started::Owned(lease, deadline_epoch_ms) => Ok(Begun::Run(Run::new(
                lease,
                self.serialization().to_owned(),
                deadline_epoch_ms,
            ))),
            Started::Existing(existing) => Ok(match final_result(&existing)? {
                Some(result) => Begun::Stored(result),
                None => Begun::Attach(workflow_id),
            }),
        }
    }

    async fn workflow_id_for_begin(&self, begin: &Begin<'_>) -> Result<String, Error> {
        match begin.parent {
            Some(parent) => {
                self.child_workflow_id(parent, begin.name, begin.workflow_id)
                    .await
            }
            None => Ok(begin
                .workflow_id
                .filter(|id| !id.is_empty())
                .map_or_else(new_token, str::to_owned)),
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
            .load_step_owned(&parent.run.lease, parent.step_id, name)
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

    /// What happens when a workflow's function stops, however it stopped.
    pub async fn conclude(&self, run: &Run, ended: Ended) -> Result<Finished, Error> {
        match ended {
            Ended::Success(output) => self.finish(run, Outcome::Success(output)).await,
            Ended::Error(error) => self.finish(run, Outcome::Error(error)).await,
            Ended::Interrupted => {
                self.release(run).await?;
                Ok(Finished::Accepted)
            }
        }
    }

    pub async fn finish(&self, run: &Run, outcome: Outcome) -> Result<Finished, Error> {
        if matches!(outcome, Outcome::Error(_)) && self.is_closing() {
            self.release(run).await?;
            return Err(Error::ShuttingDown);
        }
        match self.store.finish_workflow(&run.lease, &outcome).await {
            Ok(()) => {
                self.wakeups
                    .wake(&WakeKey::Workflow(run.workflow_id().to_owned()));
                Ok(Finished::Accepted)
            }
            Err(Error::LeaseLost(_) | Error::WorkflowCancelled(_)) => {
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
        let (store, lease, duration) = (self.store.clone(), run.lease.clone(), self.config.lease);
        let (releasing, release_lease) = (store.clone(), lease.clone());
        // Give the workflow back before anyone hears the loss. Stopping renewals
        // alone would leave it owned until the old `claim_after`.
        let keeper = LeaseKeeper::spawn_until(
            duration,
            self.config.max_run,
            move || {
                let (store, lease) = (store.clone(), lease.clone());
                async move { store.renew_lease(&lease, duration).await }
            },
            move || {
                let (store, lease) = (releasing.clone(), release_lease.clone());
                async move {
                    if let Err(error) = store.release_lease(&lease).await
                        && !matches!(error, Error::LeaseLost(_))
                    {
                        log::warn!(
                            "could not release workflow {} after its max run time: {error}",
                            lease.workflow_id
                        );
                    }
                }
            },
        );
        self.local_runs
            .lock()
            .entry(run.lease.workflow_id.clone())
            .or_default()
            .push(keeper.poker());
        ActiveRun {
            keeper,
            active: self.active.clone(),
            stopped: AtomicBool::new(false),
            local_runs: self.local_runs.clone(),
            workflow_id: run.lease.workflow_id.clone(),
        }
    }

    pub async fn await_result(&self, workflow_id: &str) -> Result<WorkflowResult, Error> {
        self.await_result_within(workflow_id, None).await
    }

    /// `interval` is the longest wait between polls. `None` keeps the engine cap. A local wake
    /// still returns at once.
    pub async fn await_result_within(
        &self,
        workflow_id: &str,
        interval: Option<Duration>,
    ) -> Result<WorkflowResult, Error> {
        let interval = interval.unwrap_or(POLL_MAX);
        if interval.is_zero() {
            return Err(Error::InvalidConfig(
                "polling_interval_sec must be positive".to_owned(),
            ));
        }
        let waiter = self
            .wakeups
            .waiter(vec![WakeKey::Workflow(workflow_id.to_owned())]);
        let mut delay = POLL_MIN.min(interval);
        loop {
            let workflow = self.get_status(workflow_id).await?;
            if let Some(result) = final_result(&workflow)? {
                return Ok(result);
            }
            waiter.wait(delay).await;
            delay = (delay * 2).min(interval);
        }
    }

    pub async fn get_status(&self, workflow_id: &str) -> Result<Workflow, Error> {
        if !self.config.read_only {
            self.store.cancel_if_expired(workflow_id).await?;
        }
        self.store
            .get_workflow(workflow_id)
            .await?
            .ok_or_else(|| Error::WorkflowNotFound(workflow_id.to_owned()))
    }

    pub async fn list_workflows(&self, filter: &WorkflowFilter) -> Result<Vec<Workflow>, Error> {
        self.store.list_workflows(filter).await
    }

    pub async fn list_steps(&self, workflow_id: &str) -> Result<Vec<RecordedStep>, Error> {
        self.store.list_steps(workflow_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_stopped_run_leaves_the_local_registry() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("a.db").display());
        let engine = Engine::connect(&url, EngineConfig::default())
            .await
            .unwrap();
        let run = |token: &str| {
            let lease = Lease {
                workflow_id: "a".to_owned(),
                token: token.to_owned(),
            };
            Run::new(lease, PORTABLE_JSON.to_owned(), None)
        };
        let (first, second) = (engine.keep_lease(&run("1")), engine.keep_lease(&run("2")));
        assert_eq!(engine.local_runs.lock()["a"].len(), 2);
        first.stop();
        first.stop();
        assert_eq!(engine.local_runs.lock()["a"].len(), 1);
        drop(first);
        assert_eq!(engine.local_runs.lock()["a"].len(), 1);
        drop(second);
        assert!(engine.local_runs.lock().is_empty());
    }

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
    fn a_step_id_is_only_taken_outside_a_step() {
        let run = Run::new(
            Lease {
                workflow_id: "w".to_owned(),
                token: "t".to_owned(),
            },
            PORTABLE_JSON.to_owned(),
            None,
        );
        assert_eq!(run.step_id_unless_in_step(false), Some(1));
        assert_eq!(run.step_id_unless_in_step(true), None);
        assert_eq!(run.step_id_unless_in_step(false), Some(2));
    }

    #[test]
    fn a_reserved_step_id_is_claimed_once_and_never_escapes_a_step() {
        let run = Run::new(
            Lease {
                workflow_id: "w".into(),
                token: "t".into(),
            },
            PORTABLE_JSON.into(),
            None,
        );
        let reserved = run.reserve_step_id();
        assert_eq!(run.step_id_for_call(true, Some(reserved)).unwrap(), None);
        assert_eq!(
            run.step_id_for_call(false, Some(reserved)).unwrap(),
            Some(1)
        );
        assert!(matches!(
            run.step_id_for_call(false, Some(reserved)),
            Err(Error::StepReservationUsed(1))
        ));
        assert_eq!(run.step_id_for_call(false, None).unwrap(), Some(2));
    }

    #[test]
    fn step_ids_start_at_one_and_never_repeat() {
        let run = Arc::new(Run::new(
            Lease {
                workflow_id: "a".to_owned(),
                token: "t".to_owned(),
            },
            PORTABLE_JSON.to_owned(),
            None,
        ));
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
