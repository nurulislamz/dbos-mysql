use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use dwos_core::{
    ActiveRun, Begin, Begun, Claimed, DEFAULT_MAX_RECOVERY_ATTEMPTS, Ended, Engine, EngineConfig,
    Error, FinalOutcome, Finished, PORTABLE_JSON, PY_PICKLE, ParentStep, RecordedStep, ReusePolicy,
    Run, StepAttempts, StepDecision, StepExpiry, StepPolicy, StepWatch, Workflow, WorkflowOptions,
    WorkflowResult, pickle_from_serializer, recovery_limit, recovery_limit_of,
};
use pyo3::IntoPyObjectExt;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::{future_into_py, get_runtime};

mod attributes;
mod events;
mod queues;
mod retention;
mod schedules;
mod sleep;
mod wait_first;
mod workflow_management;

create_exception!(_native, DBOSException, PyException);
create_exception!(_native, StoreError, DBOSException);
create_exception!(_native, DBOSConflictingWorkflowError, DBOSException);
create_exception!(_native, DBOSUnexpectedStepError, DBOSException);
create_exception!(_native, DBOSStepNondeterminismError, DBOSException);
create_exception!(_native, DBOSNonExistentWorkflowError, DBOSException);
create_exception!(_native, DBOSLeaseLostError, DBOSException);
create_exception!(_native, DBOSWorkflowCancelledError, DBOSException);
create_exception!(_native, DBOSWorkflowIDInUseError, DBOSException);
create_exception!(_native, DBOSQueueDeduplicatedError, DBOSException);

pub(crate) fn to_py(error: Error) -> PyErr {
    let message = error.to_string();
    match error {
        Error::ConflictingWorkflow { .. } => DBOSConflictingWorkflowError::new_err(message),
        Error::QueueDeduplicated { .. } => DBOSQueueDeduplicatedError::new_err(message),
        Error::WorkflowIdInUse {
            workflow_id,
            status,
            name,
        } => Python::attach(|py| {
            let error = DBOSWorkflowIDInUseError::new_err(message);
            let value = error.value(py);
            let _ = value.setattr("workflow_id", workflow_id);
            let _ = value.setattr("workflow_status", status);
            let _ = value.setattr("workflow_name", name);
            error
        }),
        Error::UnexpectedStep { .. } | Error::StepKindMismatch { .. } => {
            DBOSUnexpectedStepError::new_err(message)
        }
        Error::StepNondeterminism { .. } => DBOSStepNondeterminismError::new_err(message),
        Error::WorkflowNotFound(_) | Error::SendDestinationNotFound(_) => {
            DBOSNonExistentWorkflowError::new_err(message)
        }
        Error::WorkflowCancelled(_) => DBOSWorkflowCancelledError::new_err(message),
        Error::LeaseLost(_) => DBOSLeaseLostError::new_err(message),
        Error::ShuttingDown => DBOSException::new_err(message),
        _ => StoreError::new_err(message),
    }
}

/// Awaitable by default. `blocking` waits on this thread with the GIL released, which skips the
/// event-loop thread hops a sync caller would otherwise pay on every call.
pub(crate) fn dispatch<'py, T, F>(
    py: Python<'py>,
    blocking: bool,
    future: F,
) -> PyResult<Bound<'py, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'a> IntoPyObject<'a> + Send + 'static,
{
    if blocking {
        py.detach(|| get_runtime().block_on(future))?
            .into_bound_py_any(py)
    } else {
        future_into_py(py, future)
    }
}

pub(crate) fn seconds(value: f64) -> PyResult<Duration> {
    Duration::try_from_secs_f64(value)
        .map_err(|error| StoreError::new_err(format!("invalid duration: {error}")))
}

#[pyclass(name = "Run", frozen)]
pub(crate) struct PyRun(pub(crate) Arc<Run>);

#[pymethods]
impl PyRun {
    #[getter]
    fn workflow_id(&self) -> &str {
        self.0.workflow_id()
    }

    #[getter]
    fn serialization(&self) -> &str {
        self.0.serialization()
    }

    #[getter]
    fn workflow_deadline_epoch_ms(&self) -> Option<i64> {
        self.0.deadline_epoch_ms()
    }

    fn step_id_unless_in_step(&self, in_step: bool) -> Option<u32> {
        self.0.step_id_unless_in_step(in_step)
    }
}

#[pyclass(name = "WorkflowRow", frozen, get_all)]
pub(crate) struct PyWorkflow {
    workflow_id: String,
    name: String,
    status: &'static str,
    input: String,
    output: Option<String>,
    error: Option<String>,
    recovery_attempts: u32,
    max_recovery_attempts: Option<u32>,
    parent_workflow_id: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
    completed_at_ms: Option<i64>,
    serialization: String,
    workflow_timeout_ms: Option<i64>,
    workflow_deadline_epoch_ms: Option<i64>,
    application_version: Option<String>,
    application_name: Option<String>,
    executor_id: Option<String>,
    forked_from: Option<String>,
    schedule_name: Option<String>,
    authenticated_user: Option<String>,
    assumed_role: Option<String>,
    authenticated_roles: Option<String>,
    attributes: Option<String>,
    queue_name: Option<String>,
    priority: Option<i64>,
    deduplication_id: Option<String>,
    queue_partition_key: Option<String>,
    dequeued_at_ms: Option<i64>,
    delay_until_ms: Option<i64>,
}

impl From<Workflow> for PyWorkflow {
    fn from(row: Workflow) -> Self {
        Self {
            workflow_id: row.workflow_id,
            name: row.name,
            status: row.status.as_str(),
            input: row.input,
            output: row.output,
            error: row.error,
            recovery_attempts: row.recovery_attempts,
            max_recovery_attempts: recovery_limit_of(row.max_recovery_attempts),
            parent_workflow_id: row.parent_workflow_id,
            created_at_ms: row.created_at_ms,
            updated_at_ms: row.updated_at_ms,
            completed_at_ms: row.completed_at_ms,
            serialization: row.serialization,
            workflow_timeout_ms: row.timeout_ms,
            workflow_deadline_epoch_ms: row.deadline_epoch_ms,
            application_version: row.application_version,
            application_name: row.application_name,
            executor_id: row.executor_id,
            forked_from: row.forked_from,
            schedule_name: row.schedule_name,
            authenticated_user: row.authenticated_user,
            assumed_role: row.assumed_role,
            authenticated_roles: row.authenticated_roles,
            attributes: row.attributes,
            queue_name: row.queue_name,
            priority: row.priority,
            deduplication_id: row.deduplication_id,
            queue_partition_key: row.queue_partition_key,
            dequeued_at_ms: row.dequeued_at_ms,
            delay_until_ms: row.delay_until_ms,
        }
    }
}

#[pyclass(name = "StepRow", frozen, get_all)]
struct PyStep {
    started_at_epoch_ms: Option<i64>,
    completed_at_epoch_ms: i64,
    step_id: u32,
    step_name: String,
    output: Option<String>,
    error: Option<String>,
    child_workflow_id: Option<String>,
    serialization: String,
}

impl From<RecordedStep> for PyStep {
    fn from(step: RecordedStep) -> Self {
        Self {
            started_at_epoch_ms: step.started_at_epoch_ms,
            completed_at_epoch_ms: step.completed_at_epoch_ms,
            step_id: step.step_id,
            step_name: step.step_name,
            output: step.record.output,
            error: step.record.error,
            child_workflow_id: step.record.child_workflow_id,
            serialization: step.serialization,
        }
    }
}

#[pyclass(name = "WorkflowResult", frozen, get_all)]
struct PyResult_ {
    workflow_id: String,
    name: String,
    kind: &'static str,
    value: Option<String>,
    max_recovery_attempts: Option<u32>,
    serialization: String,
}

impl From<WorkflowResult> for PyResult_ {
    fn from(result: WorkflowResult) -> Self {
        let (kind, value, max_recovery_attempts) = match result.outcome {
            FinalOutcome::Success(value) => ("success", Some(value), None),
            FinalOutcome::Error(value) => ("error", Some(value), None),
            FinalOutcome::MaxRecoveryAttemptsExceeded(max) => {
                ("max_recovery_attempts", None, Some(max))
            }
            FinalOutcome::Cancelled => ("cancelled", None, None),
        };
        Self {
            workflow_id: result.workflow_id,
            name: result.name,
            kind,
            value,
            max_recovery_attempts,
            serialization: result.serialization,
        }
    }
}

#[pyclass(name = "Begun", frozen, get_all)]
struct PyBegun {
    workflow_id: String,
    run: Option<Py<PyRun>>,
    result: Option<Py<PyResult_>>,
}

#[pyclass(name = "StepReplay", frozen, get_all)]
struct PyStepReplay {
    output: Option<String>,
    error: Option<String>,
}

#[pyclass(name = "Claim", frozen, get_all)]
pub(crate) struct PyClaim {
    workflow_id: String,
    run: Py<PyRun>,
    name: String,
    input: String,
    recovery_attempts: u32,
}

pub(crate) fn claims_to_py(claims: Vec<Claimed>) -> PyResult<Vec<PyClaim>> {
    Python::attach(|py| {
        claims
            .into_iter()
            .map(|claim| {
                Ok(PyClaim {
                    workflow_id: claim.lease.workflow_id.clone(),
                    run: Py::new(
                        py,
                        PyRun(Arc::new(Run::new(
                            claim.lease,
                            claim.serialization,
                            claim.deadline_epoch_ms,
                        ))),
                    )?,
                    name: claim.name,
                    input: claim.input,
                    recovery_attempts: claim.recovery_attempts,
                })
            })
            .collect()
    })
}

fn finished_to_py(finished: Finished) -> Option<PyResult_> {
    match finished {
        Finished::Accepted => None,
        Finished::Superseded(result) => Some(result.into()),
    }
}

#[pyclass(name = "ActiveRun", frozen)]
pub(crate) struct PyActiveRun(pub(crate) Arc<ActiveRun>);

#[pymethods]
impl PyActiveRun {
    fn lost<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let active = self.0.clone();
        future_into_py(py, async move { Ok(active.lost().await.as_str()) })
    }

    /// Waits up to `seconds` without the GIL. Returns why the lease was lost, or `None` if it was not.
    fn wait_lost(&self, py: Python<'_>, seconds: f64) -> PyResult<Option<&'static str>> {
        let wait = self::seconds(seconds)?;
        let active = self.0.clone();
        Ok(py.detach(|| {
            get_runtime().block_on(async {
                tokio::time::timeout(wait, active.lost())
                    .await
                    .ok()
                    .map(|loss| loss.as_str())
            })
        }))
    }

    fn stop(&self) {
        self.0.stop();
    }
}

#[pyclass(name = "StepPolicy", frozen)]
struct PyStepPolicy(StepPolicy);

#[pymethods]
impl PyStepPolicy {
    #[new]
    #[pyo3(signature = (retries_allowed, interval_seconds, max_attempts, backoff_rate, timeout_seconds=None, heartbeat_timeout_seconds=None))]
    fn new(
        retries_allowed: bool,
        interval_seconds: f64,
        max_attempts: u32,
        backoff_rate: f64,
        timeout_seconds: Option<f64>,
        heartbeat_timeout_seconds: Option<f64>,
    ) -> PyResult<Self> {
        StepPolicy::new(
            retries_allowed,
            interval_seconds,
            max_attempts,
            backoff_rate,
        )
        .and_then(|policy| policy.with_timeouts(timeout_seconds, heartbeat_timeout_seconds))
        .map(Self)
        .map_err(to_py)
    }

    #[getter]
    fn wraps_failures(&self) -> bool {
        self.0.wraps_failures()
    }

    #[getter]
    fn max_attempts(&self) -> u32 {
        self.0.max_attempts()
    }

    fn attempts(&self) -> PyStepAttempts {
        PyStepAttempts(self.0.attempts())
    }

    fn watch(&self) -> Option<PyStepWatch> {
        self.0.watch().map(|watch| PyStepWatch(Arc::new(watch)))
    }
}

/// `beat` and `finish` return why the attempt expired, or `None` if it has not.
#[pyclass(name = "StepWatch", frozen)]
struct PyStepWatch(Arc<StepWatch>);

#[pymethods]
impl PyStepWatch {
    fn beat(&self) -> Option<&'static str> {
        self.0.beat().err().map(StepExpiry::as_str)
    }

    fn finish(&self) -> Option<&'static str> {
        self.0.finish().err().map(StepExpiry::as_str)
    }

    #[pyo3(signature = (*, blocking=false))]
    fn expired<'py>(&self, py: Python<'py>, blocking: bool) -> PyResult<Bound<'py, PyAny>> {
        let watch = self.0.clone();
        dispatch(py, blocking, async move {
            Ok(watch.expired().await.map(StepExpiry::as_str))
        })
    }
}

#[pyclass(name = "StepAttempts")]
struct PyStepAttempts(StepAttempts);

#[pymethods]
impl PyStepAttempts {
    fn status(&self) -> (Option<u32>, Option<u32>) {
        self.0.status()
    }

    #[getter]
    fn wraps_failures(&self) -> bool {
        self.0.wraps_failures()
    }

    #[pyo3(signature = (retryable=true))]
    fn failed(&mut self, retryable: bool) -> Option<f64> {
        self.0.failed(retryable).map(|delay| delay.as_secs_f64())
    }
}

#[pyclass(name = "Engine", frozen)]
pub(crate) struct PyEngine(pub(crate) Engine);

#[pymethods]
impl PyEngine {
    #[staticmethod]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (url, *, lease_seconds=None, max_run_seconds=None, recovery_interval_seconds=None, max_concurrent_recoveries=None, run_migrations=None, serializer=None, read_only=None))]
    fn connect(
        py: Python<'_>,
        url: String,
        lease_seconds: Option<f64>,
        max_run_seconds: Option<f64>,
        recovery_interval_seconds: Option<f64>,
        max_concurrent_recoveries: Option<u32>,
        run_migrations: Option<bool>,
        serializer: Option<String>,
        read_only: Option<bool>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let mut config = EngineConfig::default();
        if let Some(value) = lease_seconds {
            config.lease = seconds(value)?;
        }
        if let Some(value) = max_run_seconds {
            config.max_run = seconds(value)?;
        }
        if let Some(value) = recovery_interval_seconds {
            config.recovery_interval = seconds(value)?;
        }
        if let Some(value) = max_concurrent_recoveries {
            config.max_concurrent_recoveries = value;
        }
        if let Some(value) = run_migrations {
            config.run_migrations = value;
        }
        if let Some(value) = serializer {
            config.pickle = pickle_from_serializer(&value).map_err(to_py)?;
        }
        if let Some(value) = read_only {
            config.read_only = value;
        }
        future_into_py(py, async move {
            let engine = Engine::connect(&url, config).await.map_err(to_py)?;
            Ok(PyEngine(engine))
        })
    }

    #[getter]
    fn serialization(&self) -> &'static str {
        self.0.serialization()
    }

    fn close(&self) {
        self.0.close();
    }

    #[pyo3(signature = (name, input, max_recovery_attempts, workflow_id=None, parent=None, in_step=false, timeout_ms=None, ignore_propagated_deadline=false, reuse_policy=None, *, attributes=None, blocking=false))]
    #[allow(clippy::too_many_arguments)]
    fn begin<'py>(
        &self,
        py: Python<'py>,
        name: String,
        input: String,
        max_recovery_attempts: Option<u32>,
        workflow_id: Option<String>,
        parent: Option<PyRef<'py, PyRun>>,
        in_step: bool,
        timeout_ms: Option<i64>,
        ignore_propagated_deadline: bool,
        reuse_policy: Option<String>,
        attributes: Option<String>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        let parent = parent.map(|run| run.0.clone());
        // Taken at the call, not in the future, so concurrent calls keep their call order.
        let parent_step_id = parent
            .as_ref()
            .and_then(|run| run.step_id_unless_in_step(in_step));
        let mut options = start_options(reuse_policy.as_deref())?;
        options.attributes = attributes;
        dispatch(py, blocking, async move {
            let parent_step = match (&parent, parent_step_id) {
                (Some(run), Some(step_id)) => Some(ParentStep { run, step_id }),
                _ => None,
            };
            let begun = engine
                .begin_with(
                    Begin {
                        name: &name,
                        input: &input,
                        max_recovery_attempts: recovery_limit(max_recovery_attempts),
                        workflow_id: workflow_id.as_deref(),
                        parent: parent_step,
                        timeout_ms,
                        ignore_propagated_deadline,
                    },
                    &options,
                )
                .await
                .map_err(to_py)?;
            Python::attach(|py| {
                Ok(match begun {
                    Begun::Run(run) => PyBegun {
                        workflow_id: run.workflow_id().to_owned(),
                        run: Some(Py::new(py, PyRun(Arc::new(run)))?),
                        result: None,
                    },
                    Begun::Stored(result) => PyBegun {
                        workflow_id: result.workflow_id.clone(),
                        run: None,
                        result: Some(Py::new(py, PyResult_::from(result))?),
                    },
                    Begun::Attach(workflow_id) => PyBegun {
                        workflow_id,
                        run: None,
                        result: None,
                    },
                })
            })
        })
    }

    #[pyo3(signature = (run, name, in_step, *, reserved_step_id=None, blocking=false))]
    fn begin_step<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        name: String,
        in_step: bool,
        reserved_step_id: Option<u32>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        // Taken at the call, not in the future, so concurrent calls keep their call order.
        let step_id = run.step_id_for_call(in_step, reserved_step_id);
        dispatch(py, blocking, async move {
            let Some(step_id) = step_id else {
                return Ok((None, None));
            };
            let decision = engine
                .check_step(&run, step_id, &name)
                .await
                .map_err(to_py)?;
            let replay = match decision {
                StepDecision::Run => None,
                StepDecision::ReplayOutput(output) => Some(PyStepReplay {
                    output: Some(output),
                    error: None,
                }),
                StepDecision::ReplayError(error) => Some(PyStepReplay {
                    output: None,
                    error: Some(error),
                }),
            };
            Ok((Some(step_id), replay))
        })
    }

    #[pyo3(signature = (run, step_id, name, output, *, blocking=false))]
    fn save_step_output<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        step_id: u32,
        name: String,
        output: String,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        dispatch(py, blocking, async move {
            engine
                .save_step_output(&run, step_id, &name, output)
                .await
                .map_err(to_py)
        })
    }

    #[pyo3(signature = (run, step_id, name, error, *, blocking=false))]
    fn save_step_error<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        step_id: u32,
        name: String,
        error: String,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        dispatch(py, blocking, async move {
            engine
                .save_step_error(&run, step_id, &name, error)
                .await
                .map_err(to_py)
        })
    }

    #[pyo3(signature = (run, *, output=None, error=None, blocking=false))]
    fn conclude<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        output: Option<String>,
        error: Option<String>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        let ended = match (output, error) {
            (Some(output), None) => Ended::Success(output),
            (None, Some(error)) => Ended::Error(error),
            (None, None) => Ended::Interrupted,
            (Some(_), Some(_)) => {
                return Err(to_py(Error::InvalidConfig(
                    "a workflow cannot end with both an output and an error".to_owned(),
                )));
            }
        };
        dispatch(py, blocking, async move {
            let finished = engine.conclude(&run, ended).await.map_err(to_py)?;
            Ok(finished_to_py(finished))
        })
    }

    fn keep_lease(&self, run: PyRef<'_, PyRun>) -> PyActiveRun {
        let _runtime = get_runtime().enter();
        PyActiveRun(Arc::new(self.0.keep_lease(&run.0)))
    }

    #[pyo3(signature = (workflow_id, polling_interval_sec=None))]
    fn await_result<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        polling_interval_sec: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        let interval = poll_interval(polling_interval_sec)?;
        future_into_py(py, async move {
            let result = engine
                .await_result_within(&workflow_id, interval)
                .await
                .map_err(to_py)?;
            Ok(PyResult_::from(result))
        })
    }

    #[pyo3(signature = (workflow_id, *, blocking=false))]
    fn get_status<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            let row = engine.get_status(&workflow_id).await.map_err(to_py)?;
            Ok(PyWorkflow::from(row))
        })
    }

    #[pyo3(signature = (workflow_id, *, limit=None, offset=None, blocking=false))]
    fn list_steps<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        limit: Option<i64>,
        offset: Option<i64>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            let rows = engine
                .list_steps_page(&workflow_id, limit, offset)
                .await
                .map_err(to_py)?;
            Ok(rows.into_iter().map(PyStep::from).collect::<Vec<_>>())
        })
    }

    #[pyo3(signature = (names, *, blocking=false))]
    fn recover_now<'py>(
        &self,
        py: Python<'py>,
        names: Vec<String>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            claims_to_py(engine.recover_now(&names).await.map_err(to_py)?)
        })
    }

    fn recovery_tick<'py>(
        &self,
        py: Python<'py>,
        names: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        future_into_py(py, async move {
            claims_to_py(engine.recovery_tick(&names).await)
        })
    }
}

#[pyfunction]
fn silence_logs() {
    log::set_max_level(log::LevelFilter::Off);
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("PORTABLE_JSON", PORTABLE_JSON)?;
    m.add("PY_PICKLE", PY_PICKLE)?;
    pyo3_log::Logger::new(py, pyo3_log::Caching::Loggers)?
        .install()
        .ok();
    m.add_function(wrap_pyfunction!(silence_logs, m)?)?;
    m.add_class::<PyEngine>()?;
    m.add_class::<PyRun>()?;
    m.add_class::<PyWorkflow>()?;
    m.add_class::<PyStep>()?;
    m.add_class::<PyResult_>()?;
    m.add_class::<PyBegun>()?;
    m.add_class::<PyStepReplay>()?;
    m.add_class::<PyClaim>()?;
    m.add_class::<schedules::PyScheduleFire>()?;
    m.add_class::<PyActiveRun>()?;
    m.add_class::<PyStepPolicy>()?;
    m.add_class::<PyStepAttempts>()?;
    m.add_class::<PyStepWatch>()?;
    m.add_class::<events::PyPoll>()?;
    m.add_class::<events::PySendResult>()?;
    m.add_class::<events::PyEventRow>()?;
    m.add(
        "DEFAULT_MAX_RECOVERY_ATTEMPTS",
        DEFAULT_MAX_RECOVERY_ATTEMPTS,
    )?;
    m.add("DBOSException", py.get_type::<DBOSException>())?;
    m.add("StoreError", py.get_type::<StoreError>())?;
    m.add(
        "DBOSQueueDeduplicatedError",
        py.get_type::<DBOSQueueDeduplicatedError>(),
    )?;
    m.add(
        "DBOSConflictingWorkflowError",
        py.get_type::<DBOSConflictingWorkflowError>(),
    )?;
    m.add(
        "DBOSUnexpectedStepError",
        py.get_type::<DBOSUnexpectedStepError>(),
    )?;
    m.add(
        "DBOSStepNondeterminismError",
        py.get_type::<DBOSStepNondeterminismError>(),
    )?;
    m.add(
        "DBOSNonExistentWorkflowError",
        py.get_type::<DBOSNonExistentWorkflowError>(),
    )?;
    m.add("DBOSLeaseLostError", py.get_type::<DBOSLeaseLostError>())?;
    m.add(
        "DBOSWorkflowCancelledError",
        py.get_type::<DBOSWorkflowCancelledError>(),
    )?;
    m.add(
        "DBOSWorkflowIDInUseError",
        py.get_type::<DBOSWorkflowIDInUseError>(),
    )?;
    Ok(())
}

fn poll_interval(seconds: Option<f64>) -> PyResult<Option<Duration>> {
    let Some(seconds) = seconds else {
        return Ok(None);
    };
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(to_py(Error::InvalidConfig(
            "polling_interval_sec must be positive".to_owned(),
        )));
    }
    Ok(Some(
        Duration::try_from_secs_f64(seconds).unwrap_or(Duration::MAX),
    ))
}

fn start_options(reuse_policy: Option<&str>) -> PyResult<WorkflowOptions> {
    let mut options = WorkflowOptions::default();
    if let Some(policy) = reuse_policy {
        options.reuse = ReusePolicy::parse(policy).map_err(to_py)?;
    }
    Ok(options)
}
