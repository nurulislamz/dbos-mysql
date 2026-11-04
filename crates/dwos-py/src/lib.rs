use std::sync::Arc;
use std::time::Duration;

use dwos_core::{
    ActiveRun, Begin, Begun, Claimed, DEFAULT_MAX_RECOVERY_ATTEMPTS, Engine, EngineConfig, Error,
    FinalOutcome, Finished, Outcome, ParentStep, RecordedStep, Run, StepDecision, StepPolicy,
    Store, Workflow, WorkflowResult,
};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::{future_into_py, get_runtime};

create_exception!(_native, DBOSException, PyException);
create_exception!(_native, StoreError, DBOSException);
create_exception!(_native, DBOSConflictingWorkflowError, DBOSException);
create_exception!(_native, DBOSUnexpectedStepError, DBOSException);
create_exception!(_native, DBOSStepNondeterminismError, DBOSException);
create_exception!(_native, DBOSNonExistentWorkflowError, DBOSException);
create_exception!(_native, DBOSLeaseLostError, DBOSException);

fn to_py(error: Error) -> PyErr {
    let message = error.to_string();
    match error {
        Error::ConflictingWorkflow { .. } => DBOSConflictingWorkflowError::new_err(message),
        Error::UnexpectedStep { .. } | Error::StepKindMismatch { .. } => {
            DBOSUnexpectedStepError::new_err(message)
        }
        Error::StepNondeterminism { .. } => DBOSStepNondeterminismError::new_err(message),
        Error::WorkflowNotFound(_) => DBOSNonExistentWorkflowError::new_err(message),
        Error::LeaseLost(_) => DBOSLeaseLostError::new_err(message),
        Error::ShuttingDown => DBOSException::new_err(message),
        _ => StoreError::new_err(message),
    }
}

fn seconds(value: f64) -> PyResult<Duration> {
    Duration::try_from_secs_f64(value)
        .map_err(|error| StoreError::new_err(format!("invalid duration: {error}")))
}

#[pyclass(name = "Run", frozen)]
struct PyRun(Arc<Run>);

#[pymethods]
impl PyRun {
    #[getter]
    fn workflow_id(&self) -> &str {
        self.0.workflow_id()
    }
}

#[pyclass(name = "WorkflowRow", frozen, get_all)]
struct PyWorkflow {
    workflow_id: String,
    name: String,
    status: &'static str,
    input: String,
    output: Option<String>,
    error: Option<String>,
    recovery_attempts: u32,
    max_recovery_attempts: u32,
    parent_workflow_id: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
    completed_at_ms: Option<i64>,
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
            max_recovery_attempts: row.max_recovery_attempts,
            parent_workflow_id: row.parent_workflow_id,
            created_at_ms: row.created_at_ms,
            updated_at_ms: row.updated_at_ms,
            completed_at_ms: row.completed_at_ms,
        }
    }
}

#[pyclass(name = "StepRow", frozen, get_all)]
struct PyStep {
    step_id: u32,
    step_name: String,
    output: Option<String>,
    error: Option<String>,
    child_workflow_id: Option<String>,
}

impl From<RecordedStep> for PyStep {
    fn from(step: RecordedStep) -> Self {
        Self {
            step_id: step.step_id,
            step_name: step.step_name,
            output: step.record.output,
            error: step.record.error,
            child_workflow_id: step.record.child_workflow_id,
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
}

impl From<WorkflowResult> for PyResult_ {
    fn from(result: WorkflowResult) -> Self {
        let (kind, value, max_recovery_attempts) = match result.outcome {
            FinalOutcome::Success(value) => ("success", Some(value), None),
            FinalOutcome::Error(value) => ("error", Some(value), None),
            FinalOutcome::MaxRecoveryAttemptsExceeded(max) => {
                ("max_recovery_attempts", None, Some(max))
            }
        };
        Self {
            workflow_id: result.workflow_id,
            name: result.name,
            kind,
            value,
            max_recovery_attempts,
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
struct PyClaim {
    workflow_id: String,
    run: Py<PyRun>,
    name: String,
    input: String,
    recovery_attempts: u32,
}

fn claims_to_py(claims: Vec<Claimed>) -> PyResult<Vec<PyClaim>> {
    Python::attach(|py| {
        claims
            .into_iter()
            .map(|claim| {
                Ok(PyClaim {
                    workflow_id: claim.lease.workflow_id.clone(),
                    run: Py::new(py, PyRun(Arc::new(Run::new(claim.lease))))?,
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
struct PyActiveRun(Arc<ActiveRun>);

#[pymethods]
impl PyActiveRun {
    fn lost<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let active = self.0.clone();
        future_into_py(py, async move { Ok(active.lost().await.as_str()) })
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
    fn new(
        retries_allowed: bool,
        interval_seconds: f64,
        max_attempts: u32,
        backoff_rate: f64,
    ) -> PyResult<Self> {
        StepPolicy::new(
            retries_allowed,
            interval_seconds,
            max_attempts,
            backoff_rate,
        )
        .map(Self)
        .map_err(to_py)
    }

    #[getter]
    fn retries_allowed(&self) -> bool {
        self.0.retries_allowed()
    }

    #[getter]
    fn max_attempts(&self) -> u32 {
        self.0.max_attempts()
    }

    fn status(&self, attempt: u32) -> (Option<u32>, Option<u32>) {
        self.0.status(attempt)
    }

    fn retry_delay(&self, failed_attempt: u32) -> Option<f64> {
        self.0
            .retry_delay(failed_attempt)
            .map(|delay| delay.as_secs_f64())
    }
}

#[pyclass(name = "Engine", frozen)]
struct PyEngine(Engine);

#[pymethods]
impl PyEngine {
    #[staticmethod]
    #[pyo3(signature = (url, *, lease_seconds=None, max_run_seconds=None, recovery_interval_seconds=None, max_concurrent_recoveries=None, run_migrations=None))]
    fn connect(
        py: Python<'_>,
        url: String,
        lease_seconds: Option<f64>,
        max_run_seconds: Option<f64>,
        recovery_interval_seconds: Option<f64>,
        max_concurrent_recoveries: Option<u32>,
        run_migrations: Option<bool>,
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
        future_into_py(py, async move {
            let engine = Engine::connect(&url, config).await.map_err(to_py)?;
            Ok(PyEngine(engine))
        })
    }

    fn close(&self) {
        self.0.close();
    }

    #[pyo3(signature = (name, input, max_recovery_attempts, workflow_id=None, parent=None, in_step=false))]
    #[allow(clippy::too_many_arguments)]
    fn begin<'py>(
        &self,
        py: Python<'py>,
        name: String,
        input: String,
        max_recovery_attempts: u32,
        workflow_id: Option<String>,
        parent: Option<PyRef<'py, PyRun>>,
        in_step: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        let parent = parent.map(|run| run.0.clone());
        // Taken at the call, not in the future, so concurrent calls keep their call order.
        let parent_step_id = parent.as_ref().and_then(|run| run.child_step_id(in_step));
        future_into_py(py, async move {
            let parent_step = match (&parent, parent_step_id) {
                (Some(run), Some(step_id)) => Some(ParentStep { run, step_id }),
                _ => None,
            };
            let begun = engine
                .begin(Begin {
                    name: &name,
                    input: &input,
                    max_recovery_attempts,
                    workflow_id: workflow_id.as_deref(),
                    parent: parent_step,
                })
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

    fn begin_step<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        // Taken at the call, not in the future, so concurrent calls keep their call order.
        let step_id = run.next_step_id();
        future_into_py(py, async move {
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
            Ok((step_id, replay))
        })
    }

    fn save_step_output<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        step_id: u32,
        name: String,
        output: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        future_into_py(py, async move {
            engine
                .save_step_output(&run, step_id, &name, output)
                .await
                .map_err(to_py)
        })
    }

    fn save_step_error<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        step_id: u32,
        name: String,
        error: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        future_into_py(py, async move {
            engine
                .save_step_error(&run, step_id, &name, error)
                .await
                .map_err(to_py)
        })
    }

    fn finish_success<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        output: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        future_into_py(py, async move {
            let finished = engine
                .finish(&run, Outcome::Success(output))
                .await
                .map_err(to_py)?;
            Ok(finished_to_py(finished))
        })
    }

    fn finish_error<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        error: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        future_into_py(py, async move {
            let finished = engine
                .finish(&run, Outcome::Error(error))
                .await
                .map_err(to_py)?;
            Ok(finished_to_py(finished))
        })
    }

    fn release<'py>(&self, py: Python<'py>, run: PyRef<'py, PyRun>) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        future_into_py(py, async move { engine.release(&run).await.map_err(to_py) })
    }

    fn keep_lease(&self, run: PyRef<'_, PyRun>) -> PyActiveRun {
        let _runtime = get_runtime().enter();
        PyActiveRun(Arc::new(self.0.keep_lease(&run.0)))
    }

    fn await_result<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        future_into_py(py, async move {
            let result = engine.await_result(&workflow_id).await.map_err(to_py)?;
            Ok(PyResult_::from(result))
        })
    }

    fn get_status<'py>(&self, py: Python<'py>, workflow_id: String) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        future_into_py(py, async move {
            let row = engine.get_status(&workflow_id).await.map_err(to_py)?;
            Ok(PyWorkflow::from(row))
        })
    }

    fn list_workflows<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        future_into_py(py, async move {
            let rows = engine.list_workflows().await.map_err(to_py)?;
            Ok(rows.into_iter().map(PyWorkflow::from).collect::<Vec<_>>())
        })
    }

    fn list_steps<'py>(&self, py: Python<'py>, workflow_id: String) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        future_into_py(py, async move {
            let rows = engine.list_steps(&workflow_id).await.map_err(to_py)?;
            Ok(rows.into_iter().map(PyStep::from).collect::<Vec<_>>())
        })
    }

    fn recover_now<'py>(&self, py: Python<'py>, names: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        future_into_py(py, async move {
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

#[pyclass(name = "Store", frozen)]
struct PyStore(Store);

#[pymethods]
impl PyStore {
    #[getter]
    fn backend(&self) -> &'static str {
        self.0.backend()
    }

    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move { store.ping().await.map_err(to_py) })
    }

    fn session_settings<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(
            py,
            async move { store.session_settings().await.map_err(to_py) },
        )
    }

    fn spike_reset<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move { store.spike_reset().await.map_err(to_py) })
    }

    fn spike_hold_transaction<'py>(
        &self,
        py: Python<'py>,
        seconds: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move {
            store.spike_hold_transaction(seconds).await.map_err(to_py)
        })
    }

    fn spike_rows<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move { store.spike_rows().await.map_err(to_py) })
    }

    fn spike_open_transactions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move {
            store.spike_open_transactions().await.map_err(to_py)
        })
    }

    fn connections_in_use(&self) -> u32 {
        self.0.connections_in_use()
    }
}

#[pyfunction]
fn connect(py: Python<'_>, url: String) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let store = Store::connect(&url).await.map_err(to_py)?;
        Ok(PyStore(store))
    })
}

#[pyfunction]
fn silence_logs() {
    log::set_max_level(log::LevelFilter::Off);
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    pyo3_log::Logger::new(py, pyo3_log::Caching::Loggers)?
        .install()
        .ok();
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(silence_logs, m)?)?;
    m.add_class::<PyEngine>()?;
    m.add_class::<PyStore>()?;
    m.add_class::<PyRun>()?;
    m.add_class::<PyWorkflow>()?;
    m.add_class::<PyStep>()?;
    m.add_class::<PyResult_>()?;
    m.add_class::<PyBegun>()?;
    m.add_class::<PyStepReplay>()?;
    m.add_class::<PyClaim>()?;
    m.add_class::<PyActiveRun>()?;
    m.add_class::<PyStepPolicy>()?;
    m.add(
        "DEFAULT_MAX_RECOVERY_ATTEMPTS",
        DEFAULT_MAX_RECOVERY_ATTEMPTS,
    )?;
    m.add("DBOSException", py.get_type::<DBOSException>())?;
    m.add("StoreError", py.get_type::<StoreError>())?;
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
    Ok(())
}
