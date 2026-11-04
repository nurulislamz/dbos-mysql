use std::sync::Arc;
use std::time::Duration;

use dbos_mysql_core::{
    Claimed, Error, Lease, LeaseKeeper, NewWorkflow, Outcome, RecordedStep, Started, StepRecord,
    Store, Workflow,
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
        Error::UnexpectedStep { .. } => DBOSUnexpectedStepError::new_err(message),
        Error::StepNondeterminism { .. } => DBOSStepNondeterminismError::new_err(message),
        Error::WorkflowNotFound(_) => DBOSNonExistentWorkflowError::new_err(message),
        Error::LeaseLost(_) => DBOSLeaseLostError::new_err(message),
        _ => StoreError::new_err(message),
    }
}

fn seconds(value: f64) -> PyResult<Duration> {
    Duration::try_from_secs_f64(value)
        .map_err(|error| StoreError::new_err(format!("invalid duration: {error}")))
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

impl PyStep {
    fn from_record(step_id: u32, step_name: String, record: StepRecord) -> Self {
        Self {
            step_id,
            step_name,
            output: record.output,
            error: record.error,
            child_workflow_id: record.child_workflow_id,
        }
    }
}

impl From<RecordedStep> for PyStep {
    fn from(step: RecordedStep) -> Self {
        Self::from_record(step.step_id, step.step_name, step.record)
    }
}

#[pyclass(name = "ClaimRow", frozen, get_all)]
struct PyClaim {
    workflow_id: String,
    token: String,
    name: String,
    input: String,
    recovery_attempts: u32,
}

impl From<Claimed> for PyClaim {
    fn from(claim: Claimed) -> Self {
        Self {
            workflow_id: claim.lease.workflow_id,
            token: claim.lease.token,
            name: claim.name,
            input: claim.input,
            recovery_attempts: claim.recovery_attempts,
        }
    }
}

#[pyclass(name = "StartRow", frozen, get_all)]
struct PyStarted {
    token: Option<String>,
    existing: Option<Py<PyWorkflow>>,
}

#[pyclass(name = "LeaseKeeper", frozen)]
struct PyLeaseKeeper(Arc<LeaseKeeper>);

#[pymethods]
impl PyLeaseKeeper {
    fn lost<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let keeper = self.0.clone();
        future_into_py(py, async move { Ok(keeper.lost().await.as_str()) })
    }

    fn stop(&self) {
        self.0.stop();
    }
}

#[pyclass(name = "Store", frozen)]
struct PyStore(Store);

fn lease(workflow_id: String, token: String) -> Lease {
    Lease { workflow_id, token }
}

#[pymethods]
impl PyStore {
    #[getter]
    fn backend(&self) -> &'static str {
        self.0.backend()
    }

    fn migrate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move { store.migrate().await.map_err(to_py) })
    }

    fn start_workflow<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        name: String,
        input: String,
        max_recovery_attempts: u32,
        lease_seconds: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        let lease = seconds(lease_seconds)?;
        future_into_py(py, async move {
            let new = NewWorkflow {
                workflow_id: &workflow_id,
                name: &name,
                input: &input,
                max_recovery_attempts,
                lease,
            };
            let started = store.start_workflow(&new).await.map_err(to_py)?;
            Python::attach(|py| {
                Ok(match started {
                    Started::Owned(lease) => PyStarted {
                        token: Some(lease.token),
                        existing: None,
                    },
                    Started::Existing(row) => PyStarted {
                        token: None,
                        existing: Some(Py::new(py, PyWorkflow::from(row))?),
                    },
                })
            })
        })
    }

    fn get_workflow<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move {
            Ok(store
                .get_workflow(&workflow_id)
                .await
                .map_err(to_py)?
                .map(PyWorkflow::from))
        })
    }

    fn list_workflows<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move {
            let rows = store.list_workflows().await.map_err(to_py)?;
            Ok(rows.into_iter().map(PyWorkflow::from).collect::<Vec<_>>())
        })
    }

    fn list_steps<'py>(&self, py: Python<'py>, workflow_id: String) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move {
            let rows = store.list_steps(&workflow_id).await.map_err(to_py)?;
            Ok(rows.into_iter().map(PyStep::from).collect::<Vec<_>>())
        })
    }

    fn load_step<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        step_id: u32,
        step_name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move {
            let record = store
                .load_step(&workflow_id, step_id, &step_name)
                .await
                .map_err(to_py)?;
            Ok(record.map(|record| PyStep::from_record(step_id, step_name, record)))
        })
    }

    #[pyo3(signature = (workflow_id, token, step_id, step_name, output=None, error=None, child_workflow_id=None))]
    #[allow(clippy::too_many_arguments)]
    fn save_step<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        token: String,
        step_id: u32,
        step_name: String,
        output: Option<String>,
        error: Option<String>,
        child_workflow_id: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move {
            let record = StepRecord {
                output,
                error,
                child_workflow_id,
            };
            store
                .save_step(&lease(workflow_id, token), step_id, &step_name, &record)
                .await
                .map_err(to_py)
        })
    }

    fn finish_success<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        token: String,
        output: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move {
            store
                .finish_workflow(&lease(workflow_id, token), &Outcome::Success(output))
                .await
                .map_err(to_py)
        })
    }

    fn finish_error<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        token: String,
        error: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move {
            store
                .finish_workflow(&lease(workflow_id, token), &Outcome::Error(error))
                .await
                .map_err(to_py)
        })
    }

    fn release_lease<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        token: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move {
            store
                .release_lease(&lease(workflow_id, token))
                .await
                .map_err(to_py)
        })
    }

    fn reclaim<'py>(
        &self,
        py: Python<'py>,
        names: Vec<String>,
        limit: u32,
        lease_seconds: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        let duration = seconds(lease_seconds)?;
        future_into_py(py, async move {
            let claims = store
                .reclaim(&names, limit, duration)
                .await
                .map_err(to_py)?;
            Ok(claims.into_iter().map(PyClaim::from).collect::<Vec<_>>())
        })
    }

    fn keep_lease(
        &self,
        workflow_id: String,
        token: String,
        lease_seconds: f64,
        max_run_seconds: f64,
    ) -> PyResult<PyLeaseKeeper> {
        let _runtime = get_runtime().enter();
        let keeper = self.0.keep_lease(
            lease(workflow_id, token),
            seconds(lease_seconds)?,
            seconds(max_run_seconds)?,
        );
        Ok(PyLeaseKeeper(Arc::new(keeper)))
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

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    pyo3_log::Logger::new(py, pyo3_log::Caching::Loggers)?
        .install()
        .ok();
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_class::<PyStore>()?;
    m.add_class::<PyWorkflow>()?;
    m.add_class::<PyStep>()?;
    m.add_class::<PyClaim>()?;
    m.add_class::<PyStarted>()?;
    m.add_class::<PyLeaseKeeper>()?;
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
