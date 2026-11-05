use dwos_core::{EventRow, Poll, SendOutcome};
use pyo3::prelude::*;

use super::{PyEngine, PyRun, dispatch, to_py};

#[pyclass(name = "Poll", frozen, get_all)]
pub(crate) struct PyPoll {
    kind: &'static str,
    value: Option<String>,
    serialization: Option<String>,
    remaining_ms: Option<i64>,
    deadline_ms: Option<i64>,
}

impl From<Poll> for PyPoll {
    fn from(poll: Poll) -> Self {
        match poll {
            Poll::Ready(value) => Self {
                kind: "ready",
                value: Some(value.text),
                serialization: Some(value.serialization),
                remaining_ms: None,
                deadline_ms: None,
            },
            Poll::Waiting {
                remaining_ms,
                deadline_ms,
            } => Self {
                kind: "waiting",
                value: None,
                serialization: None,
                remaining_ms: Some(remaining_ms),
                deadline_ms: Some(deadline_ms),
            },
        }
    }
}

#[pyclass(name = "SendResult", frozen, get_all)]
pub(crate) struct PySendResult {
    kind: &'static str,
    error: Option<String>,
}

impl From<SendOutcome> for PySendResult {
    fn from(outcome: SendOutcome) -> Self {
        match outcome {
            SendOutcome::Sent => Self {
                kind: "sent",
                error: None,
            },
            SendOutcome::Replayed => Self {
                kind: "replayed",
                error: None,
            },
            SendOutcome::ReplayedError(error) => Self {
                kind: "replayed_error",
                error: Some(error),
            },
        }
    }
}

#[pyclass(name = "EventRow", frozen, get_all)]
pub(crate) struct PyEventRow {
    key: String,
    value: String,
    serialization: String,
}

impl From<EventRow> for PyEventRow {
    fn from(row: EventRow) -> Self {
        Self {
            key: row.key,
            value: row.value.text,
            serialization: row.value.serialization,
        }
    }
}

#[pymethods]
impl PyEngine {
    #[pyo3(signature = (destination, message, serialization, topic=None, idempotency_key=None, run=None, step_id=None, *, blocking=false))]
    #[allow(clippy::too_many_arguments)]
    fn send<'py>(
        &self,
        py: Python<'py>,
        destination: String,
        message: String,
        serialization: String,
        topic: Option<String>,
        idempotency_key: Option<String>,
        run: Option<PyRef<'py, PyRun>>,
        step_id: Option<u32>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        let run = run.map(|run| run.0.clone());
        dispatch(py, blocking, async move {
            let outcome = engine
                .send(
                    &destination,
                    topic.as_deref(),
                    &message,
                    &serialization,
                    idempotency_key.as_deref(),
                    run.as_deref(),
                    step_id,
                )
                .await
                .map_err(to_py)?;
            Ok(PySendResult::from(outcome))
        })
    }

    #[pyo3(signature = (run, step_id, timeout_step_id, timeout_ms, topic=None, *, blocking=false))]
    #[allow(clippy::too_many_arguments)]
    fn poll_recv<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        step_id: u32,
        timeout_step_id: u32,
        timeout_ms: i64,
        topic: Option<String>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        dispatch(py, blocking, async move {
            let poll = engine
                .poll_recv(&run, step_id, timeout_step_id, topic.as_deref(), timeout_ms)
                .await
                .map_err(to_py)?;
            Ok(PyPoll::from(poll))
        })
    }

    #[pyo3(signature = (target, key, timeout_ms, deadline_ms=None, run=None, step_id=None, timeout_step_id=None, *, blocking=false))]
    #[allow(clippy::too_many_arguments)]
    fn poll_event<'py>(
        &self,
        py: Python<'py>,
        target: String,
        key: String,
        timeout_ms: i64,
        deadline_ms: Option<i64>,
        run: Option<PyRef<'py, PyRun>>,
        step_id: Option<u32>,
        timeout_step_id: Option<u32>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        let run = run.map(|run| run.0.clone());
        dispatch(py, blocking, async move {
            let caller = match (run.as_deref(), step_id, timeout_step_id) {
                (Some(run), Some(step_id), Some(timeout_step_id)) => {
                    Some((run, step_id, timeout_step_id))
                }
                _ => None,
            };
            let poll = engine
                .poll_event(&target, &key, timeout_ms, deadline_ms, caller)
                .await
                .map_err(to_py)?;
            Ok(PyPoll::from(poll))
        })
    }

    #[pyo3(signature = (run, key, value, serialization, step_id=None, *, blocking=false))]
    #[allow(clippy::too_many_arguments)]
    fn set_event<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        key: String,
        value: String,
        serialization: String,
        step_id: Option<u32>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        dispatch(py, blocking, async move {
            let outcome = engine
                .set_event(&run, step_id, &key, &value, &serialization)
                .await
                .map_err(to_py)?;
            Ok(PySendResult::from(outcome))
        })
    }

    #[pyo3(signature = (workflow_id, *, blocking=false))]
    fn list_events<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            let rows = engine.list_events(&workflow_id).await.map_err(to_py)?;
            Ok(rows.into_iter().map(PyEventRow::from).collect::<Vec<_>>())
        })
    }
}
