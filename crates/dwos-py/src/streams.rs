use std::time::Duration;

use dwos_core::StreamRead;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;

use super::events::PySendResult;
use super::{PyEngine, PyRun, dispatch, poll_interval, to_py};

#[pyclass(name = "StreamItem", frozen, get_all)]
pub(crate) struct PyStreamItem {
    /// "value", "closed" or "timeout".
    kind: &'static str,
    value: Option<String>,
    serialization: Option<String>,
}

impl From<StreamRead> for PyStreamItem {
    fn from(read: StreamRead) -> Self {
        let (kind, value) = match read {
            StreamRead::Value(value) => ("value", Some(value)),
            StreamRead::Closed => ("closed", None),
            StreamRead::Waiting => ("timeout", None),
        };
        let (value, serialization) = value.map(|v| (v.text, v.serialization)).unzip();
        Self {
            kind,
            value,
            serialization,
        }
    }
}

#[pymethods]
impl PyEngine {
    #[pyo3(signature = (run, key, value, serialization, step_id=None, *, blocking=false))]
    #[allow(clippy::too_many_arguments)]
    fn write_stream<'py>(
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
                .write_stream(&run, step_id, &key, &value, &serialization)
                .await
                .map_err(to_py)?;
            Ok(PySendResult::from(outcome))
        })
    }

    #[pyo3(signature = (run, key, step_id=None, *, blocking=false))]
    fn close_stream<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        key: String,
        step_id: Option<u32>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        dispatch(py, blocking, async move {
            let outcome = engine
                .close_stream(&run, step_id, &key)
                .await
                .map_err(to_py)?;
            Ok(PySendResult::from(outcome))
        })
    }

    /// `timeout_seconds` `None` waits until a value arrives or the stream ends.
    #[pyo3(signature = (workflow_id, key, offset, timeout_seconds=None, polling_interval_sec=None))]
    fn read_stream<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        key: String,
        offset: i64,
        timeout_seconds: Option<f64>,
        polling_interval_sec: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        let interval = poll_interval(polling_interval_sec)?;
        let timeout = timeout_seconds
            .map(|seconds| Duration::try_from_secs_f64(seconds.max(0.0)).unwrap_or(Duration::MAX));
        future_into_py(py, async move {
            let read = engine
                .read_stream(&workflow_id, &key, offset, timeout, interval)
                .await
                .map_err(to_py)?;
            Ok(PyStreamItem::from(read))
        })
    }
}
