use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;

use super::{PyEngine, poll_interval, to_py};

#[pymethods]
impl PyEngine {
    fn await_first<'py>(
        &self,
        py: Python<'py>,
        workflow_ids: Vec<String>,
        polling_interval_sec: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        let interval = poll_interval(Some(polling_interval_sec))?;
        future_into_py(py, async move {
            engine
                .await_first(&workflow_ids, interval)
                .await
                .map_err(to_py)
        })
    }
}
