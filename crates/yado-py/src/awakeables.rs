use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use yado_core::Poll;

use super::events::PyPoll;
use super::{PyEngine, PyRun, dispatch, to_py};

#[pymethods]
impl PyEngine {
    #[pyo3(signature = (run, step_id, *, blocking=false))]
    fn create_awakeable<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        step_id: u32,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        dispatch(py, blocking, async move {
            engine.create_awakeable(&run, step_id).await.map_err(to_py)
        })
    }

    fn wait_awakeable<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        step_id: u32,
        timeout_step_id: u32,
        id: String,
        timeout_ms: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        future_into_py(py, async move {
            let value = engine
                .wait_awakeable(&run, step_id, timeout_step_id, &id, timeout_ms)
                .await
                .map_err(to_py)?;
            Ok(PyPoll::from(Poll::Ready(value)))
        })
    }

    #[pyo3(signature = (id, value, serialization, run=None, step_id=None, *, blocking=false))]
    fn complete_awakeable<'py>(
        &self,
        py: Python<'py>,
        id: String,
        value: String,
        serialization: String,
        run: Option<PyRef<'py, PyRun>>,
        step_id: Option<u32>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        let run = run.map(|run| run.0.clone());
        dispatch(py, blocking, async move {
            engine
                .complete_awakeable(&id, &value, &serialization, run.as_deref(), step_id)
                .await
                .map_err(to_py)
        })
    }
}
