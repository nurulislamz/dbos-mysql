use pyo3::prelude::*;

use super::{PyEngine, PyRun, dispatch, to_py};

#[pymethods]
impl PyEngine {
    /// Raises `DBOSLeaseLostError` unless this run still owns its workflow.
    #[pyo3(signature = (run, *, blocking=false))]
    fn check_owner<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        dispatch(py, blocking, async move {
            engine.check_owner(&run).await.map_err(to_py)
        })
    }
}
