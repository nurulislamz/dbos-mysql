use pyo3::prelude::*;

use super::{PyEngine, PyRun, dispatch, to_py};

#[pymethods]
impl PyEngine {
    #[pyo3(signature = (run, name, *, blocking=false))]
    fn patch<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        name: String,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        dispatch(py, blocking, async move {
            engine.patch(&run, &name).await.map_err(to_py)
        })
    }

    #[pyo3(signature = (run, name, *, blocking=false))]
    fn deprecate_patch<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        name: String,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        dispatch(py, blocking, async move {
            engine.deprecate_patch(&run, &name).await.map_err(to_py)
        })
    }
}
