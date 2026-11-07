use std::sync::Arc;

use pyo3::prelude::*;

use super::{PyEngine, PyRun, dispatch, to_py};

#[pymethods]
impl PyEngine {
    #[pyo3(signature = (run, input, *, blocking=false))]
    fn continue_as_new<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        input: String,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        dispatch(py, blocking, async move {
            Ok(PyRun(Arc::new(
                engine.continue_as_new(&run, &input).await.map_err(to_py)?,
            )))
        })
    }
}
