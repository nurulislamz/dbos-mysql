use pyo3::prelude::*;

use super::{PyEngine, PyRun, dispatch, to_py};

#[pymethods]
impl PyEngine {
    #[pyo3(signature = (run, seconds, in_step, *, blocking=false))]
    fn sleep<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        seconds: f64,
        in_step: bool,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run) = (self.0.clone(), run.0.clone());
        let step_id = run.step_id_unless_in_step(in_step);
        dispatch(py, blocking, async move {
            let Some(step_id) = step_id else {
                return Ok(seconds);
            };
            let remaining = engine.sleep(&run, step_id, seconds).await.map_err(to_py)?;
            Ok(remaining.as_secs_f64())
        })
    }
}
