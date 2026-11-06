use pyo3::prelude::*;

use super::{PyActiveRun, PyEngine, PyRun, dispatch, seconds as duration, to_py};

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

    /// Returns why the lease was lost, or `None` when the sleep ended on time.
    #[pyo3(signature = (run, active, seconds, *, blocking=false))]
    fn wait_sleep<'py>(
        &self,
        py: Python<'py>,
        run: PyRef<'py, PyRun>,
        active: PyRef<'py, PyActiveRun>,
        seconds: f64,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (engine, run, active) = (self.0.clone(), run.0.clone(), active.0.clone());
        let remaining = duration(seconds)?;
        dispatch(py, blocking, async move {
            let loss = engine.wait_sleep(&run, &active, remaining).await;
            Ok(loss.map(|loss| loss.as_str()))
        })
    }
}
