use super::events::PySendResult;
use super::{PyEngine, PyRun, dispatch, to_py};
use pyo3::prelude::*;

#[pymethods]
impl PyEngine {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (workflow_id, attributes, merge=false, run=None, step_id=None, *, blocking=false))]
    fn update_workflow_attributes<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        attributes: Option<String>,
        merge: bool,
        run: Option<PyRef<'py, PyRun>>,
        step_id: Option<u32>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        let run = run.map(|run| run.0.clone());
        dispatch(py, blocking, async move {
            let result = engine
                .update_workflow_attributes(
                    &workflow_id,
                    attributes.as_deref(),
                    merge,
                    run.as_deref(),
                    step_id,
                )
                .await
                .map_err(to_py)?;
            Ok(PySendResult::from(result))
        })
    }
}
