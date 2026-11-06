use pyo3::prelude::*;

use super::{PyEngine, dispatch, to_py};

#[pymethods]
impl PyEngine {
    #[pyo3(signature = (workflow_ids, delete_children=false, *, blocking=false))]
    fn delete_workflows<'py>(
        &self,
        py: Python<'py>,
        workflow_ids: Vec<String>,
        delete_children: bool,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            engine
                .delete_workflows(&workflow_ids, delete_children)
                .await
                .map_err(to_py)
        })
    }

    #[pyo3(signature = (max_age_hours, keep_rows, batch, *, blocking=false))]
    fn apply_retention<'py>(
        &self,
        py: Python<'py>,
        max_age_hours: Option<f64>,
        keep_rows: Option<u32>,
        batch: u32,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            engine
                .apply_retention(max_age_hours, keep_rows, batch)
                .await
                .map_err(to_py)
        })
    }

    #[pyo3(signature = (cutoff_epoch_ms, batch, *, blocking=false))]
    fn delete_finished_before<'py>(
        &self,
        py: Python<'py>,
        cutoff_epoch_ms: i64,
        batch: u32,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            engine
                .delete_finished_before(cutoff_epoch_ms, batch)
                .await
                .map_err(to_py)
        })
    }
}
