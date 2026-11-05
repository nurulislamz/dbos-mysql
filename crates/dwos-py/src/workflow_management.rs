use dwos_core::{Status, WorkflowFilter};
use pyo3::prelude::*;

use super::{PyEngine, PyWorkflow, claims_to_py, dispatch, to_py};

fn statuses(values: Option<Vec<String>>) -> PyResult<Option<Vec<Status>>> {
    values
        .map(|values| {
            values
                .iter()
                .map(|value| Status::parse(value).map_err(to_py))
                .collect()
        })
        .transpose()
}

#[pymethods]
impl PyEngine {
    #[pyo3(signature = (workflow_id, *, blocking=false))]
    fn cancel_workflow<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            engine.cancel_workflow(&workflow_id).await.map_err(to_py)
        })
    }

    #[pyo3(signature = (workflow_id, names, *, blocking=false))]
    fn resume_workflow<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        names: Vec<String>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            Ok(
                match engine
                    .resume_workflow(&workflow_id, &names)
                    .await
                    .map_err(to_py)?
                {
                    Some(claim) => Some(claims_to_py(vec![claim])?.remove(0)),
                    None => None,
                },
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        *,
        workflow_ids=None,
        status=None,
        name=None,
        parent_workflow_id=None,
        workflow_id_prefix=None,
        start_time_ms=None,
        end_time_ms=None,
        completed_after_ms=None,
        completed_before_ms=None,
        has_parent=None,
        limit=None,
        offset=None,
        sort_desc=false,
        blocking=false,
    ))]
    fn list_workflows<'py>(
        &self,
        py: Python<'py>,
        workflow_ids: Option<Vec<String>>,
        status: Option<Vec<String>>,
        name: Option<Vec<String>>,
        parent_workflow_id: Option<Vec<String>>,
        workflow_id_prefix: Option<Vec<String>>,
        start_time_ms: Option<i64>,
        end_time_ms: Option<i64>,
        completed_after_ms: Option<i64>,
        completed_before_ms: Option<i64>,
        has_parent: Option<bool>,
        limit: Option<i64>,
        offset: Option<i64>,
        sort_desc: bool,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let filter = WorkflowFilter {
            workflow_ids,
            statuses: statuses(status)?,
            names: name,
            parent_workflow_ids: parent_workflow_id,
            workflow_id_prefixes: workflow_id_prefix,
            start_time_ms,
            end_time_ms,
            completed_after_ms,
            completed_before_ms,
            has_parent,
            limit,
            offset,
            sort_desc,
        };
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            let rows = engine.list_workflows(&filter).await.map_err(to_py)?;
            Ok(rows.into_iter().map(PyWorkflow::from).collect::<Vec<_>>())
        })
    }
}
