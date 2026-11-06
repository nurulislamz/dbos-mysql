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
    #[pyo3(signature = (workflow_id, cancel_children=false, *, blocking=false))]
    fn cancel_workflow<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        cancel_children: bool,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            engine
                .cancel_workflows(&[workflow_id], cancel_children)
                .await
                .map_err(to_py)
        })
    }

    #[pyo3(signature = (workflow_ids, cancel_children=false, *, blocking=false))]
    fn cancel_workflows<'py>(
        &self,
        py: Python<'py>,
        workflow_ids: Vec<String>,
        cancel_children: bool,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            engine
                .cancel_workflows(&workflow_ids, cancel_children)
                .await
                .map_err(to_py)
        })
    }

    #[pyo3(signature = (workflow_id, names, queue_name=None, *, blocking=false))]
    fn resume_workflow<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        names: Vec<String>,
        queue_name: Option<String>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            Ok(
                match engine
                    .resume_workflows(&[workflow_id], &names, queue_name.as_deref())
                    .await
                    .map_err(to_py)?
                    .into_iter()
                    .next()
                {
                    Some(claim) => Some(claims_to_py(vec![claim])?.remove(0)),
                    None => None,
                },
            )
        })
    }

    #[pyo3(signature = (workflow_ids, names, queue_name=None, *, blocking=false))]
    fn resume_workflows<'py>(
        &self,
        py: Python<'py>,
        workflow_ids: Vec<String>,
        names: Vec<String>,
        queue_name: Option<String>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            claims_to_py(
                engine
                    .resume_workflows(&workflow_ids, &names, queue_name.as_deref())
                    .await
                    .map_err(to_py)?,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        *,
        workflow_ids=None,
        queue_name=None,
        app_version=None,
        forked_from=None,
        is_fork=None,
        was_forked_from=None,
        user=None,
        queues_only=false,
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
        queue_name: Option<Vec<String>>,
        app_version: Option<Vec<String>>,
        forked_from: Option<Vec<String>>,
        is_fork: Option<bool>,
        was_forked_from: Option<bool>,
        user: Option<Vec<String>>,
        queues_only: bool,
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
            queue_names: queue_name,
            app_versions: app_version,
            forked_from,
            is_fork,
            was_forked_from,
            users: user,
            queues_only,
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
