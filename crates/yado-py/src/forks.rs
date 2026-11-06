use std::collections::HashMap;

use pyo3::prelude::*;
use yado_core::ForkOptions;

use super::events::PySendResult;
use super::{PyEngine, PyRun, claims_to_py, dispatch, to_py};

#[pymethods]
impl PyEngine {
    /// Returns the fork's ID and, when it runs here, its claim.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (workflow_id, start_step, names, new_id=None, application_version=None, queue_name=None, queue_partition_key=None, replacement_children=None, timeout_ms=None, *, blocking=false))]
    fn fork_workflow<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        start_step: u32,
        names: Vec<String>,
        new_id: Option<String>,
        application_version: Option<String>,
        queue_name: Option<String>,
        queue_partition_key: Option<String>,
        replacement_children: Option<HashMap<String, String>>,
        timeout_ms: Option<i64>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        let options = ForkOptions {
            application_version,
            queue_name,
            queue_partition_key,
            timeout_ms,
            replacement_children: replacement_children.unwrap_or_default(),
        };
        dispatch(py, blocking, async move {
            let (fork_id, claim) = engine
                .fork_workflow(
                    &workflow_id,
                    start_step,
                    new_id.as_deref(),
                    &names,
                    &options,
                )
                .await
                .map_err(to_py)?;
            let claim = claims_to_py(claim.into_iter().collect())?.pop();
            Ok((fork_id, claim))
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (workflow_id, names, start_step=None, application_version=None, queue_name=None, queue_partition_key=None, *, blocking=false))]
    fn rewind_workflow<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        names: Vec<String>,
        start_step: Option<u32>,
        application_version: Option<String>,
        queue_name: Option<String>,
        queue_partition_key: Option<String>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            let claim = engine
                .rewind_workflow(
                    &workflow_id,
                    start_step,
                    &names,
                    application_version.as_deref(),
                    queue_name.as_deref(),
                    queue_partition_key.as_deref(),
                )
                .await
                .map_err(to_py)?;
            Ok(claims_to_py(claim.into_iter().collect())?.pop())
        })
    }

    #[pyo3(signature = (messages, serialization, run=None, step_id=None, *, blocking=false))]
    fn send_bulk_to_forks<'py>(
        &self,
        py: Python<'py>,
        messages: Vec<(String, Option<String>, String, Option<String>)>,
        serialization: String,
        run: Option<PyRef<'py, PyRun>>,
        step_id: Option<u32>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        let run = run.map(|run| run.0.clone());
        dispatch(py, blocking, async move {
            let outcome = engine
                .send_bulk_to_forks(&messages, &serialization, run.as_deref(), step_id)
                .await
                .map_err(to_py)?;
            Ok(PySendResult::from(outcome))
        })
    }
}
