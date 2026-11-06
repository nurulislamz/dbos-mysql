use pyo3::prelude::*;
use yado_core::{Begin, Error, ParentStep, recovery_limit};

use super::{PyEngine, PyRun, dispatch, start_options, to_py};

fn millis(field: &str, seconds: f64) -> PyResult<i64> {
    let ms = seconds * 1000.0;
    if !ms.is_finite() || ms < 0.0 || ms >= i64::MAX as f64 {
        return Err(to_py(Error::InvalidConfig(format!(
            "{field} must be finite and non-negative"
        ))));
    }
    Ok(ms as i64)
}

#[pymethods]
impl PyEngine {
    #[pyo3(signature = (queue, name, input, max_recovery_attempts, debounce_key, debounce_period_sec, debounce_timeout_sec=None, workflow_id=None, parent=None, in_step=false, timeout_ms=None, reuse_policy=None, serialization=None, *, app_version=None, application_name=None, attributes=None, blocking=false))]
    #[allow(clippy::too_many_arguments)]
    fn debounce<'py>(
        &self,
        py: Python<'py>,
        queue: String,
        name: String,
        input: String,
        max_recovery_attempts: Option<u32>,
        debounce_key: String,
        debounce_period_sec: f64,
        debounce_timeout_sec: Option<f64>,
        workflow_id: Option<String>,
        parent: Option<PyRef<'py, PyRun>>,
        in_step: bool,
        timeout_ms: Option<i64>,
        reuse_policy: Option<String>,
        serialization: Option<String>,
        app_version: Option<String>,
        application_name: Option<String>,
        attributes: Option<String>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        let parent = parent.map(|run| run.0.clone());
        // Taken at the call, not in the future, so concurrent calls keep their call order.
        let parent_step_id = parent
            .as_ref()
            .and_then(|run| run.step_id_unless_in_step(in_step));
        let period_ms = millis("debounce_period_sec", debounce_period_sec)?;
        // DBOS treats a zero timeout as no timeout.
        let debounce_timeout_ms = debounce_timeout_sec
            .map(|seconds| millis("debounce_timeout_sec", seconds))
            .transpose()?
            .filter(|ms| *ms > 0);
        let mut options = start_options(reuse_policy.as_deref())?;
        options.application_version = app_version;
        options.application_name = application_name;
        options.attributes = attributes;
        options.serialization = serialization;
        dispatch(py, blocking, async move {
            let parent_step = match (&parent, parent_step_id) {
                (Some(run), Some(step_id)) => Some(ParentStep { run, step_id }),
                _ => None,
            };
            engine
                .debounce(
                    &queue,
                    Begin {
                        name: &name,
                        input: &input,
                        max_recovery_attempts: recovery_limit(max_recovery_attempts),
                        workflow_id: workflow_id.as_deref(),
                        parent: parent_step,
                        timeout_ms,
                        ignore_propagated_deadline: true,
                    },
                    &options,
                    &debounce_key,
                    period_ms,
                    debounce_timeout_ms,
                )
                .await
                .map_err(to_py)
        })
    }
}
