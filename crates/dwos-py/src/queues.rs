use std::num::NonZeroU32;
use std::time::Duration;

use dwos_core::{Begin, Error, ParentStep, recovery_limit};

use super::start_options;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;

use super::{PyEngine, PyRun, claims_to_py, dispatch, to_py};

#[pymethods]
impl PyEngine {
    #[pyo3(signature = (name, *, blocking=false))]
    fn get_queue<'py>(
        &self,
        py: Python<'py>,
        name: String,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            engine
                .get_queue(&name)
                .await
                .map(|queue| queue.map(NonZeroU32::get))
                .map_err(to_py)
        })
    }

    /// `None` keeps an existing queue's limit. A new queue then has no limit.
    #[pyo3(signature = (name, concurrency=None, *, blocking=false))]
    fn ensure_queue<'py>(
        &self,
        py: Python<'py>,
        name: String,
        concurrency: Option<u32>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let limit = concurrency
            .map(|value| {
                NonZeroU32::new(value).ok_or_else(|| {
                    to_py(Error::InvalidConfig(
                        "queue concurrency must be at least 1".to_owned(),
                    ))
                })
            })
            .transpose()?;
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            match limit {
                Some(limit) => engine.ensure_queue(&name, limit).await,
                None => engine.ensure_queue_exists(&name).await,
            }
            .map_err(to_py)
        })
    }

    #[pyo3(signature = (queue, name, input, max_recovery_attempts, workflow_id=None, parent=None, in_step=false, timeout_ms=None, ignore_propagated_deadline=false, reuse_policy=None, *, deduplication_id=None, duplication_policy=None, priority=None, blocking=false))]
    #[allow(clippy::too_many_arguments)]
    fn enqueue<'py>(
        &self,
        py: Python<'py>,
        queue: String,
        name: String,
        input: String,
        max_recovery_attempts: Option<u32>,
        workflow_id: Option<String>,
        parent: Option<PyRef<'py, PyRun>>,
        in_step: bool,
        timeout_ms: Option<i64>,
        ignore_propagated_deadline: bool,
        reuse_policy: Option<String>,
        deduplication_id: Option<String>,
        duplication_policy: Option<String>,
        priority: Option<i64>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        let parent = parent.map(|run| run.0.clone());
        // Taken at the call, not in the future, so concurrent calls keep their call order.
        let parent_step_id = parent
            .as_ref()
            .and_then(|run| run.step_id_unless_in_step(in_step));
        let mut options = start_options(reuse_policy.as_deref())?;
        options.deduplication_id = deduplication_id;
        options.priority = priority;
        options.duplication_policy = duplication_policy
            .as_deref()
            .map(dwos_core::ReusePolicy::parse)
            .transpose()
            .map_err(to_py)?;
        dispatch(py, blocking, async move {
            let parent_step = match (&parent, parent_step_id) {
                (Some(run), Some(step_id)) => Some(ParentStep { run, step_id }),
                _ => None,
            };
            engine
                .enqueue_with(
                    &queue,
                    Begin {
                        name: &name,
                        input: &input,
                        max_recovery_attempts: recovery_limit(max_recovery_attempts),
                        workflow_id: workflow_id.as_deref(),
                        parent: parent_step,
                        timeout_ms,
                        ignore_propagated_deadline,
                    },
                    &options,
                )
                .await
                .map_err(to_py)
        })
    }

    fn claim_queue<'py>(
        &self,
        py: Python<'py>,
        queue: String,
        names: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        future_into_py(py, async move {
            claims_to_py(engine.claim_queue(&queue, &names).await.map_err(to_py)?)
        })
    }

    fn wake_queue(&self, queue: &str) {
        self.0.wake_queue(queue);
    }

    fn wait_queue_claims<'py>(
        &self,
        py: Python<'py>,
        queue: String,
        names: Vec<String>,
        interval_seconds: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        // A positive interval too large for a Duration only ever means "wait as long as possible".
        let interval = Duration::try_from_secs_f64(interval_seconds).unwrap_or(Duration::MAX);
        let engine = self.0.clone();
        future_into_py(py, async move {
            claims_to_py(engine.wait_queue_claims(&queue, &names, interval).await)
        })
    }
}
