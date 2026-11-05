use dwos_core::{NewSchedule, recovery_limit};

use super::{PyEngine, dispatch, to_py};
use pyo3::prelude::*;

#[pyclass(name = "ScheduleFire", frozen, get_all)]
pub(crate) struct PyScheduleFire {
    pub schedule_name: String,
    pub queue_name: String,
    pub workflow_id: String,
    pub scheduled_at_ms: i64,
    pub started: bool,
}

#[pymethods]
impl PyEngine {
    #[pyo3(signature = (name, workflow_name, cron, queue, inputs, serialization, max_recovery_attempts=None, *, blocking=false))]
    #[allow(clippy::too_many_arguments)]
    fn ensure_schedule<'py>(
        &self,
        py: Python<'py>,
        name: String,
        workflow_name: String,
        cron: String,
        queue: String,
        inputs: String,
        serialization: String,
        max_recovery_attempts: Option<u32>,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        let limit = recovery_limit(max_recovery_attempts);
        dispatch(py, blocking, async move {
            engine
                .ensure_schedule(NewSchedule {
                    name: &name,
                    workflow_name: &workflow_name,
                    cron: &cron,
                    queue: &queue,
                    inputs: &inputs,
                    serialization: &serialization,
                    max_recovery_attempts: limit,
                })
                .await
                .map_err(to_py)
        })
    }

    #[pyo3(signature = (*, blocking=false))]
    fn fire_schedules<'py>(&self, py: Python<'py>, blocking: bool) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            engine.fire_schedules().await.map_err(to_py).map(|fired| {
                fired
                    .into_iter()
                    .map(|fire| PyScheduleFire {
                        schedule_name: fire.schedule_name,
                        queue_name: fire.queue_name,
                        workflow_id: fire.workflow_id,
                        scheduled_at_ms: fire.scheduled_at_ms,
                        started: fire.started,
                    })
                    .collect::<Vec<_>>()
            })
        })
    }
}
