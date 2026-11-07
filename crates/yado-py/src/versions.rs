use pyo3::prelude::*;
use yado_core::VersionRow;

use super::{PyEngine, dispatch, to_py};

#[pyclass(name = "VersionRow", frozen, get_all)]
pub(crate) struct PyVersionRow {
    version_id: String,
    version_name: String,
    version_timestamp: i64,
    created_at: i64,
}

impl From<VersionRow> for PyVersionRow {
    fn from(row: VersionRow) -> Self {
        Self {
            version_id: row.version_id,
            version_name: row.version_name,
            version_timestamp: row.version_timestamp,
            created_at: row.created_at,
        }
    }
}

#[pymethods]
impl PyEngine {
    #[getter]
    fn application_version(&self) -> Option<String> {
        self.0.application_version().map(str::to_owned)
    }

    #[pyo3(signature = (*, blocking=false))]
    fn list_versions<'py>(&self, py: Python<'py>, blocking: bool) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            let rows = engine.list_versions().await.map_err(to_py)?;
            Ok(rows.into_iter().map(PyVersionRow::from).collect::<Vec<_>>())
        })
    }

    #[pyo3(signature = (name, *, blocking=false))]
    fn set_latest_version<'py>(
        &self,
        py: Python<'py>,
        name: String,
        blocking: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.0.clone();
        dispatch(py, blocking, async move {
            engine.set_latest_version(&name).await.map_err(to_py)
        })
    }
}
