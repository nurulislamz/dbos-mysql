use dbos_mysql_core::{Error, Store};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;

create_exception!(_native, StoreError, PyException);

fn to_py(error: Error) -> PyErr {
    StoreError::new_err(error.to_string())
}

#[pyclass(name = "Store", frozen)]
struct PyStore(Store);

#[pymethods]
impl PyStore {
    #[getter]
    fn backend(&self) -> &'static str {
        self.0.backend()
    }

    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move { store.ping().await.map_err(to_py) })
    }

    fn session_settings<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(
            py,
            async move { store.session_settings().await.map_err(to_py) },
        )
    }

    fn spike_reset<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move { store.spike_reset().await.map_err(to_py) })
    }

    fn spike_hold_transaction<'py>(
        &self,
        py: Python<'py>,
        seconds: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move {
            store.spike_hold_transaction(seconds).await.map_err(to_py)
        })
    }

    fn spike_rows<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move { store.spike_rows().await.map_err(to_py) })
    }

    fn spike_open_transactions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.0.clone();
        future_into_py(py, async move {
            store.spike_open_transactions().await.map_err(to_py)
        })
    }

    fn connections_in_use(&self) -> u32 {
        self.0.connections_in_use()
    }
}

#[pyfunction]
fn connect(py: Python<'_>, url: String) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let store = Store::connect(&url).await.map_err(to_py)?;
        Ok(PyStore(store))
    })
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::Logger::new(m.py(), pyo3_log::Caching::Loggers)?
        .install()
        .ok();
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_class::<PyStore>()?;
    m.add("StoreError", m.py().get_type::<StoreError>())?;
    Ok(())
}
