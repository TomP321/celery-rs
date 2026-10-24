//! Binding del worker master: per ora solo il [`Waker`] condiviso dal loop.

use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// `_celery_rs.Waker`: il loop Python dorme in `wait` finché un componente
/// Rust (consumer, pool) non chiama `notify`.
#[pyclass(name = "Waker", module = "_celery_rs", frozen)]
pub struct PyWaker {
    pub inner: celery_worker::Waker,
}

#[pymethods]
impl PyWaker {
    #[new]
    fn new() -> Self {
        Self {
            inner: celery_worker::Waker::new(),
        }
    }

    fn notify(&self) {
        self.inner.notify();
    }

    /// `wait(timeout=None) -> bool`; il GIL è rilasciato durante l'attesa.
    #[pyo3(signature = (timeout=None))]
    fn wait(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<bool> {
        let timeout = match timeout {
            None => None,
            Some(t) if t.is_finite() && t >= 0.0 => Some(Duration::from_secs_f64(t)),
            Some(_) => return Err(PyValueError::new_err("timeout deve essere >= 0")),
        };
        let waker = self.inner.clone();
        Ok(py.detach(move || waker.wait(timeout)))
    }
}
