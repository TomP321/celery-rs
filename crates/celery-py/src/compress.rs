//! Native bindings for kombu compression.

use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use celery_proto::compression;

#[pyfunction]
pub fn compress(
    py: Python<'_>,
    body: &Bound<'_, PyBytes>,
    name: &str,
) -> PyResult<(Py<PyBytes>, String)> {
    let data = body.as_bytes();
    let out = py.detach(move || compression::compress(name, data));
    let (out, content_type) = out.map_err(map_error)?;
    Ok((PyBytes::new(py, &out).unbind(), content_type.to_string()))
}

#[pyfunction]
pub fn decompress(
    py: Python<'_>,
    body: &Bound<'_, PyBytes>,
    content_type: &str,
) -> PyResult<Py<PyBytes>> {
    let data = body.as_bytes();
    let out = py.detach(move || compression::decompress(content_type, data));
    let out = out.map_err(map_error)?;
    Ok(PyBytes::new(py, &out).unbind())
}

fn map_error(err: compression::CompressionError) -> PyErr {
    match err {
        compression::CompressionError::UnknownMethod(name) => PyKeyError::new_err(name),
        compression::CompressionError::Codec(message) => PyValueError::new_err(message),
    }
}
