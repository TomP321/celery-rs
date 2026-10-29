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
    let addr = data.as_ptr() as usize;
    let len = data.len();
    // SAFETY: body stays alive and bytes are immutable; releasing the GIL cannot invalidate the buffer.
    let out = py.detach(move || unsafe {
        compression::compress(name, std::slice::from_raw_parts(addr as *const u8, len))
    });
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
    let addr = data.as_ptr() as usize;
    let len = data.len();
    // SAFETY: body stays alive and its immutable bytes remain valid while the GIL is released.
    let out = py.detach(move || unsafe {
        compression::decompress(
            content_type,
            std::slice::from_raw_parts(addr as *const u8, len),
        )
    });
    let out = out.map_err(map_error)?;
    Ok(PyBytes::new(py, &out).unbind())
}

fn map_error(err: compression::CompressionError) -> PyErr {
    match err {
        compression::CompressionError::UnknownMethod(name) => PyKeyError::new_err(name),
        compression::CompressionError::Codec(message) => PyValueError::new_err(message),
    }
}
