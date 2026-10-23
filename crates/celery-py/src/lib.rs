//! Modulo di estensione `_celery_rs`: espone il core Rust al guscio Python.
use pyo3::prelude::*;

mod compress;
mod json;
mod saferepr;
mod task;
mod util;

#[pymodule]
fn _celery_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(json::json_dumps, m)?)?;
    m.add_function(wrap_pyfunction!(json::json_loads, m)?)?;
    m.add_function(wrap_pyfunction!(saferepr::saferepr, m)?)?;
    m.add_function(wrap_pyfunction!(compress::compress, m)?)?;
    m.add_function(wrap_pyfunction!(compress::decompress, m)?)?;
    m.add_function(wrap_pyfunction!(task::encode_task_v2, m)?)?;
    Ok(())
}
