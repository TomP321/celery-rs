//! Modulo di estensione `_celery_rs`: espone il core Rust al guscio Python.
use pyo3::prelude::*;

mod backend_redis;
mod compress;
mod json;
mod saferepr;
mod task;
mod transport;
mod util;
mod worker;

#[pymodule]
fn _celery_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(json::json_dumps, m)?)?;
    m.add_function(wrap_pyfunction!(json::json_loads, m)?)?;
    m.add_function(wrap_pyfunction!(saferepr::saferepr, m)?)?;
    m.add_function(wrap_pyfunction!(compress::compress, m)?)?;
    m.add_function(wrap_pyfunction!(compress::decompress, m)?)?;
    m.add_function(wrap_pyfunction!(task::encode_task_v2, m)?)?;
    m.add_function(wrap_pyfunction!(transport::after_fork, m)?)?;
    m.add_class::<transport::Connection>()?;
    m.add_class::<worker::PyWaker>()?;
    m.add_class::<worker::PyWorkerConsumer>()?;
    m.add_class::<backend_redis::RedisClient>()?;
    m.add_class::<backend_redis::RedisPipeline>()?;
    m.add_class::<backend_redis::RedisPubSub>()?;
    Ok(())
}
