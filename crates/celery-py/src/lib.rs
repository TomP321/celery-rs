//! Modulo di estensione `_celery_rs`: espone il core Rust al guscio Python.
use pyo3::prelude::*;

mod backend_redis;
mod compress;
mod cron;
mod fork;
mod json;
#[cfg(feature = "memory-probe")]
mod memory_probe;
mod saferepr;
mod task;
mod transport;
mod util;
mod worker;

#[pymodule]
fn _celery_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    #[cfg(feature = "memory-probe")]
    m.add_function(wrap_pyfunction!(memory_probe::_allocator_stats, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("CronParseError", m.py().get_type::<cron::CronParseError>())?;
    m.add_function(wrap_pyfunction!(json::json_dumps, m)?)?;
    m.add_function(wrap_pyfunction!(json::json_loads, m)?)?;
    m.add_function(wrap_pyfunction!(saferepr::saferepr, m)?)?;
    m.add_function(wrap_pyfunction!(compress::compress, m)?)?;
    m.add_function(wrap_pyfunction!(compress::decompress, m)?)?;
    m.add_function(wrap_pyfunction!(task::encode_task_v2, m)?)?;
    m.add_function(wrap_pyfunction!(cron::cron_parse, m)?)?;
    m.add_function(wrap_pyfunction!(transport::after_fork, m)?)?;
    m.add_class::<cron::PyCronSpec>()?;
    m.add_class::<transport::Connection>()?;
    m.add_class::<worker::PyWaker>()?;
    m.add_class::<worker::PyWorkerConsumer>()?;
    m.add_class::<worker::PyPool>()?;
    m.add_class::<worker::PyPoolEvent>()?;
    m.add_class::<backend_redis::RedisClient>()?;
    m.add_class::<backend_redis::RedisPipeline>()?;
    m.add_class::<backend_redis::RedisPubSub>()?;
    let kwargs = pyo3::types::PyDict::new(m.py());
    kwargs.set_item("after_in_child", m.getattr("after_fork")?)?;
    m.py()
        .import("os")?
        .call_method("register_at_fork", (), Some(&kwargs))?;
    Ok(())
}
