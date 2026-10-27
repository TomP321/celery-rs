//! Binding Python del client Redis del result backend.
//!
//! Espone `_celery_rs.RedisClient`, `RedisPipeline` e `RedisPubSub`: le stesse
//! operazioni che il backend usa da redis-py, con l'I/O in Rust. Il GIL resta
//! rilasciato per tutta la durata delle chiamate (`py.detach`), e il runtime
//! tokio è quello condiviso di `transport.rs` (stesso fork safety).

use crate::fork::ForkSafe;

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyByteArray, PyBytes, PyDict, PyList, PyString};

use celery_backend::redis::{
    Address, Client, Command, Config, Error as RedisError, Pipeline, PubSubConnection, Reply,
};

use crate::transport;

fn block<T>(future: impl Future<Output = Result<T, RedisError>>) -> PyResult<T> {
    let runtime = transport::runtime().map_err(PyRuntimeError::new_err)?;
    runtime.block_on(future).map_err(redis_error)
}

/// Le eccezioni sono quelle di redis-py: `connection_errors`, `ensure` e
/// `_reconnect_pubsub` del backend restano invariati.
static REDIS_EXCEPTIONS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn redis_exceptions(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    Ok(REDIS_EXCEPTIONS
        .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
            Ok(py.import("redis.exceptions")?.unbind().into())
        })?
        .bind(py)
        .clone())
}

fn redis_error(error: RedisError) -> PyErr {
    let name = match error {
        RedisError::Connection(_) => "ConnectionError",
        RedisError::Timeout(_) => "TimeoutError",
        RedisError::Response(_) => "ResponseError",
        RedisError::Authentication(_) => "AuthenticationError",
        RedisError::Invalid(_) => return PyValueError::new_err(error.to_string()),
    };
    Python::attach(|py| {
        match redis_exceptions(py)
            .and_then(|module| module.getattr(name))
            .and_then(|class| class.call1((error.to_string(),)))
        {
            Ok(exception) => PyErr::from_value(exception),
            Err(_) => PyRuntimeError::new_err(error.to_string()),
        }
    })
}

fn bytes_arg(value: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return Ok(Bytes::copy_from_slice(bytes.as_bytes()));
    }
    if let Ok(bytes) = value.cast::<PyByteArray>() {
        return Ok(Bytes::from(bytes.to_vec()));
    }
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(Bytes::copy_from_slice(text.to_str()?.as_bytes()));
    }
    Err(PyTypeError::new_err(
        "argomenti Redis: attesi bytes, bytearray o str",
    ))
}

fn timeouts(seconds: Option<f64>) -> PyResult<Option<Duration>> {
    match seconds {
        None => Ok(None),
        Some(seconds) if seconds < 0.0 => Err(PyValueError::new_err(
            "timeout negativo non valido per Redis",
        )),
        Some(seconds) => Ok(Some(Duration::from_secs_f64(seconds))),
    }
}

fn reply_to_py(py: Python<'_>, reply: Reply) -> PyResult<Py<PyAny>> {
    Ok(match reply {
        Reply::Nil => py.None(),
        Reply::Bytes(bytes) => PyBytes::new(py, &bytes).into_any().unbind(),
        Reply::Int(number) => number.into_pyobject(py)?.into_any().unbind(),
        Reply::Bool(value) => value.into_pyobject(py)?.to_owned().into_any().unbind(),
        Reply::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(reply_to_py(py, item)?)?;
            }
            list.into_any().unbind()
        }
    })
}

/// Comando singolo: un wrapper sottile su [`Command`].
#[pyclass(module = "_celery_rs")]
pub struct RedisClient {
    inner: ForkSafe<Arc<Client>>,
}

impl RedisClient {
    fn command(&self, py: Python<'_>, command: Command) -> PyResult<Py<PyAny>> {
        let reply = py.detach(|| block(self.inner.execute(command)))?;
        reply_to_py(py, reply)
    }
}

#[pymethods]
impl RedisClient {
    #[new]
    #[pyo3(signature = (host="localhost", port=6379, db=0, username=None, password=None,
                        socket_timeout=None, socket_connect_timeout=None, client_name=None,
                        max_connections=None, path=None, ssl=false, ssl_cert_reqs=None,
                        health_check_interval=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        host: &str,
        port: u16,
        db: i64,
        username: Option<String>,
        password: Option<String>,
        socket_timeout: Option<f64>,
        socket_connect_timeout: Option<f64>,
        client_name: Option<String>,
        max_connections: Option<usize>,
        path: Option<String>,
        ssl: bool,
        ssl_cert_reqs: Option<String>,
        health_check_interval: Option<f64>,
    ) -> PyResult<Self> {
        let address = match path {
            Some(path) => Address::Unix(path.into()),
            None if ssl => Address::Tls {
                host: host.to_string(),
                port,
                // Solo `CERT_REQUIRED` arriva qui: le altre modalità restano
                // su redis-py (attivazione in `RedisBackend`).
                insecure: matches!(
                    ssl_cert_reqs.as_deref(),
                    Some("CERT_NONE") | Some("none") | Some("0")
                ),
            },
            None => Address::Tcp {
                host: host.to_string(),
                port,
            },
        };
        let config = Config {
            address,
            db,
            username,
            password,
            response_timeout: timeouts(socket_timeout)?,
            connect_timeout: timeouts(socket_connect_timeout)?,
            client_name,
            max_connections,
            health_check_interval: timeouts(health_check_interval)?,
        };
        let client = Client::open(config).map_err(redis_error)?;
        Ok(Self {
            inner: ForkSafe::new(Arc::new(client)),
        })
    }

    fn get(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.command(
            py,
            Command::Get {
                key: bytes_arg(key)?,
            },
        )
    }

    fn mget(&self, py: Python<'_>, keys: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let keys = keys
            .try_iter()?
            .map(|item| bytes_arg(&item?))
            .collect::<PyResult<Vec<_>>>()?;
        self.command(py, Command::MGet { keys })
    }

    #[pyo3(signature = (key, value, ex=None))]
    fn set(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        ex: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        self.command(
            py,
            Command::Set {
                key: bytes_arg(key)?,
                value: bytes_arg(value)?,
                ex,
            },
        )
    }

    fn setex(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        seconds: u64,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.command(
            py,
            Command::SetEx {
                key: bytes_arg(key)?,
                seconds,
                value: bytes_arg(value)?,
            },
        )
    }

    fn delete(&self, py: Python<'_>, keys: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let keys = keys
            .try_iter()?
            .map(|item| bytes_arg(&item?))
            .collect::<PyResult<Vec<_>>>()?;
        self.command(py, Command::Delete { keys })
    }

    #[pyo3(signature = (key, amount=None))]
    fn incr(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        amount: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        self.command(
            py,
            Command::Incr {
                key: bytes_arg(key)?,
                amount,
            },
        )
    }

    fn expire(&self, py: Python<'_>, key: &Bound<'_, PyAny>, seconds: i64) -> PyResult<Py<PyAny>> {
        self.command(
            py,
            Command::Expire {
                key: bytes_arg(key)?,
                seconds,
            },
        )
    }

    fn ttl(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.command(
            py,
            Command::Ttl {
                key: bytes_arg(key)?,
            },
        )
    }

    fn publish(
        &self,
        py: Python<'_>,
        channel: &Bound<'_, PyAny>,
        message: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.command(
            py,
            Command::Publish {
                channel: bytes_arg(channel)?,
                message: bytes_arg(message)?,
            },
        )
    }

    fn zadd(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        pairs: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let mut parsed = Vec::new();
        for pair in pairs.try_iter()? {
            let pair = pair?;
            let pair = pair.cast::<pyo3::types::PyTuple>()?;
            parsed.push((
                bytes_arg(&pair.get_item(0)?)?,
                bytes_arg(&pair.get_item(1)?)?,
            ));
        }
        self.command(
            py,
            Command::ZAdd {
                key: bytes_arg(key)?,
                pairs: parsed,
            },
        )
    }

    fn zcount(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        min: &Bound<'_, PyAny>,
        max: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.command(
            py,
            Command::ZCount {
                key: bytes_arg(key)?,
                min: bytes_arg(min)?,
                max: bytes_arg(max)?,
            },
        )
    }

    fn zrange(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        start: i64,
        stop: i64,
    ) -> PyResult<Py<PyAny>> {
        self.command(
            py,
            Command::ZRange {
                key: bytes_arg(key)?,
                start,
                stop,
            },
        )
    }

    fn rpush(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let values = values
            .try_iter()?
            .map(|item| bytes_arg(&item?))
            .collect::<PyResult<Vec<_>>>()?;
        self.command(
            py,
            Command::RPush {
                key: bytes_arg(key)?,
                values,
            },
        )
    }

    fn llen(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.command(
            py,
            Command::LLen {
                key: bytes_arg(key)?,
            },
        )
    }

    fn lrange(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        start: i64,
        stop: i64,
    ) -> PyResult<Py<PyAny>> {
        self.command(
            py,
            Command::LRange {
                key: bytes_arg(key)?,
                start,
                stop,
            },
        )
    }

    #[pyo3(signature = (transaction=true))]
    fn pipeline(&self, transaction: bool) -> RedisPipeline {
        RedisPipeline {
            client: ForkSafe::new(Arc::clone(&self.inner)),
            state: Mutex::new(Pipeline::new(transaction)),
        }
    }

    fn pubsub(&self, py: Python<'_>) -> PyResult<RedisPubSub> {
        let inner = py.detach(|| block(self.inner.pubsub()))?;
        Ok(RedisPubSub {
            inner: ForkSafe::new(Arc::new(inner)),
            closed: Mutex::new(false),
        })
    }

    /// Scarta le connessioni inattive (`ConnectionPool.reset()`).
    fn reset(&self) {
        self.inner.reset();
    }
}

#[pyclass(module = "_celery_rs")]
pub struct RedisPipeline {
    client: ForkSafe<Arc<Client>>,
    state: Mutex<Pipeline>,
}

impl RedisPipeline {
    fn push(&self, command: Command) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.push(command);
    }
}

#[pymethods]
impl RedisPipeline {
    #[pyo3(signature = (key, value, ex=None))]
    fn set(
        &self,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        ex: Option<u64>,
    ) -> PyResult<()> {
        self.push(Command::Set {
            key: bytes_arg(key)?,
            value: bytes_arg(value)?,
            ex,
        });
        Ok(())
    }

    fn setex(
        &self,
        key: &Bound<'_, PyAny>,
        seconds: u64,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.push(Command::SetEx {
            key: bytes_arg(key)?,
            seconds,
            value: bytes_arg(value)?,
        });
        Ok(())
    }

    fn publish(&self, channel: &Bound<'_, PyAny>, message: &Bound<'_, PyAny>) -> PyResult<()> {
        self.push(Command::Publish {
            channel: bytes_arg(channel)?,
            message: bytes_arg(message)?,
        });
        Ok(())
    }

    fn delete(&self, keys: &Bound<'_, PyAny>) -> PyResult<()> {
        let keys = keys
            .try_iter()?
            .map(|item| bytes_arg(&item?))
            .collect::<PyResult<Vec<_>>>()?;
        self.push(Command::Delete { keys });
        Ok(())
    }

    #[pyo3(signature = (key, amount=None))]
    fn incr(&self, key: &Bound<'_, PyAny>, amount: Option<i64>) -> PyResult<()> {
        self.push(Command::Incr {
            key: bytes_arg(key)?,
            amount,
        });
        Ok(())
    }

    fn expire(&self, key: &Bound<'_, PyAny>, seconds: i64) -> PyResult<()> {
        self.push(Command::Expire {
            key: bytes_arg(key)?,
            seconds,
        });
        Ok(())
    }

    fn zadd(&self, key: &Bound<'_, PyAny>, pairs: &Bound<'_, PyAny>) -> PyResult<()> {
        let mut parsed = Vec::new();
        for pair in pairs.try_iter()? {
            let pair = pair?;
            let pair = pair.cast::<pyo3::types::PyTuple>()?;
            parsed.push((
                bytes_arg(&pair.get_item(0)?)?,
                bytes_arg(&pair.get_item(1)?)?,
            ));
        }
        self.push(Command::ZAdd {
            key: bytes_arg(key)?,
            pairs: parsed,
        });
        Ok(())
    }

    fn zcount(
        &self,
        key: &Bound<'_, PyAny>,
        min: &Bound<'_, PyAny>,
        max: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.push(Command::ZCount {
            key: bytes_arg(key)?,
            min: bytes_arg(min)?,
            max: bytes_arg(max)?,
        });
        Ok(())
    }

    fn zrange(&self, key: &Bound<'_, PyAny>, start: i64, stop: i64) -> PyResult<()> {
        self.push(Command::ZRange {
            key: bytes_arg(key)?,
            start,
            stop,
        });
        Ok(())
    }

    fn rpush(&self, key: &Bound<'_, PyAny>, values: &Bound<'_, PyAny>) -> PyResult<()> {
        let values = values
            .try_iter()?
            .map(|item| bytes_arg(&item?))
            .collect::<PyResult<Vec<_>>>()?;
        self.push(Command::RPush {
            key: bytes_arg(key)?,
            values,
        });
        Ok(())
    }

    fn llen(&self, key: &Bound<'_, PyAny>) -> PyResult<()> {
        self.push(Command::LLen {
            key: bytes_arg(key)?,
        });
        Ok(())
    }

    fn lrange(&self, key: &Bound<'_, PyAny>, start: i64, stop: i64) -> PyResult<()> {
        self.push(Command::LRange {
            key: bytes_arg(key)?,
            start,
            stop,
        });
        Ok(())
    }

    fn get(&self, key: &Bound<'_, PyAny>) -> PyResult<()> {
        self.push(Command::Get {
            key: bytes_arg(key)?,
        });
        Ok(())
    }

    fn mget(&self, keys: &Bound<'_, PyAny>) -> PyResult<()> {
        let keys = keys
            .try_iter()?
            .map(|item| bytes_arg(&item?))
            .collect::<PyResult<Vec<_>>>()?;
        self.push(Command::MGet { keys });
        Ok(())
    }

    fn execute(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let transaction = state.transaction;
        let pipeline = std::mem::replace(&mut *state, Pipeline::new(transaction));
        drop(state);
        let replies = py.detach(|| block(self.client.execute_pipeline(pipeline)))?;
        let list = PyList::empty(py);
        for reply in replies {
            list.append(reply_to_py(py, reply)?)?;
        }
        Ok(list.into_any().unbind())
    }

    fn reset(&self) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }
}

#[pyclass(module = "_celery_rs")]
pub struct RedisPubSub {
    inner: ForkSafe<Arc<PubSubConnection>>,
    closed: Mutex<bool>,
}

#[pymethods]
impl RedisPubSub {
    fn subscribe(&self, py: Python<'_>, channels: Vec<String>) -> PyResult<()> {
        py.detach(|| self.inner.subscribe(&channels).map_err(redis_error))
    }

    fn unsubscribe(&self, py: Python<'_>, channels: Vec<String>) -> PyResult<()> {
        py.detach(|| block(self.inner.unsubscribe(&channels)))
    }

    /// `can_read(timeout=0)`: c'è un messaggio senza consumarlo?
    #[pyo3(signature = (timeout=None))]
    fn can_read(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<bool> {
        let timeout = timeouts(timeout)?;
        py.detach(|| block(self.inner.can_read(timeout)))
    }

    /// `get_message(timeout)`: messaggio o `None`.
    #[pyo3(signature = (timeout=None))]
    fn get_message(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Py<PyDict>>> {
        let timeout = timeouts(timeout)?;
        let message = py.detach(|| block(self.inner.get_message(timeout)))?;
        message
            .map(|message| {
                let dict = PyDict::new(py);
                dict.set_item("type", "message")?;
                dict.set_item("pattern", py.None())?;
                dict.set_item("channel", PyBytes::new(py, message.channel.as_bytes()))?;
                dict.set_item("data", PyBytes::new(py, &message.payload))?;
                Ok(dict.unbind())
            })
            .transpose()
    }

    fn close(&self) {
        let mut closed = self
            .closed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !*closed {
            self.inner.close();
            *closed = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeouts_reject_negative() {
        assert!(timeouts(Some(-1.0)).is_err());
        assert_eq!(
            timeouts(Some(0.5)).expect("ok"),
            Some(Duration::from_millis(500))
        );
        assert_eq!(timeouts(None).expect("ok"), None);
    }
}
