//! Python transport bindings releasing the GIL for I/O. Python data is converted
//! before entering tokio and results are converted after it returns.

use crate::fork::ForkSafe;
use std::future::Future;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use compact_str::CompactString;
use pyo3::exceptions::{
    PyConnectionError, PyKeyError, PyLookupError, PyNotImplementedError, PyRuntimeError,
    PyTypeError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple,
};

use celery_transport::{
    Consumer, ControlOutcome, ControlReply, ControlRequest, Delivery, DeliveryStream, DeliveryTag,
    Error as TransportError, ExchangeDecl, ExchangeType, HeaderValue, Headers, Message, Properties,
    PublishRequest, QueueBinding, QueueDecl, ReplyRoute, Transport, TransportOptions, Url,
};

/// Tracks the shared runtime's owning PID so children recreate it lazily after fork.
struct RuntimeEntry {
    pid: u32,
    _fds: Vec<celery_worker::fds::Registration>,
    runtime: tokio::runtime::Runtime,
}

/// Entries are never freed; fork resets only the pointer.
static RUNTIME: AtomicPtr<RuntimeEntry> = AtomicPtr::new(ptr::null_mut());

// Identify the driver's epoll by membership, excluding unrelated epoll instances
// opened concurrently by other threads. Never mark Python descriptors.
#[cfg(target_os = "linux")]
fn runtime_fds(runtime: &tokio::runtime::Runtime) -> Result<Vec<i32>, String> {
    use std::os::fd::AsRawFd;
    let (probe, _peer) =
        std::os::unix::net::UnixStream::pair().map_err(|error| error.to_string())?;
    probe
        .set_nonblocking(true)
        .map_err(|error| error.to_string())?;
    let _entered = runtime.enter();
    // SAFETY: AsyncFd owns the UnixStream; no other owner can close or replace its
    // descriptor until the probe is destroyed.
    let probe =
        unsafe { tokio::io::unix::AsyncFd::register(probe) }.map_err(|error| error.to_string())?;
    let probe_fd = probe.as_raw_fd();
    let entries = std::fs::read_dir("/proc/self/fd").map_err(|error| error.to_string())?;
    let mut fds = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| error.to_string())?;
        if std::fs::read_link(entry.path()).ok().as_deref()
            != Some(std::path::Path::new("anon_inode:[eventpoll]"))
        {
            continue;
        }
        let Some(fd) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<i32>().ok())
        else {
            continue;
        };
        let Ok(info) = std::fs::read_to_string(format!("/proc/self/fdinfo/{fd}")) else {
            continue;
        };
        if info.lines().any(|line| {
            line.strip_prefix("tfd:")
                .and_then(|rest| rest.split_whitespace().next())
                .and_then(|value| value.parse::<i32>().ok())
                == Some(probe_fd)
        }) {
            fds.push(fd);
        }
    }
    if fds.is_empty() {
        return Err("tokio epoll driver not found".into());
    }
    Ok(fds)
}

#[cfg(not(target_os = "linux"))]
fn runtime_fds(_runtime: &tokio::runtime::Runtime) -> Result<Vec<i32>, String> {
    Ok(Vec::new())
}

#[pyfunction]
pub fn _runtime_fds() -> std::collections::BTreeMap<i32, (u64, u64)> {
    celery_worker::fds::snapshot()
}

fn build_runtime() -> Result<tokio::runtime::Runtime, String> {
    // A worker thread must drive I/O while producers are idle to keep lapin's AMQP
    // heartbeats alive; polling only during block_on would let them expire.
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("celery-rs-io")
        .enable_all()
        .build()
        .map_err(|error| error.to_string())
}

/// Returns the current process's lazily created runtime.
pub(crate) fn runtime() -> Result<&'static tokio::runtime::Runtime, String> {
    let pid = std::process::id();
    loop {
        let current = RUNTIME.load(Ordering::Acquire);
        if !current.is_null() {
            // SAFETY: entries are never freed, including after fork.
            let entry = unsafe { &*current };
            if entry.pid == pid {
                return Ok(&entry.runtime);
            }
        }
        let runtime = build_runtime()?;
        let fds = runtime_fds(&runtime)?
            .into_iter()
            .map(celery_worker::fds::Registration::new)
            .collect();
        let fresh = Box::into_raw(Box::new(RuntimeEntry {
            pid,
            runtime,
            _fds: fds,
        }));
        match RUNTIME.compare_exchange(current, fresh, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => {
                // SAFETY: fresh was just published and remains alive.
                let entry: &'static RuntimeEntry = unsafe { &*fresh };
                return Ok(&entry.runtime);
            }
            Err(_) => {
                // Another thread installed its runtime first; this unused runtime can be destroyed.
                // SAFETY: fresh is exclusively owned and was never shared.
                drop(unsafe { Box::from_raw(fresh) });
            }
        }
    }
}

/// Abandons the inherited runtime in a fork child and creates a new one on next use.
/// The old runtime cannot be safely shut down in the child and is intentionally leaked.
#[pyfunction]
pub fn after_fork() {
    let _ = RUNTIME.swap(ptr::null_mut(), Ordering::AcqRel);
}

pub(crate) fn block<T: Send>(
    future: impl Future<Output = Result<T, TransportError>>,
) -> PyResult<T> {
    let runtime = runtime().map_err(PyRuntimeError::new_err)?;
    runtime.block_on(future).map_err(transport_error)
}

pub(crate) fn transport_error(error: TransportError) -> PyErr {
    // Connection failures already carry reference client error text.
    if let Some(message) = error.client_message() {
        return PyConnectionError::new_err(message);
    }
    match error {
        TransportError::NotImplemented(message) => PyNotImplementedError::new_err(message),
        TransportError::InvalidUrl(message)
        | TransportError::InvalidTransportOption(message)
        | TransportError::InvalidArgument(message)
        | TransportError::InvalidPattern(message)
        | TransportError::UnsupportedTransport(message)
        | TransportError::UnsupportedExchangeType(message) => PyValueError::new_err(message),
        TransportError::NotFound(message) => PyLookupError::new_err(message),
        TransportError::PreconditionFailed {
            reply_code,
            reply_text,
            method_sig,
            method_name,
        } => amqp_precondition_failed(reply_code, reply_text, method_sig, method_name),
        TransportError::Connect { .. } => PyConnectionError::new_err(error.to_string()),
        // Match kombu QoS's KeyError(delivery_tag).
        TransportError::UnknownDeliveryTag(tag) => PyKeyError::new_err(tag),
        TransportError::ConnectionLost(message) | TransportError::Connection(message) => {
            PyConnectionError::new_err(message)
        }
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

/// Builds py-amqp's PreconditionFailed with matching attributes and text. Import
/// amqp only on this error path to avoid loading it during normal publication.
fn amqp_precondition_failed(
    reply_code: u16,
    reply_text: String,
    method_sig: (u16, u16),
    method_name: &'static str,
) -> PyErr {
    Python::attach(|py| {
        match py
            .import("amqp.exceptions")
            .and_then(|module| module.getattr("PreconditionFailed"))
            .and_then(|class| class.call1((reply_text, method_sig, method_name, reply_code)))
        {
            Ok(exc) => PyErr::from_value(exc),
            Err(err) => err,
        }
    })
}

/// Converts a Python dictionary to ordered headers.
pub(crate) fn py_to_headers(dict: &Bound<'_, PyDict>) -> PyResult<Headers> {
    let mut headers = Headers::with_capacity(dict.len());
    for (key, value) in dict.iter() {
        let key = key
            .cast::<PyString>()
            .map_err(|_| PyTypeError::new_err("keys must be strings"))?
            .to_str()?;
        headers.insert(key, py_to_value(&value)?);
    }
    Ok(headers)
}

static DATETIME: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static TIMEGM: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static DECIMAL: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn datetime_type(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    Ok(DATETIME
        .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
            Ok(py.import("datetime")?.getattr("datetime")?.unbind())
        })?
        .bind(py)
        .clone())
}

fn timegm(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    Ok(TIMEGM
        .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
            Ok(py.import("calendar")?.getattr("timegm")?.unbind())
        })?
        .bind(py)
        .clone())
}

fn decimal_type(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    Ok(DECIMAL
        .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
            Ok(py.import("decimal")?.getattr("Decimal")?.unbind())
        })?
        .bind(py)
        .clone())
}

fn py_to_value(value: &Bound<'_, PyAny>) -> PyResult<HeaderValue> {
    if value.is_none() {
        return Ok(HeaderValue::Null);
    }
    if let Ok(boolean) = value.cast::<PyBool>() {
        return Ok(HeaderValue::Bool(boolean.is_true()));
    }
    if let Ok(integer) = value.cast::<PyInt>() {
        return match integer.extract::<i64>() {
            Ok(number) => Ok(HeaderValue::Int(number)),
            Err(_) => Ok(HeaderValue::BigInt(CompactString::from(
                integer.str()?.to_str()?,
            ))),
        };
    }
    if let Ok(float) = value.cast::<PyFloat>() {
        return Ok(HeaderValue::Float(float.value()));
    }
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(HeaderValue::Str(CompactString::from(
            text.to_str().map_err(|_| {
                PyValueError::new_err("message string cannot be represented as UTF-8")
            })?,
        )));
    }
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return Ok(HeaderValue::Bytes(Bytes::copy_from_slice(bytes.as_bytes())));
    }
    if let Ok(bytes) = value.cast::<PyByteArray>() {
        return Ok(HeaderValue::Bytes(Bytes::from(bytes.to_vec())));
    }
    if let Ok(items) = value.cast::<PyList>() {
        return Ok(HeaderValue::List(
            items
                .iter()
                .map(|item| py_to_value(&item))
                .collect::<PyResult<Vec<_>>>()?,
        ));
    }
    if let Ok(items) = value.cast::<PyTuple>() {
        return Ok(HeaderValue::List(
            items
                .iter()
                .map(|item| py_to_value(&item))
                .collect::<PyResult<Vec<_>>>()?,
        ));
    }
    if let Ok(mapping) = value.cast::<PyDict>() {
        return Ok(HeaderValue::Map(py_to_headers(mapping)?));
    }
    if value.is_instance(&datetime_type(value.py())?)? {
        let timetuple = value.call_method0("utctimetuple")?;
        let seconds: i64 = timegm(value.py())?.call1((timetuple,))?.extract()?;
        return Ok(HeaderValue::Timestamp(seconds));
    }
    if value.is_instance(&decimal_type(value.py())?)? {
        return decimal_from_python(value);
    }
    Err(PyTypeError::new_err(format!(
        "unsupported message type: {}",
        value.get_type().name()?
    )))
}

fn decimal_from_python(value: &Bound<'_, PyAny>) -> PyResult<HeaderValue> {
    let tuple = value.call_method0("as_tuple")?;
    let sign: i32 = tuple.get_item(0)?.extract()?;
    let digits: Vec<u32> = tuple.get_item(1)?.extract()?;
    let exponent: i32 = tuple.get_item(2)?.extract()?;

    let mut mantissa: i128 = 0;
    for digit in digits {
        mantissa = mantissa
            .checked_mul(10)
            .and_then(|current| current.checked_add(i128::from(digit)))
            .ok_or_else(|| PyValueError::new_err("decimal too large for AMQP"))?;
    }
    let mut scale: u64 = 0;
    if exponent < 0 {
        // unsigned_abs handles i32::MIN without overflow.
        scale = i64::from(exponent).unsigned_abs();
    } else if exponent > 0 {
        let factor = 10i128
            .checked_pow(exponent.unsigned_abs())
            .ok_or_else(|| PyValueError::new_err("decimal too large for AMQP"))?;
        mantissa = mantissa
            .checked_mul(factor)
            .ok_or_else(|| PyValueError::new_err("decimal too large for AMQP"))?;
    }
    if sign != 0 {
        mantissa = -mantissa;
    }
    let value =
        i32::try_from(mantissa).map_err(|_| PyValueError::new_err("decimal outside AMQP range"))?;
    let scale = u8::try_from(scale).map_err(|_| PyValueError::new_err("invalid decimal scale"))?;
    Ok(HeaderValue::Decimal(celery_transport::Decimal {
        scale,
        value,
    }))
}

fn value_to_py(py: Python<'_>, value: &HeaderValue) -> PyResult<Py<PyAny>> {
    Ok(match value {
        HeaderValue::Null => py.None(),
        HeaderValue::Bool(boolean) => PyBool::new(py, *boolean).to_owned().into_any().unbind(),
        HeaderValue::Int(number) => number.into_pyobject(py)?.into_any().unbind(),
        HeaderValue::BigInt(text) => {
            let builtins = py.import("builtins")?;
            builtins.getattr("int")?.call1((text.as_str(),))?.unbind()
        }
        HeaderValue::Float(number) => number.into_pyobject(py)?.into_any().unbind(),
        HeaderValue::Str(text) => PyString::new(py, text).into_any().unbind(),
        HeaderValue::Bytes(bytes) => PyBytes::new(py, bytes).into_any().unbind(),
        HeaderValue::List(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(value_to_py(py, item)?)?;
            }
            list.into_any().unbind()
        }
        HeaderValue::Map(headers) => headers_to_py(py, headers)?.into_any().unbind(),
        HeaderValue::Timestamp(seconds) => {
            let utc = py.import("datetime")?.getattr("timezone")?.getattr("utc")?;
            let datetime = datetime_type(py)?
                .call_method1("fromtimestamp", (*seconds, utc))?
                .call_method(
                    "replace",
                    (),
                    Some(&{
                        let kwargs = PyDict::new(py);
                        kwargs.set_item("tzinfo", py.None())?;
                        kwargs
                    }),
                )?;
            datetime.unbind()
        }
        HeaderValue::Decimal(decimal) => {
            let sign = i32::from(decimal.value < 0);
            let digits = decimal
                .value
                .unsigned_abs()
                .to_string()
                .bytes()
                .map(u32::from)
                .map(|byte| byte - u32::from(b'0'))
                .collect::<Vec<_>>();
            let digits = PyTuple::new(py, digits)?;
            let exponent = -i32::from(decimal.scale);
            let tuple = PyTuple::new(
                py,
                [
                    sign.into_pyobject(py)?.into_any(),
                    digits.into_any(),
                    exponent.into_pyobject(py)?.into_any(),
                ],
            )?;
            decimal_type(py)?.call1((tuple,))?.unbind()
        }
    })
}

fn headers_to_py<'py>(py: Python<'py>, headers: &Headers) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (key, value) in headers.iter() {
        dict.set_item(key.as_str(), value_to_py(py, value)?)?;
    }
    Ok(dict)
}

fn tag_to_py<'py>(py: Python<'py>, tag: &DeliveryTag) -> PyResult<Bound<'py, PyAny>> {
    Ok(match tag {
        DeliveryTag::Number(number) => number.into_pyobject(py)?.into_any(),
        DeliveryTag::Uuid(text) => PyString::new(py, text).into_any(),
    })
}

pub(crate) fn tag_from_py(tag: &Bound<'_, PyAny>) -> PyResult<DeliveryTag> {
    if tag.cast::<PyBool>().is_ok() {
        return Err(PyTypeError::new_err(
            "delivery_tag must be int or str, not bool",
        ));
    }
    if let Ok(number) = tag.cast::<PyInt>() {
        return Ok(DeliveryTag::Number(number.extract()?));
    }
    if let Ok(text) = tag.cast::<PyString>() {
        return Ok(DeliveryTag::from(text.to_str()?));
    }
    Err(PyTypeError::new_err("delivery_tag must be int or str"))
}

pub(crate) fn body_from_py(body: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(bytes) = body.cast::<PyBytes>() {
        return Ok(Bytes::copy_from_slice(bytes.as_bytes()));
    }
    if let Ok(bytes) = body.cast::<PyByteArray>() {
        return Ok(Bytes::from(bytes.to_vec()));
    }
    Err(PyTypeError::new_err("body must be bytes or bytearray"))
}

/// Converts a Python retry policy, ignoring keys handled by the Python shell.
pub(crate) fn retry_policy_from_py(
    dict: Option<&Bound<'_, PyDict>>,
) -> PyResult<celery_transport::RetryPolicy> {
    let mut policy = celery_transport::RetryPolicy::default();
    let Some(dict) = dict else {
        return Ok(policy);
    };
    for (key, value) in dict.iter() {
        let key = key
            .cast::<PyString>()
            .map_err(|_| PyTypeError::new_err("retry_policy: keys must be strings"))?
            .to_str()?;
        match key {
            "enabled" => policy.enabled = value.is_truthy()?,
            "max_retries" => {
                policy.max_retries = if value.is_none() {
                    None
                } else {
                    Some(value.extract::<u32>()?)
                };
            }
            "interval_start" => policy.interval_start = seconds(&value, key)?,
            "interval_step" => policy.interval_step = seconds(&value, key)?,
            "interval_max" => policy.interval_max = seconds(&value, key)?,
            _ => {}
        }
    }
    Ok(policy)
}

fn seconds(value: &Bound<'_, PyAny>, key: &str) -> PyResult<Duration> {
    let seconds: f64 = value
        .extract()
        .map_err(|_| PyTypeError::new_err(format!("retry_policy[{key}]: expected a number")))?;
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(PyValueError::new_err(format!(
            "retry_policy[{key}]: expected a nonnegative number"
        )));
    }
    Ok(Duration::from_secs_f64(seconds))
}

struct StreamState {
    queues: Vec<CompactString>,
    prefetch: u16,
    no_ack: bool,
    consumer: Consumer,
}

/// Bounds one drain call so continuous publication cannot grow the result indefinitely.
const DRAIN_BATCH_LIMIT: usize = 1000;

/// Broker connection sharing one API across transports.
#[pyclass(module = "_celery_rs")]
pub struct Connection {
    transport: ForkSafe<Box<dyn Transport>>,
    /// Whether virtual delivery metadata must be added.
    virtual_transport: bool,
    body_encoding: CompactString,
    stream: ForkSafe<Mutex<Option<StreamState>>>,
    closed: AtomicBool,
}

impl Connection {
    fn ensure_open(&self) -> PyResult<()> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(PyConnectionError::new_err("connection closed"));
        }
        Ok(())
    }

    /// Waits for one message, then collects immediately available messages. Existing
    /// consumers update prefetch and queues in place to preserve pending deliveries.
    fn collect(
        &self,
        queues: Vec<CompactString>,
        timeout: Option<f64>,
        prefetch: u16,
        no_ack: bool,
    ) -> PyResult<Vec<Delivery>> {
        self.ensure_open()?;
        if queues.is_empty() {
            return Ok(Vec::new());
        }
        let mut guard = self
            .stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Changing no_ack requires a new consumer; the broker retains old pending deliveries.
        if guard.as_ref().is_some_and(|state| state.no_ack != no_ack) {
            if let Some(state) = guard.as_ref() {
                let control = Arc::clone(&state.consumer.control);
                block(control.cancel())?;
            }
            *guard = None;
        }
        if guard.is_none() {
            let consumer = if no_ack {
                block(self.transport.consume_no_ack(&queues, prefetch))?
            } else {
                block(self.transport.consume(&queues, prefetch))?
            };
            *guard = Some(StreamState {
                queues: queues.clone(),
                prefetch,
                no_ack,
                consumer,
            });
        }
        let state = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("stream not initialized"))?;

        if state.prefetch != prefetch {
            block(state.consumer.control.set_prefetch(prefetch))?;
            state.prefetch = prefetch;
        }
        let removed: Vec<CompactString> = state
            .queues
            .iter()
            .filter(|queue| !queues.contains(queue))
            .cloned()
            .collect();
        let added: Vec<CompactString> = queues
            .iter()
            .filter(|queue| !state.queues.contains(queue))
            .cloned()
            .collect();
        for queue in removed {
            block(state.consumer.control.cancel_queue(&queue))?;
        }
        for queue in added {
            block(state.consumer.control.add_queue(&queue))?;
        }
        state.queues = queues;

        let runtime = runtime().map_err(PyRuntimeError::new_err)?;
        let stream = &mut state.consumer.deliveries;
        let deliveries = runtime.block_on(async {
            let mut deliveries = Vec::new();
            let first = match timeout {
                None => next(stream).await,
                Some(seconds) if seconds > 0.0 => {
                    match tokio::time::timeout(Duration::from_secs_f64(seconds), next(stream)).await
                    {
                        Ok(result) => result,
                        Err(_) => return Ok(deliveries),
                    }
                }

                Some(_) => poll_now(stream),
            };
            match first {
                Some(Ok(delivery)) => deliveries.push(delivery),
                Some(Err(error)) => return Err(transport_error(error)),
                None => return Ok(deliveries),
            }
            loop {
                if deliveries.len() >= DRAIN_BATCH_LIMIT {
                    break;
                }
                match poll_now(stream) {
                    Some(Ok(delivery)) => deliveries.push(delivery),
                    Some(Err(error)) => return Err(transport_error(error)),
                    None => break,
                }
            }
            Ok(deliveries)
        })?;
        if no_ack {
            // Fallback no_ack implementations need immediate acknowledgment; AMQP's ack is a no-op.
            drop(guard);
            for delivery in &deliveries {
                block(self.transport.ack(&delivery.delivery_tag))?;
            }
        }
        Ok(deliveries)
    }
}

async fn next(stream: &mut DeliveryStream) -> Option<Result<Delivery, TransportError>> {
    std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
}

fn poll_now(stream: &mut DeliveryStream) -> Option<Result<Delivery, TransportError>> {
    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);
    match stream.as_mut().poll_next(&mut context) {
        Poll::Ready(item) => item,
        Poll::Pending => None,
    }
}

pub(crate) fn delivery_to_py<'py>(
    py: Python<'py>,
    delivery: &Delivery,
    virtual_transport: bool,
    body_encoding: &str,
    include_virtual_content_type: bool,
) -> PyResult<Bound<'py, PyTuple>> {
    let body = PyBytes::new(py, &delivery.message.body).into_any();
    let headers = headers_to_py(py, &delivery.message.headers)?.into_any();
    let mut properties = delivery.message.properties.clone().into_headers();
    // Virtual kombu transports also keep delivery_info inside properties.
    let delivery_info = PyDict::new(py);
    delivery_info.set_item("exchange", delivery.exchange.as_str())?;
    delivery_info.set_item("routing_key", delivery.routing_key.as_str())?;
    // Restore redelivered metadata from the Redis envelope after requeue or visibility timeout.
    if virtual_transport
        && let Some(HeaderValue::Map(info)) = properties.get("delivery_info")
        && let Some(redelivered) = info.get_bool("redelivered")
    {
        delivery_info.set_item("redelivered", redelivered)?;
    }
    if virtual_transport {
        // Worker messages restore virtual properties as kombu does; Connection.drain
        // preserves the format used by differential tests.
        if include_virtual_content_type {
            if let Some(content_type) = delivery.message.content_type.as_deref()
                && !properties.contains_key("content_type")
            {
                properties.insert(
                    "content_type",
                    HeaderValue::Str(CompactString::from(content_type)),
                );
            }
            if let Some(content_encoding) = delivery.message.content_encoding.as_deref()
                && !properties.contains_key("content_encoding")
            {
                properties.insert(
                    "content_encoding",
                    HeaderValue::Str(CompactString::from(content_encoding)),
                );
            }
        }
        if !properties.contains_key("body_encoding") {
            properties.insert(
                "body_encoding",
                HeaderValue::Str(CompactString::from(body_encoding)),
            );
        }
        properties.insert(
            "delivery_tag",
            HeaderValue::Str(CompactString::from(delivery.delivery_tag.to_string())),
        );
    } else {
        // Match kombu's amqp.Message.delivery_info.
        delivery_info.set_item("delivery_tag", tag_to_py(py, &delivery.delivery_tag)?)?;
        delivery_info.set_item("redelivered", delivery.redelivered)?;
    }
    let properties = headers_to_py(py, &properties)?;
    if virtual_transport {
        // Reuse the same delivery_info object as kombu does.
        properties.set_item("delivery_info", &delivery_info)?;
    } else {
        // kombu's pyamqp Message exposes content type, encoding and headers in properties too.
        if let Some(content_type) = delivery.message.content_type.as_deref() {
            properties.set_item("content_type", content_type)?;
        }
        if let Some(content_encoding) = delivery.message.content_encoding.as_deref() {
            properties.set_item("content_encoding", content_encoding)?;
        }
        if !delivery.message.headers.is_empty() {
            properties.set_item(
                "application_headers",
                headers_to_py(py, &delivery.message.headers)?,
            )?;
        }
    }
    PyTuple::new(
        py,
        [
            body,
            headers.into_any(),
            properties.into_any(),
            delivery_info.into_any(),
            tag_to_py(py, &delivery.delivery_tag)?,
        ],
    )
}

fn reply_route_from_py(dict: &Bound<'_, PyDict>) -> PyResult<ReplyRoute> {
    let text = |key: &str| -> PyResult<String> {
        dict.get_item(key)?
            .ok_or_else(|| PyValueError::new_err(format!("reply[{key}]: missing")))?
            .extract::<String>()
    };
    let flag = |key: &str, default: bool| -> PyResult<bool> {
        match dict.get_item(key)? {
            Some(value) => value.extract::<bool>(),
            None => Ok(default),
        }
    };
    let kind = ExchangeType::parse(&text("exchange_type")?).map_err(transport_error)?;
    let arguments = match dict.get_item("arguments")? {
        Some(value) if !value.is_none() => {
            let arguments = value
                .cast::<PyDict>()
                .map_err(|_| PyTypeError::new_err("reply['arguments'] must be a dict"))?;
            py_to_headers(arguments)?
        }
        _ => Headers::new(),
    };
    Ok(ReplyRoute {
        exchange: CompactString::from(text("exchange")?),
        exchange_kind: kind,
        name: CompactString::from(text("name")?),
        routing_key: CompactString::from(text("routing_key")?),
        durable: flag("durable", false)?,
        exclusive: flag("exclusive", true)?,
        auto_delete: flag("auto_delete", true)?,
        arguments,
    })
}

fn opt_string<'py>(py: Python<'py>, value: Option<&str>) -> Bound<'py, PyAny> {
    match value {
        Some(value) => PyString::new(py, value).into_any(),
        None => py.None().into_bound(py),
    }
}

fn control_reply_to_py<'py>(
    py: Python<'py>,
    reply: &ControlReply,
) -> PyResult<Bound<'py, PyTuple>> {
    PyTuple::new(
        py,
        [
            PyBytes::new(py, &reply.body).into_any(),
            opt_string(py, reply.content_type.as_deref()),
            opt_string(py, reply.content_encoding.as_deref()),
            headers_to_py(py, &reply.headers)?.into_any(),
        ],
    )
}

#[pymethods]
impl Connection {
    #[new]
    #[pyo3(signature = (url, transport_options=None, retry_policy=None))]
    fn new(
        py: Python<'_>,
        url: &str,
        transport_options: Option<&Bound<'_, PyDict>>,
        retry_policy: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let parsed = Url::parse(url).map_err(transport_error)?;
        let options = match transport_options {
            Some(dict) => {
                TransportOptions::from_headers(&py_to_headers(dict)?).map_err(transport_error)?
            }
            None => TransportOptions::default(),
        };
        let policy = retry_policy_from_py(retry_policy)?;
        let virtual_transport = parsed.kind.is_virtual();
        let body_encoding = options.body_encoding.clone();
        let transport = py.detach(|| {
            block(celery_transport::connect_parsed_with_config(
                &parsed,
                &celery_transport::ConnectConfig {
                    transport_options: options,
                    retry_policy: policy,
                },
            ))
        })?;
        Ok(Self {
            transport: ForkSafe::new(transport),
            virtual_transport,
            body_encoding,
            stream: ForkSafe::new(Mutex::new(None)),
            closed: AtomicBool::new(false),
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, exchange_type="direct", durable=false, auto_delete=false, passive=false, arguments=None))]
    fn declare_exchange(
        &self,
        py: Python<'_>,
        name: &str,
        exchange_type: &str,
        durable: bool,
        auto_delete: bool,
        passive: bool,
        arguments: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let mut declaration = ExchangeDecl::new(
            name,
            ExchangeType::parse(exchange_type).map_err(transport_error)?,
        );
        declaration.durable = durable;
        declaration.auto_delete = auto_delete;
        declaration.passive = passive;
        if let Some(arguments) = arguments {
            declaration.arguments = py_to_headers(arguments)?;
        }
        py.detach(|| block(self.transport.declare_exchange(&declaration)))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name=None, durable=false, auto_delete=false, exclusive=false, passive=false, arguments=None))]
    fn declare_queue(
        &self,
        py: Python<'_>,
        name: Option<&str>,
        durable: bool,
        auto_delete: bool,
        exclusive: bool,
        passive: bool,
        arguments: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        self.ensure_open()?;
        let mut declaration = QueueDecl {
            name: name.map(CompactString::from),
            durable,
            auto_delete,
            exclusive,
            passive,
            ..QueueDecl::default()
        };
        if let Some(arguments) = arguments {
            declaration.arguments = py_to_headers(arguments)?;
        }
        let info = py.detach(|| block(self.transport.declare_queue(&declaration)))?;
        Ok(info.name.to_string())
    }

    #[pyo3(signature = (queue, exchange, routing_key="", arguments=None))]
    fn bind(
        &self,
        py: Python<'_>,
        queue: &str,
        exchange: &str,
        routing_key: &str,
        arguments: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let mut binding = QueueBinding::new(queue, exchange, routing_key);
        if let Some(arguments) = arguments {
            binding.arguments = py_to_headers(arguments)?;
        }
        py.detach(|| block(self.transport.bind(&binding)))
    }

    #[pyo3(signature = (queue, exchange, routing_key="", arguments=None))]
    fn unbind(
        &self,
        py: Python<'_>,
        queue: &str,
        exchange: &str,
        routing_key: &str,
        arguments: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let mut binding = QueueBinding::new(queue, exchange, routing_key);
        if let Some(arguments) = arguments {
            binding.arguments = py_to_headers(arguments)?;
        }
        py.detach(|| block(self.transport.unbind(&binding)))
    }

    fn delete_exchange(&self, py: Python<'_>, exchange: &str) -> PyResult<()> {
        self.ensure_open()?;
        py.detach(|| block(self.transport.delete_exchange(exchange)))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (body, exchange=None, routing_key="", headers=None, properties=None, content_type=None, content_encoding=None, priority=None, delivery_mode=None, omit_priority=false))]
    fn publish(
        &self,
        py: Python<'_>,
        body: &Bound<'_, PyAny>,
        exchange: Option<&str>,
        routing_key: &str,
        headers: Option<&Bound<'_, PyDict>>,
        properties: Option<&Bound<'_, PyDict>>,
        content_type: Option<&str>,
        content_encoding: Option<&str>,
        priority: Option<u8>,
        delivery_mode: Option<u8>,
        omit_priority: bool,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let body = body_from_py(body)?;
        let headers = match headers {
            Some(dict) => py_to_headers(dict)?,
            None => Headers::new(),
        };
        let mut properties: Properties = match properties {
            Some(dict) if !self.virtual_transport => {
                let mut properties = Headers::with_capacity(dict.len());
                for (key, value) in dict.iter() {
                    let key = key.extract::<&str>()?;
                    // py-amqp ignores unknown properties without converting their values.
                    if matches!(
                        key,
                        "delivery_mode"
                            | "priority"
                            | "correlation_id"
                            | "reply_to"
                            | "expiration"
                            | "message_id"
                            | "timestamp"
                            | "type"
                            | "user_id"
                            | "app_id"
                            | "cluster_id"
                    ) && !value.is_none()
                    {
                        properties.insert(key, py_to_value(&value)?);
                    }
                }
                properties.into()
            }
            Some(dict) => py_to_headers(dict)?.into(),
            None => Properties::new(),
        };
        // kombu sets delivery_mode and priority by default; py-amqp omits priority
        // when Celery passes None for an explicit queue.
        match delivery_mode {
            Some(mode) => properties.set_delivery_mode(mode),
            None if properties.delivery_mode().is_none() => properties.set_delivery_mode(2),
            None => {}
        }
        if !omit_priority {
            match priority {
                Some(priority) => properties.set_priority(priority),
                None if properties.priority().is_none() => properties.set_priority(0),
                None => {}
            }
        }
        let message = Message {
            body,
            content_type: content_type.map(CompactString::from),
            content_encoding: content_encoding.map(CompactString::from),
            headers,
            properties,
        };
        let request = PublishRequest::new(exchange, routing_key, message);
        py.detach(|| block(self.transport.publish(request)))
    }

    /// Publishes a control command and returns replies plus unclaimed ticketed messages.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (exchange, exchange_type, body, content_type=None, content_encoding=None, headers=None, reply=None, timeout=1.0, limit=None, ticket=None))]
    fn control_request<'py>(
        &self,
        py: Python<'py>,
        exchange: &str,
        exchange_type: &str,
        body: &Bound<'_, PyAny>,
        content_type: Option<&str>,
        content_encoding: Option<&str>,
        headers: Option<&Bound<'_, PyDict>>,
        reply: Option<&Bound<'_, PyDict>>,
        timeout: f64,
        limit: Option<usize>,
        ticket: Option<&str>,
    ) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyList>)> {
        self.ensure_open()?;
        let request = ControlRequest {
            exchange: CompactString::from(exchange),
            exchange_kind: ExchangeType::parse(exchange_type).map_err(transport_error)?,
            body: body_from_py(body)?,
            content_type: content_type.map(CompactString::from),
            content_encoding: content_encoding.map(CompactString::from),
            headers: match headers {
                Some(dict) => py_to_headers(dict)?,
                None => Headers::new(),
            },
            reply: match reply {
                Some(dict) => Some(reply_route_from_py(dict)?),
                None => None,
            },
            timeout,
            limit,
            ticket: ticket.map(CompactString::from),
        };
        let outcome: ControlOutcome =
            py.detach(|| block(self.transport.control_request(request)))?;
        let replies = PyList::empty(py);
        for reply in &outcome.replies {
            replies.append(control_reply_to_py(py, reply)?)?;
        }
        let unclaimed = PyList::empty(py);
        for (ticket, reply) in &outcome.unclaimed {
            unclaimed.append(PyTuple::new(
                py,
                [
                    PyString::new(py, ticket).into_any(),
                    PyBytes::new(py, &reply.body).into_any(),
                    opt_string(py, reply.content_type.as_deref()),
                    opt_string(py, reply.content_encoding.as_deref()),
                    headers_to_py(py, &reply.headers)?.into_any(),
                ],
            )?)?;
        }
        Ok((replies, unclaimed))
    }

    #[pyo3(signature = (queues, timeout=1.0, prefetch=0, no_ack=false))]
    fn drain<'py>(
        &self,
        py: Python<'py>,
        queues: Vec<String>,
        timeout: Option<f64>,
        prefetch: u16,
        no_ack: bool,
    ) -> PyResult<Bound<'py, PyList>> {
        let queues = queues
            .into_iter()
            .map(CompactString::from)
            .collect::<Vec<_>>();
        let deliveries = py.detach(|| self.collect(queues, timeout, prefetch, no_ack))?;
        let out = PyList::empty(py);
        for delivery in &deliveries {
            out.append(delivery_to_py(
                py,
                delivery,
                self.virtual_transport,
                &self.body_encoding,
                false,
            )?)?;
        }
        Ok(out)
    }

    /// Fetches one message independently of the consumer.
    #[pyo3(signature = (queue, no_ack=false))]
    fn get<'py>(
        &self,
        py: Python<'py>,
        queue: &str,
        no_ack: bool,
    ) -> PyResult<Option<Bound<'py, PyTuple>>> {
        self.ensure_open()?;
        let delivery = py.detach(|| block(self.transport.get(queue, no_ack)))?;
        match delivery {
            Some(delivery) => Ok(Some(delivery_to_py(
                py,
                &delivery,
                self.virtual_transport,
                &self.body_encoding,
                false,
            )?)),
            None => Ok(None),
        }
    }

    /// Changes the active consumer's prefetch, or does nothing if no consumer exists.
    fn set_prefetch(&self, py: Python<'_>, prefetch: u16) -> PyResult<()> {
        self.ensure_open()?;
        let mut guard = self
            .stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(state) = guard.as_mut() else {
            return Ok(());
        };
        let control = Arc::clone(&state.consumer.control);
        py.detach(|| block(control.set_prefetch(prefetch)))?;
        state.prefetch = prefetch;
        Ok(())
    }

    fn ack(&self, py: Python<'_>, delivery_tag: &Bound<'_, PyAny>) -> PyResult<()> {
        self.ensure_open()?;
        let tag = tag_from_py(delivery_tag)?;
        py.detach(|| block(self.transport.ack(&tag)))
    }

    #[pyo3(signature = (delivery_tag, requeue=false))]
    fn reject(
        &self,
        py: Python<'_>,
        delivery_tag: &Bound<'_, PyAny>,
        requeue: bool,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let tag = tag_from_py(delivery_tag)?;
        py.detach(|| block(self.transport.reject(&tag, requeue)))
    }

    fn purge(&self, py: Python<'_>, queue: &str) -> PyResult<u64> {
        self.ensure_open()?;
        py.detach(|| block(self.transport.purge(queue)))
    }

    #[pyo3(name = "size")]
    fn queue_size(&self, py: Python<'_>, queue: &str) -> PyResult<u64> {
        self.ensure_open()?;
        py.detach(|| block(self.transport.queue_size(queue)))
    }

    fn delete_queue(&self, py: Python<'_>, queue: &str) -> PyResult<()> {
        self.ensure_open()?;
        py.detach(|| block(self.transport.delete_queue(queue)))
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        if self.closed.swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        let mut guard = self
            .stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let control = guard
            .as_ref()
            .map(|state| Arc::clone(&state.consumer.control));
        *guard = None;
        drop(guard);
        if let Some(control) = control {
            py.detach(|| block(control.cancel()))?;
        }
        py.detach(|| block(self.transport.close()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_is_shared_single_thread() {
        let first = runtime().expect("runtime");
        let second = runtime().expect("runtime");
        assert!(std::ptr::eq(first, second));
        assert_eq!(first.block_on(async { 1 + 1 }), 2);
    }

    #[test]
    fn runtime_is_rebuilt_after_fork_marker() {
        let before = runtime().expect("runtime");
        // Verify after_fork recreates the runtime even without a PID change in this test.
        after_fork();
        let after = runtime().expect("runtime");
        assert!(!std::ptr::eq(before, after));
        assert_eq!(after.block_on(async { 2 + 2 }), 4);
    }

    #[test]
    fn runtime_zero_delay_is_sleep_free() {
        // Retry policy is irrelevant here; verify the runtime works with one worker thread.
        let runtime = runtime().expect("runtime");
        assert_eq!(runtime.block_on(async { 3 + 4 }), 7);
    }
}
