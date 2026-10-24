//! Binding Python minimale dei trasporti: `_celery_rs.Connection(url, transport_options)`.
//!
//! Il GIL viene rilasciato durante tutto l'I/O: tutti i dati Python sono
//! convertiti in tipi Rust prima di entrare nel runtime tokio, e i risultati
//! sono riconvertiti solo alla fine.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use compact_str::CompactString;
use pyo3::exceptions::{
    PyConnectionError, PyLookupError, PyNotImplementedError, PyRuntimeError, PyTypeError,
    PyValueError,
};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple,
};

use celery_transport::{
    Consumer, Delivery, DeliveryStream, DeliveryTag, Error as TransportError, ExchangeDecl,
    ExchangeType, HeaderValue, Headers, Message, Properties, PublishRequest, QueueBinding,
    QueueDecl, Transport, TransportOptions, Url, connect_parsed,
};

/// Runtime tokio condiviso, single-thread (come richiesto dalla spec).
fn runtime() -> Result<&'static tokio::runtime::Runtime, String> {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    let _ = RUNTIME.set(runtime);
    RUNTIME
        .get()
        .ok_or_else(|| "runtime tokio non disponibile".to_string())
}

fn block<T: Send>(future: impl Future<Output = Result<T, TransportError>>) -> PyResult<T> {
    let runtime = runtime().map_err(PyRuntimeError::new_err)?;
    runtime.block_on(future).map_err(transport_error)
}

fn transport_error(error: TransportError) -> PyErr {
    match error {
        TransportError::NotImplemented(message) => PyNotImplementedError::new_err(message),
        TransportError::InvalidUrl(message)
        | TransportError::InvalidTransportOption(message)
        | TransportError::InvalidArgument(message)
        | TransportError::InvalidPattern(message)
        | TransportError::UnsupportedTransport(message)
        | TransportError::UnsupportedExchangeType(message) => PyValueError::new_err(message),
        TransportError::NotFound(message) => PyLookupError::new_err(message),
        TransportError::ConnectionLost(message) | TransportError::Connection(message) => {
            PyConnectionError::new_err(message)
        }
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

/// `dict` Python -> [`Headers`] ordinati.
fn py_to_headers(dict: &Bound<'_, PyDict>) -> PyResult<Headers> {
    let mut headers = Headers::with_capacity(dict.len());
    for (key, value) in dict.iter() {
        let key = key
            .cast::<PyString>()
            .map_err(|_| PyTypeError::new_err("le chiavi devono essere stringhe"))?
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
                PyValueError::new_err("stringa non rappresentabile in UTF-8 nei messaggi")
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
        "tipo non supportato nei messaggi: {}",
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
            .ok_or_else(|| PyValueError::new_err("Decimal troppo grande per AMQP"))?;
    }
    let mut scale: u64 = 0;
    if exponent < 0 {
        // `unsigned_abs` gestisce anche i32::MIN senza overflow.
        scale = i64::from(exponent).unsigned_abs();
    } else if exponent > 0 {
        // Esponente positivo (es. Decimal("1E+3")): scala 0 e fattore 10^exp.
        let factor = 10i128
            .checked_pow(exponent.unsigned_abs())
            .ok_or_else(|| PyValueError::new_err("Decimal troppo grande per AMQP"))?;
        mantissa = mantissa
            .checked_mul(factor)
            .ok_or_else(|| PyValueError::new_err("Decimal troppo grande per AMQP"))?;
    }
    if sign != 0 {
        mantissa = -mantissa;
    }
    let value = i32::try_from(mantissa)
        .map_err(|_| PyValueError::new_err("Decimal fuori intervallo per AMQP"))?;
    let scale =
        u8::try_from(scale).map_err(|_| PyValueError::new_err("scala del Decimal non valida"))?;
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

fn tag_from_py(tag: &Bound<'_, PyAny>) -> PyResult<DeliveryTag> {
    if tag.cast::<PyBool>().is_ok() {
        return Err(PyTypeError::new_err(
            "delivery_tag deve essere int o str, non bool",
        ));
    }
    if let Ok(number) = tag.cast::<PyInt>() {
        return Ok(DeliveryTag::Number(number.extract()?));
    }
    if let Ok(text) = tag.cast::<PyString>() {
        return Ok(DeliveryTag::from(text.to_str()?));
    }
    Err(PyTypeError::new_err("delivery_tag deve essere int o str"))
}

fn body_from_py(body: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(bytes) = body.cast::<PyBytes>() {
        return Ok(Bytes::copy_from_slice(bytes.as_bytes()));
    }
    if let Ok(bytes) = body.cast::<PyByteArray>() {
        return Ok(Bytes::from(bytes.to_vec()));
    }
    Err(PyTypeError::new_err("body deve essere bytes o bytearray"))
}

struct StreamState {
    queues: Vec<CompactString>,
    prefetch: u16,
    consumer: Consumer,
}

/// Limite di messaggi restituiti da una singola `drain`: evita di accumulare
/// indefinitamente se un produttore pubblica senza sosta.
const DRAIN_BATCH_LIMIT: usize = 1000;

/// Connessione a un broker: stessa API per tutti i trasporti di `connect`.
#[pyclass(module = "_celery_rs")]
pub struct Connection {
    transport: Box<dyn Transport>,
    /// Trasporto virtuale: aggiunge `delivery_info`/`body_encoding`/`delivery_tag`.
    virtual_transport: bool,
    body_encoding: CompactString,
    stream: Mutex<Option<StreamState>>,
    closed: AtomicBool,
}

impl Connection {
    fn ensure_open(&self) -> PyResult<()> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(PyConnectionError::new_err("connessione chiusa"));
        }
        Ok(())
    }

    /// Attende almeno un messaggio e raccoglie tutti quelli subito disponibili.
    ///
    /// Se il consumatore esiste già, prefetch e code vengono aggiornati a
    /// runtime (`ConsumerControl`) senza ricrearlo, preservando i messaggi in
    /// volo.
    fn collect(
        &self,
        queues: Vec<CompactString>,
        timeout: Option<f64>,
        prefetch: u16,
    ) -> PyResult<Vec<Delivery>> {
        self.ensure_open()?;
        if queues.is_empty() {
            return Ok(Vec::new());
        }
        let mut guard = self
            .stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.is_none() {
            let consumer = block(self.transport.consume(&queues, prefetch))?;
            *guard = Some(StreamState {
                queues: queues.clone(),
                prefetch,
                consumer,
            });
        }
        let state = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("stream non inizializzato"))?;

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
        runtime.block_on(async {
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
                // timeout <= 0: un solo poll, senza attendere.
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
        })
    }
}

async fn next(stream: &mut DeliveryStream) -> Option<Result<Delivery, TransportError>> {
    std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
}

/// Un solo poll, senza attesa: usato per svuotare i messaggi già pronti.
fn poll_now(stream: &mut DeliveryStream) -> Option<Result<Delivery, TransportError>> {
    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);
    match stream.as_mut().poll_next(&mut context) {
        Poll::Ready(item) => item,
        Poll::Pending => None,
    }
}

fn delivery_to_py<'py>(
    py: Python<'py>,
    delivery: &Delivery,
    virtual_transport: bool,
    body_encoding: &str,
) -> PyResult<Bound<'py, PyTuple>> {
    let body = PyBytes::new(py, &delivery.message.body).into_any();
    let headers = headers_to_py(py, &delivery.message.headers)?.into_any();
    let mut properties = delivery.message.properties.clone().into_headers();
    // delivery_info è sempre presente nel risultato; nei trasporti virtuali
    // kombu lo tiene anche dentro properties.
    let delivery_info = PyDict::new(py);
    delivery_info.set_item("exchange", delivery.exchange.as_str())?;
    delivery_info.set_item("routing_key", delivery.routing_key.as_str())?;
    if virtual_transport {
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
        // Come `amqp.Message.delivery_info` di kombu.
        delivery_info.set_item("delivery_tag", tag_to_py(py, &delivery.delivery_tag)?)?;
        delivery_info.set_item("redelivered", delivery.redelivered)?;
    }
    let properties = headers_to_py(py, &properties)?;
    if virtual_transport {
        // Stesso oggetto di `properties['delivery_info']`, come in kombu.
        properties.set_item("delivery_info", &delivery_info)?;
    } else {
        // `kombu.transport.pyamqp.Message` espone content-type, encoding e
        // header dentro `properties`, oltre che nei campi dedicati.
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

#[pymethods]
impl Connection {
    #[new]
    #[pyo3(signature = (url, transport_options=None))]
    fn new(
        py: Python<'_>,
        url: &str,
        transport_options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let parsed = Url::parse(url).map_err(transport_error)?;
        let options = match transport_options {
            Some(dict) => {
                TransportOptions::from_headers(&py_to_headers(dict)?).map_err(transport_error)?
            }
            None => TransportOptions::default(),
        };
        let virtual_transport = parsed.kind.is_virtual();
        let body_encoding = options.body_encoding.clone();
        let transport = py.detach(|| block(connect_parsed(&parsed, options)))?;
        Ok(Self {
            transport,
            virtual_transport,
            body_encoding,
            stream: Mutex::new(None),
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
    #[pyo3(signature = (body, exchange=None, routing_key="", headers=None, properties=None, content_type=None, content_encoding=None, priority=None, delivery_mode=None))]
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
    ) -> PyResult<()> {
        self.ensure_open()?;
        let body = body_from_py(body)?;
        let headers = match headers {
            Some(dict) => py_to_headers(dict)?,
            None => Headers::new(),
        };
        let mut properties: Properties = match properties {
            Some(dict) => py_to_headers(dict)?.into(),
            None => Properties::new(),
        };
        // Il Producer di kombu imposta sempre delivery_mode e priority.
        match delivery_mode {
            Some(mode) => properties.set_delivery_mode(mode),
            None if properties.delivery_mode().is_none() => properties.set_delivery_mode(2),
            None => {}
        }
        match priority {
            Some(priority) => properties.set_priority(priority),
            None if properties.priority().is_none() => properties.set_priority(0),
            None => {}
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

    #[pyo3(signature = (queues, timeout=1.0, prefetch=0))]
    fn drain<'py>(
        &self,
        py: Python<'py>,
        queues: Vec<String>,
        timeout: Option<f64>,
        prefetch: u16,
    ) -> PyResult<Bound<'py, PyList>> {
        let queues = queues
            .into_iter()
            .map(CompactString::from)
            .collect::<Vec<_>>();
        let deliveries = py.detach(|| self.collect(queues, timeout, prefetch))?;
        let out = PyList::empty(py);
        for delivery in &deliveries {
            out.append(delivery_to_py(
                py,
                delivery,
                self.virtual_transport,
                &self.body_encoding,
            )?)?;
        }
        Ok(out)
    }

    /// `basic_get`: un messaggio dalla coda, senza passare dal consumatore.
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
            )?)),
            None => Ok(None),
        }
    }

    /// Cambia il prefetch del consumatore attivo (no-op se non esiste).
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
}
