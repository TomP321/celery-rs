//! Binding del worker master: [`Waker`] condiviso dal loop e
//! [`WorkerConsumer`] del broker.

use std::time::Duration;

use compact_str::CompactString;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use celery_transport::{
    ExchangeDecl, ExchangeType, QueueBinding, QueueDecl, TransportOptions, Url,
};
use celery_worker::{ConsumerStats, Waker as RustWaker, WorkerConsumer};

use crate::transport::{
    block, delivery_to_py, py_to_headers, retry_policy_from_py, tag_from_py, transport_error,
};

/// `_celery_rs.Waker`: il loop Python dorme in `wait` finché un componente
/// Rust (consumer, pool) non chiama `notify`.
#[pyclass(name = "Waker", module = "_celery_rs", frozen)]
pub struct PyWaker {
    pub inner: RustWaker,
}

#[pymethods]
impl PyWaker {
    #[new]
    fn new() -> Self {
        Self {
            inner: RustWaker::new(),
        }
    }

    fn notify(&self) {
        self.inner.notify();
    }

    /// `wait(timeout=None) -> bool`; il GIL è rilasciato durante l'attesa.
    #[pyo3(signature = (timeout=None))]
    fn wait(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<bool> {
        let timeout = timeout_from_py(timeout)?;
        let waker = self.inner.clone();
        Ok(py.detach(move || waker.wait(timeout)))
    }
}

fn timeout_from_py(timeout: Option<f64>) -> PyResult<Option<Duration>> {
    match timeout {
        None => Ok(None),
        Some(t) if t.is_finite() && t >= 0.0 => Ok(Some(Duration::from_secs_f64(t))),
        Some(_) => Err(PyValueError::new_err("timeout deve essere >= 0")),
    }
}

/// `_celery_rs.WorkerConsumer`: consumer del worker sopra il trasporto Rust.
///
/// Il GIL è rilasciato per tutto l'I/O; `drain` non blocca mai. `waker` è il
/// [`PyWaker`] su cui dorme il loop Python: viene svegliato a ogni consegna,
/// alla connessione persa e alla chiusura.
#[pyclass(name = "WorkerConsumer", module = "_celery_rs")]
pub struct PyWorkerConsumer {
    inner: WorkerConsumer,
    virtual_transport: bool,
    body_encoding: CompactString,
}

fn options_from_py(
    dict: Option<&Bound<'_, PyDict>>,
) -> PyResult<(TransportOptions, CompactString)> {
    let options = match dict {
        Some(dict) => TransportOptions::from_headers(&py_to_headers(dict)?)
            .map_err(transport_error)?,
        None => TransportOptions::default(),
    };
    let body_encoding = options.body_encoding.clone();
    Ok((options, body_encoding))
}

#[pymethods]
impl PyWorkerConsumer {
    #[new]
    #[pyo3(signature = (url, waker, transport_options=None, retry_policy=None))]
    fn new(
        py: Python<'_>,
        url: &str,
        waker: PyRef<'_, PyWaker>,
        transport_options: Option<&Bound<'_, PyDict>>,
        retry_policy: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let parsed = Url::parse(url).map_err(transport_error)?;
        let (options, body_encoding) = options_from_py(transport_options)?;
        let policy = retry_policy_from_py(retry_policy)?;
        let virtual_transport = parsed.kind.is_virtual();
        let waker = waker.inner.clone();
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
            inner: WorkerConsumer::new(transport, waker, 0),
            virtual_transport,
            body_encoding,
        })
    }

    /// Avvia il consumatore sulle code indicate (lista di nomi).
    fn consume(&self, py: Python<'_>, queues: Vec<String>) -> PyResult<()> {
        let queues = queues
            .into_iter()
            .map(CompactString::from)
            .collect::<Vec<_>>();
        py.detach(|| block(self.inner.start(Some(queues))))
    }

    fn add_queue(&self, py: Python<'_>, queue: &str) -> PyResult<()> {
        py.detach(|| block(self.inner.add_queue(queue)))
    }

    fn cancel_queue(&self, py: Python<'_>, queue: &str) -> PyResult<()> {
        py.detach(|| block(self.inner.cancel_queue(queue)))
    }

    fn set_prefetch(&self, py: Python<'_>, prefetch: u16) -> PyResult<()> {
        py.detach(|| block(self.inner.set_prefetch(prefetch)))
    }

    /// Consegne già pronte (nessuna attesa); i body diventano `bytes` qui,
    /// una sola volta.
    fn drain<'py>(&self, py: Python<'py>, max: usize) -> PyResult<Bound<'py, PyList>> {
        let deliveries = self.inner.drain(max);
        let out = PyList::empty(py);
        for delivery in &deliveries {
            out.append(delivery_to_py(
                py,
                delivery,
                self.virtual_transport,
                &self.body_encoding,
                true,
            )?)?;
        }
        Ok(out)
    }

    fn pending(&self) -> usize {
        self.inner.pending()
    }

    /// Errore di connessione pendente, consumato una sola volta.
    fn take_error(&self) -> Option<String> {
        self.inner.take_error()
    }

    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let ConsumerStats {
            received,
            acked,
            rejected,
            requeued,
        } = self.inner.stats();
        let dict = PyDict::new(py);
        dict.set_item("received", received)?;
        dict.set_item("acked", acked)?;
        dict.set_item("rejected", rejected)?;
        dict.set_item("requeued", requeued)?;
        Ok(dict)
    }

    fn ack(&self, py: Python<'_>, delivery_tag: &Bound<'_, PyAny>) -> PyResult<()> {
        let tag = tag_from_py(delivery_tag)?;
        py.detach(|| block(self.inner.ack(&tag)))
    }

    #[pyo3(signature = (delivery_tag, requeue=false))]
    fn reject(
        &self,
        py: Python<'_>,
        delivery_tag: &Bound<'_, PyAny>,
        requeue: bool,
    ) -> PyResult<()> {
        let tag = tag_from_py(delivery_tag)?;
        py.detach(|| block(self.inner.reject(&tag, requeue)))
    }

    /// Cancella il consumatore lasciando la connessione aperta (come
    /// `basic_cancel`: i tag già consegnati restano confermabili).
    fn cancel(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| block(self.inner.cancel()))
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| block(self.inner.close()))
    }

    fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    // ------------------------------------------------------------ dichiarazioni

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
        py.detach(|| block(self.inner.declare_exchange(&declaration)))
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
        let info = py.detach(|| block(self.inner.declare_queue(&declaration)))?;
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
        let mut binding = QueueBinding::new(queue, exchange, routing_key);
        if let Some(arguments) = arguments {
            binding.arguments = py_to_headers(arguments)?;
        }
        py.detach(|| block(self.inner.bind(&binding)))
    }
}
