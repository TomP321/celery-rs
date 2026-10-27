//! Binding del worker master: [`Waker`] condiviso dal loop,
//! [`WorkerConsumer`] del broker e pool di processi.

use crate::fork::ForkSafe;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use compact_str::CompactString;
use pyo3::exceptions::{PyOSError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

use celery_transport::{
    ExchangeDecl, ExchangeType, Message, Properties, PublishRequest, QueueBinding, QueueDecl,
    TransportOptions, Url,
};
use celery_worker::pool::{self as pool_impl, Config, JobSpec, PoolError, Spawner};
use celery_worker::{ConsumerStats, Waker as RustWaker, WorkerConsumer};

use crate::transport::{
    block, body_from_py, delivery_to_py, py_to_headers, retry_policy_from_py, tag_from_py,
    transport_error,
};

/// `_celery_rs.Waker`: il loop Python dorme in `wait` finché un componente
/// Rust (consumer, pool) non chiama `notify`.
#[pyclass(name = "Waker", module = "_celery_rs", frozen)]
pub struct PyWaker {
    pub(crate) inner: ForkSafe<RustWaker>,
}

#[pymethods]
impl PyWaker {
    #[new]
    fn new() -> Self {
        Self {
            inner: ForkSafe::new(RustWaker::new()),
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

    /// fd leggibile quando c'è una notifica (`-1` se non disponibile).
    ///
    /// Il loop Python lo registra nel poller dell'hub (`add_reader`); la
    /// callback chiama :meth:`drain` per consumare la notifica.
    fn fileno(&self) -> i32 {
        self.inner.fileno()
    }

    /// Consuma la notifica pendente (svuota il fd e lo stato `pending`).
    fn drain(&self) {
        self.inner.drain();
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
    inner: ForkSafe<WorkerConsumer>,
    virtual_transport: bool,
    body_encoding: CompactString,
}

fn options_from_py(
    dict: Option<&Bound<'_, PyDict>>,
) -> PyResult<(TransportOptions, CompactString)> {
    let options = match dict {
        Some(dict) => {
            TransportOptions::from_headers(&py_to_headers(dict)?).map_err(transport_error)?
        }
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
            inner: ForkSafe::new(WorkerConsumer::new(transport, waker, 0)),
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

    /// Come :meth:`consume` ma con `basic.consume(no_ack=true)`.
    fn consume_no_ack(&self, py: Python<'_>, queues: Vec<String>) -> PyResult<()> {
        let queues = queues
            .into_iter()
            .map(CompactString::from)
            .collect::<Vec<_>>();
        py.detach(|| block(self.inner.start_no_ack(Some(queues))))
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

    /// Un consumer ausiliario (pidbox/gossip) che condivide questa connessione.
    fn aux_consumer(&self) -> PyWorkerConsumer {
        PyWorkerConsumer {
            inner: ForkSafe::new(self.inner.aux(0)),
            virtual_transport: self.virtual_transport,
            body_encoding: self.body_encoding.clone(),
        }
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

    /// Dichiara exchange fanout, coda del nodo e binding del pidbox.
    ///
    /// Nomi e argomenti TTL sono costruiti in Rust (`celery_worker::mailbox`),
    /// così restano identici a `kombu.pidbox.Mailbox.get_queue`.
    #[pyo3(signature = (hostname, namespace="celery", expires=None, message_ttl=None))]
    fn declare_pidbox(
        &self,
        py: Python<'_>,
        hostname: &str,
        namespace: &str,
        expires: Option<f64>,
        message_ttl: Option<f64>,
    ) -> PyResult<String> {
        let binding = celery_worker::mailbox::pidbox(hostname, namespace, expires, message_ttl);
        self.declare_mailbox(py, &binding)
    }

    /// Dichiara exchange eventi, coda del nodo e binding `worker.#`.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (prefix, node_id, exchange="celeryev", exchange_type="topic",
                        routing_key="worker.#", expires=None, message_ttl=None,
                        queue_exclusive=true, queue_durable=false))]
    fn declare_events(
        &self,
        py: Python<'_>,
        prefix: &str,
        node_id: &str,
        exchange: &str,
        exchange_type: &str,
        routing_key: &str,
        expires: Option<f64>,
        message_ttl: Option<f64>,
        queue_exclusive: bool,
        queue_durable: bool,
    ) -> PyResult<String> {
        let kind = ExchangeType::parse(exchange_type).map_err(transport_error)?;
        let binding = celery_worker::mailbox::events(
            prefix,
            node_id,
            exchange,
            kind,
            routing_key,
            expires,
            message_ttl,
            queue_exclusive,
            queue_durable,
        );
        self.declare_mailbox(py, &binding)
    }

    /// Pubblica un messaggio sulla connessione del consumer (risposte pidbox).
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (body, exchange=None, routing_key="", headers=None, content_type=None,
                        content_encoding=None, delivery_mode=1, priority=0))]
    fn publish(
        &self,
        py: Python<'_>,
        body: &Bound<'_, PyAny>,
        exchange: Option<&str>,
        routing_key: &str,
        headers: Option<&Bound<'_, PyDict>>,
        content_type: Option<&str>,
        content_encoding: Option<&str>,
        delivery_mode: u8,
        priority: u8,
    ) -> PyResult<()> {
        let body = body_from_py(body)?;
        let mut properties = Properties::new();
        properties.set_delivery_mode(delivery_mode);
        properties.set_priority(priority);
        let message = Message {
            body,
            content_type: content_type.map(CompactString::from),
            content_encoding: content_encoding.map(CompactString::from),
            headers: match headers {
                Some(dict) => py_to_headers(dict)?,
                None => celery_transport::Headers::new(),
            },
            properties,
        };
        let request = PublishRequest::new(exchange, routing_key, message);
        py.detach(|| block(self.inner.transport().publish(request)))
    }
}

impl PyWorkerConsumer {
    fn declare_mailbox(
        &self,
        py: Python<'_>,
        binding: &celery_worker::mailbox::MailboxBinding,
    ) -> PyResult<String> {
        py.detach(|| {
            block(async {
                self.inner.declare_exchange(&binding.exchange).await?;
                let info = self.inner.declare_queue(&binding.queue).await?;
                self.inner.bind(&binding.binding).await?;
                Ok(info.name.to_string())
            })
        })
    }
}

/// Un evento del pool, consumato dal loop del master.
#[pyclass(name = "PoolEvent", module = "_celery_rs", frozen)]
pub struct PyPoolEvent {
    #[pyo3(get)]
    kind: &'static str,
    #[pyo3(get)]
    job: u64,
    #[pyo3(get)]
    pid: i32,
    #[pyo3(get)]
    exitcode: i32,
    #[pyo3(get)]
    time_accepted: f64,
    #[pyo3(get)]
    success: bool,
    #[pyo3(get)]
    timeout: f64,
    #[pyo3(get)]
    payload: Option<Py<PyBytes>>,
    #[pyo3(get)]
    message: Option<String>,
}

fn event(py: Python<'_>, event: pool_impl::PoolEvent) -> PyPoolEvent {
    use pool_impl::PoolEvent as E;
    match event {
        E::Accepted {
            job,
            pid,
            time_accepted,
        } => PyPoolEvent {
            kind: "accepted",
            job,
            pid,
            exitcode: 0,
            time_accepted,
            success: true,
            timeout: 0.0,
            payload: None,
            message: None,
        },
        E::Ready {
            job,
            success,
            payload,
        } => PyPoolEvent {
            kind: "ready",
            job,
            pid: 0,
            exitcode: 0,
            time_accepted: 0.0,
            success,
            timeout: 0.0,
            payload: Some(PyBytes::new(py, &payload).unbind()),
            message: None,
        },
        E::WorkerLost { job, pid, exitcode } => PyPoolEvent {
            kind: "worker-lost",
            job,
            pid,
            exitcode,
            time_accepted: 0.0,
            success: false,
            timeout: 0.0,
            payload: None,
            message: None,
        },
        E::Terminated { job, pid, exitcode } => PyPoolEvent {
            kind: "terminated",
            job,
            pid,
            exitcode,
            time_accepted: 0.0,
            success: false,
            timeout: 0.0,
            payload: None,
            message: None,
        },
        E::SoftTimeout { job, timeout } => PyPoolEvent {
            kind: "soft-timeout",
            job,
            pid: 0,
            exitcode: 0,
            time_accepted: 0.0,
            success: false,
            timeout,
            payload: None,
            message: None,
        },
        E::HardTimeout { job, timeout } => PyPoolEvent {
            kind: "hard-timeout",
            job,
            pid: 0,
            exitcode: 0,
            time_accepted: 0.0,
            success: false,
            timeout,
            payload: None,
            message: None,
        },
        E::ZygoteLost => PyPoolEvent {
            kind: "zygote-lost",
            job: 0,
            pid: 0,
            exitcode: 0,
            time_accepted: 0.0,
            success: false,
            timeout: 0.0,
            payload: None,
            message: None,
        },
        E::PoolFailure { job, message } => PyPoolEvent {
            kind: "pool-failure",
            job,
            pid: 0,
            exitcode: 0,
            time_accepted: 0.0,
            success: false,
            timeout: 0.0,
            payload: None,
            message: Some(message),
        },
    }
}

/// `_celery_rs.Pool`: supervisore dei figli Python.
#[pyclass(name = "Pool", module = "_celery_rs")]
pub struct PyPool {
    inner: ForkSafe<Arc<pool_impl::Pool>>,
}

#[pymethods]
impl PyPool {
    #[new]
    #[pyo3(signature = (control_fd, zygote_pid, processes, maxtasksperchild=None,
                        max_memory_per_child=None, waker=None))]
    fn new(
        control_fd: i32,
        zygote_pid: i32,
        processes: usize,
        maxtasksperchild: Option<u32>,
        max_memory_per_child: Option<u32>,
        waker: Option<&PyWaker>,
    ) -> PyResult<Self> {
        let spawner = pool_impl::ZygoteSpawner::new(
            control_fd,
            zygote_pid,
            maxtasksperchild,
            max_memory_per_child,
        )
        .map_err(|error| PyOSError::new_err(error.to_string()))?;
        let waker = waker.map(|waker| waker.inner.clone()).unwrap_or_default();
        let pool = pool_impl::Pool::new(Config::new(processes), Arc::new(spawner), waker);
        Ok(Self {
            inner: ForkSafe::new(Arc::new(pool)),
        })
    }

    /// `submit(job, payload, soft=None, hard=None)`: accoda un job; il payload
    /// è il pickle di `(target, args, kwargs)`.
    #[pyo3(signature = (job, payload, soft=None, hard=None))]
    fn submit(
        &self,
        py: Python<'_>,
        job: u64,
        payload: &[u8],
        soft: Option<f64>,
        hard: Option<f64>,
    ) -> PyResult<()> {
        let payload = Bytes::copy_from_slice(payload);
        let soft = timeout(soft)?;
        let hard = timeout(hard)?;
        let pool = Arc::clone(&self.inner);
        py.detach(move || {
            pool.submit(JobSpec {
                id: job,
                payload,
                soft_timeout: soft,
                hard_timeout: hard,
            })
        })
        .map_err(pool_error)
    }

    /// Eventi pronti (non bloccante).
    fn drain_events(&self, py: Python<'_>) -> Vec<PyPoolEvent> {
        self.inner
            .drain_events()
            .into_iter()
            .map(|item| event(py, item))
            .collect()
    }

    fn grow(&self, n: usize) {
        self.inner.grow(n);
    }

    fn shrink(&self, py: Python<'_>, n: usize) -> PyResult<()> {
        let pool = Arc::clone(&self.inner);
        py.detach(move || pool.shrink(n)).map_err(pool_error)
    }

    fn terminate_job(&self, pid: i32, signal: i32) {
        self.inner.terminate_job(pid, signal);
    }

    fn restart(&self) {
        self.inner.restart();
    }

    /// Uccide i figli dopo la perdita dello zygote: i job in corso arrivano
    /// come ``worker-lost`` (WorkerLostError) e i messaggi vengono
    /// riconsegnati secondo ``acks_late``/``reject_on_worker_lost``.
    fn abandon_children(&self) {
        self.inner.abandon_children();
    }

    fn flush(&self, py: Python<'_>) -> Vec<u64> {
        let pool = Arc::clone(&self.inner);
        py.detach(move || pool.flush())
    }

    fn close(&self) {
        self.inner.close();
    }

    fn terminate(&self) {
        self.inner.terminate();
    }

    fn num_processes(&self) -> usize {
        self.inner.num_processes()
    }

    fn pids(&self) -> Vec<i32> {
        self.inner.pids()
    }

    fn alive(&self) -> usize {
        self.inner.alive()
    }

    fn stopped(&self) -> bool {
        self.inner.stopped()
    }

    fn did_start_ok(&self) -> bool {
        self.inner.did_start_ok()
    }

    fn zygote_pid(&self) -> i32 {
        self.inner.zygote_pid()
    }

    fn zygote_lost(&self) -> bool {
        self.inner.zygote_lost()
    }

    /// `(totale, conteggi per figlio)`: alimenta la chiave `writes` di
    /// `TaskPool.info`, come `human_write_stats` in prefork.
    fn write_stats(&self) -> (u64, Vec<u64>) {
        self.inner.write_stats()
    }
}

fn timeout(value: Option<f64>) -> PyResult<Option<Duration>> {
    match value {
        None => Ok(None),
        Some(value) if value <= 0.0 || !value.is_finite() => Ok(None),
        Some(value) => Ok(Some(Duration::from_secs_f64(value))),
    }
}

fn pool_error(error: PoolError) -> PyErr {
    match error {
        PoolError::ShrinkBusy => PyValueError::new_err(error.to_string()),
        PoolError::NotRunning => PyRuntimeError::new_err(error.to_string()),
        PoolError::Spawn(message) => PyRuntimeError::new_err(message),
    }
}

/// Evita il warning "trait inutilizzato" se `Spawner` serve solo ai test.
#[allow(dead_code)]
fn _assert_spawner(_: &dyn Spawner) {}
