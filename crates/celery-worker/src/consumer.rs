//! Worker consumer over celery_transport. A tokio task fills a bounded queue and
//! wakes the master; bodies remain Bytes until converted once at the Python boundary.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use celery_transport::{
    BoxFuture, ConsumerControl, Delivery, DeliveryStream, DeliveryTag, Error, ExchangeDecl,
    QueueBinding, QueueDecl, QueueInfo, Result, Transport,
};
use compact_str::CompactString;
use tokio::sync::Notify;

use crate::Waker;

// Bounds queued messages even when prefetch is unlimited and the master falls behind.
const UNLIMITED_CAPACITY: usize = 1024;

/// Delivery waiting for the master.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConsumerStats {
    pub received: u64,
    pub acked: u64,
    pub rejected: u64,
    pub requeued: u64,
}

#[derive(Default)]
struct State {
    queues: Vec<CompactString>,
    prefetch: u16,
    capacity: usize,
    deliveries: VecDeque<Delivery>,
    control: Option<Arc<dyn ConsumerControl>>,
    // Unread connection or startup error; the stopped stream requires master reconnection.
    error: Option<String>,
    started: bool,
    stats: ConsumerStats,
}

impl State {
    fn new(prefetch: u16) -> Self {
        Self {
            prefetch,
            capacity: capacity_for(prefetch),
            ..Self::default()
        }
    }

    fn push_error(&mut self, error: &Error) {
        // The broker will redeliver unread messages because they were never acknowledged.
        self.deliveries.clear();
        self.error = Some(error.to_string());
    }
}

fn capacity_for(prefetch: u16) -> usize {
    if prefetch == 0 {
        UNLIMITED_CAPACITY
    } else {
        usize::from(prefetch).max(1)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

struct Shared {
    transport: Arc<dyn Transport>,
    waker: Waker,
    state: Mutex<State>,

    closed: AtomicBool,
    // Distinguishes normal Tasks.stop cancellation from connection loss.
    cancelled: AtomicBool,
    // Acknowledge no_ack fallback deliveries in the reader task; pidbox and gossip
    // receive no acknowledgments from the master.
    no_ack: AtomicBool,
    // Only the main consumer owns the connection; pidbox and gossip share it.
    owns_transport: AtomicBool,

    space: Notify,

    stop: Notify,
}

impl Shared {
    fn push(&self, delivery: Delivery) {
        let mut state = lock(&self.state);
        state.stats.received += 1;
        state.deliveries.push_back(delivery);
        drop(state);
        // Always notify: if the master is already awake, the pending flag prevents its next wait.
        self.waker.notify();
    }

    fn fail(&self, error: &Error) {
        lock(&self.state).push_error(error);
        self.waker.notify();
    }

    fn has_space(&self) -> bool {
        let state = lock(&self.state);
        state.deliveries.len() < state.capacity
    }
}

/// Worker consumer with dynamic queues, prefetch and tagged acknowledgments.
/// The connection is already open; close stops consumption and closes it.
pub struct WorkerConsumer {
    shared: Arc<Shared>,
}

impl WorkerConsumer {
    /// Creates a consumer on an open connection.
    pub fn new(transport: Box<dyn Transport>, waker: Waker, prefetch: u16) -> Self {
        Self {
            shared: Arc::new(Shared {
                transport: Arc::<dyn Transport>::from(transport),
                waker,
                state: Mutex::new(State::new(prefetch)),
                closed: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
                no_ack: AtomicBool::new(false),
                owns_transport: AtomicBool::new(true),
                space: Notify::new(),
                stop: Notify::new(),
            }),
        }
    }

    /// Returns the transport shared by declarations and acknowledgments.
    pub fn transport(&self) -> &Arc<dyn Transport> {
        &self.shared.transport
    }

    /// Creates an auxiliary consumer sharing the task connection and runtime. Closing
    /// the auxiliary stops only its stream; the main consumer owns connection closure.
    pub fn aux(&self, prefetch: u16) -> Self {
        Self {
            shared: Arc::new(Shared {
                transport: Arc::clone(&self.shared.transport),
                waker: self.shared.waker.clone(),
                state: Mutex::new(State::new(prefetch)),
                closed: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
                no_ack: AtomicBool::new(false),
                owns_transport: AtomicBool::new(false),
                space: Notify::new(),
                stop: Notify::new(),
            }),
        }
    }

    /// Starts consuming the supplied or previously configured queues inside a tokio runtime.
    pub async fn start(&self, queues: Option<Vec<CompactString>>) -> Result<()> {
        self.start_inner(queues, false).await
    }

    /// Starts no_ack consumption, settling fallback acknowledgments in the reader task.
    pub async fn start_no_ack(&self, queues: Option<Vec<CompactString>>) -> Result<()> {
        self.start_inner(queues, true).await
    }

    async fn start_inner(&self, queues: Option<Vec<CompactString>>, no_ack: bool) -> Result<()> {
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let (queues, prefetch) = {
            let mut state = lock(&self.shared.state);
            if let Some(queues) = queues {
                state.queues = queues;
            }
            if state.queues.is_empty() {
                return Err(Error::InvalidArgument(
                    "consume requires at least one queue".to_string(),
                ));
            }
            (state.queues.clone(), state.prefetch)
        };
        self.shared.no_ack.store(no_ack, Ordering::Release);
        let consumer = if no_ack {
            self.shared
                .transport
                .consume_no_ack(&queues, prefetch)
                .await?
        } else {
            self.shared.transport.consume(&queues, prefetch).await?
        };
        let control = Arc::clone(&consumer.control);
        {
            let mut state = lock(&self.shared.state);
            state.control = Some(control);
            state.started = true;
        }
        let shared = Arc::clone(&self.shared);
        tokio::spawn(async move {
            run_stream(shared, consumer.deliveries).await;
        });
        Ok(())
    }

    /// Returns up to max ready deliveries without blocking.
    pub fn drain(&self, max: usize) -> Vec<Delivery> {
        if max == 0 {
            return Vec::new();
        }
        let mut state = lock(&self.shared.state);
        let was_full = state.deliveries.len() >= state.capacity;
        let count = max.min(state.deliveries.len());
        let deliveries = state.deliveries.drain(..count).collect();
        drop(state);
        if was_full {
            self.shared.space.notify_one();
        }
        deliveries
    }

    /// Returns the number of ready deliveries.
    pub fn pending(&self) -> usize {
        lock(&self.shared.state).deliveries.len()
    }

    /// Returns and clears a pending connection error.
    pub fn take_error(&self) -> Option<String> {
        lock(&self.shared.state).error.take()
    }

    /// Returns statistics for tests and inspect stats.
    pub fn stats(&self) -> ConsumerStats {
        lock(&self.shared.state).stats
    }

    pub fn ack<'a>(&'a self, tag: &'a DeliveryTag) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.transport.ack(tag).await?;
            lock(&self.shared.state).stats.acked += 1;
            Ok(())
        })
    }

    pub fn reject<'a>(&'a self, tag: &'a DeliveryTag, requeue: bool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.transport.reject(tag, requeue).await?;
            let mut state = lock(&self.shared.state);
            if requeue {
                state.stats.requeued += 1;
            } else {
                state.stats.rejected += 1;
            }
            Ok(())
        })
    }

    /// Changes prefetch and queue capacity at runtime.
    pub async fn set_prefetch(&self, prefetch: u16) -> Result<()> {
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let control = {
            let mut state = lock(&self.shared.state);
            state.prefetch = prefetch;
            state.capacity = capacity_for(prefetch);
            state.control.clone()
        };
        if let Some(control) = control {
            control.set_prefetch(prefetch).await?;
        }
        // A larger capacity may let the reader resume.
        self.shared.space.notify_one();
        Ok(())
    }

    pub async fn add_queue(&self, queue: &str) -> Result<()> {
        let queue = CompactString::from(queue);
        let control = {
            let mut state = lock(&self.shared.state);
            if !state.queues.contains(&queue) {
                state.queues.push(queue.clone());
            }
            state.control.clone()
        };
        if let Some(control) = control {
            control.add_queue(&queue).await?;
        }
        Ok(())
    }

    pub async fn cancel_queue(&self, queue: &str) -> Result<()> {
        let queue = CompactString::from(queue);
        let control = {
            let mut state = lock(&self.shared.state);
            state.queues.retain(|known| known != queue);
            state.control.clone()
        };
        if let Some(control) = control {
            control.cancel_queue(&queue).await?;
        }
        Ok(())
    }

    /// Cancels consumption while keeping the connection and pending tags usable. Retains
    /// the AMQP control handle until close so finishing tasks can acknowledge deliveries.
    pub async fn cancel(&self) -> Result<()> {
        self.shared.cancelled.store(true, Ordering::Release);
        let control = lock(&self.shared.state).control.clone();
        self.shared.stop.notify_one();
        if let Some(control) = control {
            control.cancel().await?;
        }
        self.shared.waker.notify();
        Ok(())
    }

    /// Stops consumption and closes the connection idempotently.
    pub async fn close(&self) -> Result<()> {
        if self.shared.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.shared.cancelled.store(true, Ordering::Release);
        let control = lock(&self.shared.state).control.take();
        self.shared.stop.notify_one();
        self.shared.space.notify_one();
        let mut result = Ok(());
        if let Some(control) = control {
            result = control.cancel().await;
        }
        self.shared.waker.notify();
        if self.shared.owns_transport.load(Ordering::Acquire)
            && let Err(error) = self.shared.transport.close().await
            && result.is_ok()
        {
            result = Err(error);
        }
        result
    }

    /// Returns whether the consumer has been closed.
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::Acquire)
    }

    pub fn declare_exchange<'a>(
        &'a self,
        declaration: &'a ExchangeDecl,
    ) -> BoxFuture<'a, Result<()>> {
        self.shared.transport.declare_exchange(declaration)
    }

    pub fn declare_queue<'a>(
        &'a self,
        declaration: &'a QueueDecl,
    ) -> BoxFuture<'a, Result<QueueInfo>> {
        self.shared.transport.declare_queue(declaration)
    }

    pub fn bind<'a>(&'a self, binding: &'a QueueBinding) -> BoxFuture<'a, Result<()>> {
        self.shared.transport.bind(binding)
    }
}

async fn next(stream: &mut DeliveryStream) -> Option<Result<Delivery>> {
    std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
}

async fn run_stream(shared: Arc<Shared>, mut stream: DeliveryStream) {
    loop {
        if shared.closed.load(Ordering::Acquire) {
            return;
        }
        if !shared.has_space() {
            tokio::select! {
                _ = shared.stop.notified() => return,
                _ = shared.space.notified() => continue,
            }
        }
        tokio::select! {
            biased;
            _ = shared.stop.notified() => return,
            item = next(&mut stream) => match item {
                Some(Ok(delivery)) => {
                    if shared.no_ack.load(Ordering::Acquire) {
                        // AMQP no_ack needs no settlement; fallback transports settle pending deliveries here.
                        let _ = shared.transport.ack(&delivery.delivery_tag).await;
                    }
                    shared.push(delivery);
                }
                Some(Err(error)) => {
                    shared.fail(&error);
                    return;
                }
                None => {
                    // An unexpected stream end indicates connection loss unless cancellation was requested.
                    if !shared.cancelled.load(Ordering::Acquire)
                        && !shared.closed.load(Ordering::Acquire)
                    {
                        shared.fail(&Error::ConnectionLost(
                            "delivery stream ended".to_string(),
                        ));
                    }
                    return;
                }
            },
        }
    }
}

pub async fn wait_for(mut condition: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    condition()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Waker;
    use bytes::Bytes;
    use celery_transport::{
        PublishRequest, Transport, connect, message::Message as TransportMessage,
    };

    async fn origin() -> Box<dyn Transport> {
        connect("memory:///worker-consumer-tests")
            .await
            .expect("memory")
    }

    async fn publish(transport: &dyn Transport, queue: &str, body: &str) {
        transport
            .publish(PublishRequest::new(
                None::<&str>,
                queue,
                TransportMessage::new(Bytes::copy_from_slice(body.as_bytes())),
            ))
            .await
            .expect("publish");
    }

    async fn setup(queues: &[&str]) {
        let transport = origin().await;
        for queue in queues {
            transport
                .declare_queue(&QueueDecl::new(*queue))
                .await
                .expect("declare");
        }
    }

    #[tokio::test]
    async fn delivers_and_acks() {
        setup(&["worker-consumer-q1"]).await;
        let producer = origin().await;
        publish(producer.as_ref(), "worker-consumer-q1", "one").await;

        let waker = Waker::new();
        let consumer = WorkerConsumer::new(origin().await, waker.clone(), 10);
        consumer
            .start(Some(vec![CompactString::from("worker-consumer-q1")]))
            .await
            .expect("start");

        assert!(wait_for(|| consumer.pending() == 1, Duration::from_secs(5)).await);
        let deliveries = consumer.drain(10);
        assert_eq!(deliveries.len(), 1);
        assert_eq!(&deliveries[0].message.body[..], b"one");
        consumer
            .ack(&deliveries[0].delivery_tag)
            .await
            .expect("ack");
        assert_eq!(consumer.stats().acked, 1);
        assert_eq!(consumer.stats().received, 1);
        consumer.close().await.expect("close");
        assert!(consumer.is_closed());
    }

    #[tokio::test]
    async fn prefetch_bounds_the_queue_and_resumes_after_drain() {
        setup(&["worker-consumer-q2"]).await;
        let producer = origin().await;
        for index in 0..3 {
            publish(
                producer.as_ref(),
                "worker-consumer-q2",
                &format!("m{index}"),
            )
            .await;
        }
        let consumer = WorkerConsumer::new(origin().await, Waker::new(), 1);
        consumer
            .start(Some(vec![CompactString::from("worker-consumer-q2")]))
            .await
            .expect("start");
        assert!(wait_for(|| consumer.pending() == 1, Duration::from_secs(5)).await);
        // Prefetch one must hold the second delivery until the first is acknowledged.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(consumer.pending(), 1);
        let first = consumer.drain(1);
        consumer.ack(&first[0].delivery_tag).await.expect("ack");
        assert!(wait_for(|| consumer.pending() == 1, Duration::from_secs(5)).await);
        consumer.close().await.expect("close");
    }

    #[tokio::test]
    async fn add_and_cancel_queue_at_runtime() {
        setup(&["worker-consumer-q3a", "worker-consumer-q3b"]).await;
        let producer = origin().await;
        publish(producer.as_ref(), "worker-consumer-q3a", "a").await;

        let consumer = WorkerConsumer::new(origin().await, Waker::new(), 5);
        consumer
            .start(Some(vec![CompactString::from("worker-consumer-q3a")]))
            .await
            .expect("start");
        assert!(wait_for(|| consumer.pending() == 1, Duration::from_secs(5)).await);
        consumer.drain(1);

        consumer
            .add_queue("worker-consumer-q3b")
            .await
            .expect("add");
        publish(producer.as_ref(), "worker-consumer-q3b", "b").await;
        assert!(wait_for(|| consumer.pending() == 1, Duration::from_secs(5)).await);
        let delivery = consumer.drain(1);
        assert_eq!(&delivery[0].message.body[..], b"b");

        consumer
            .cancel_queue("worker-consumer-q3b")
            .await
            .expect("cancel");
        publish(producer.as_ref(), "worker-consumer-q3b", "c").await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(consumer.pending(), 0);
        consumer.close().await.expect("close");
    }

    #[tokio::test]
    async fn set_prefetch_updates_the_transport_window() {
        setup(&["worker-consumer-q4"]).await;
        let producer = origin().await;
        for index in 0..3 {
            publish(
                producer.as_ref(),
                "worker-consumer-q4",
                &format!("p{index}"),
            )
            .await;
        }
        let consumer = WorkerConsumer::new(origin().await, Waker::new(), 1);
        consumer
            .start(Some(vec![CompactString::from("worker-consumer-q4")]))
            .await
            .expect("start");
        assert!(wait_for(|| consumer.pending() == 1, Duration::from_secs(5)).await);
        consumer.set_prefetch(3).await.expect("prefetch");
        assert!(wait_for(|| consumer.pending() == 3, Duration::from_secs(5)).await);
        consumer.close().await.expect("close");
    }

    #[tokio::test]
    async fn reject_requeue_returns_the_message() {
        setup(&["worker-consumer-q5"]).await;
        let producer = origin().await;
        publish(producer.as_ref(), "worker-consumer-q5", "again").await;
        let consumer = WorkerConsumer::new(origin().await, Waker::new(), 5);
        consumer
            .start(Some(vec![CompactString::from("worker-consumer-q5")]))
            .await
            .expect("start");
        assert!(wait_for(|| consumer.pending() == 1, Duration::from_secs(5)).await);
        let delivery = consumer.drain(1);
        consumer
            .reject(&delivery[0].delivery_tag, true)
            .await
            .expect("reject");
        assert_eq!(consumer.stats().requeued, 1);
        assert!(wait_for(|| consumer.pending() == 1, Duration::from_secs(5)).await);
        consumer.close().await.expect("close");
    }

    #[tokio::test]
    async fn closing_the_transport_reports_connection_lost_once() {
        setup(&["worker-consumer-q6"]).await;
        let transport = origin().await;
        let consumer = WorkerConsumer::new(transport, Waker::new(), 5);
        consumer
            .start(Some(vec![CompactString::from("worker-consumer-q6")]))
            .await
            .expect("start");
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Transport closure must surface exactly one ConnectionLost.
        consumer.shared.transport.close().await.expect("close");
        assert!(
            wait_for(|| consumer.take_error().is_some(), Duration::from_secs(5)).await,
            "connection error not reported"
        );
        assert!(consumer.take_error().is_none());
        consumer.close().await.expect("close");
    }

    #[tokio::test]
    async fn close_is_idempotent_and_start_fails_after_close() {
        setup(&["worker-consumer-q7"]).await;
        let consumer = WorkerConsumer::new(origin().await, Waker::new(), 5);
        consumer
            .start(Some(vec![CompactString::from("worker-consumer-q7")]))
            .await
            .expect("start");
        consumer.close().await.expect("close");
        consumer.close().await.expect("close");
        assert!(matches!(
            consumer
                .start(Some(vec![CompactString::from("worker-consumer-q7")]))
                .await,
            Err(Error::Closed)
        ));
    }

    #[test]
    fn capacity_follows_prefetch() {
        assert_eq!(capacity_for(0), UNLIMITED_CAPACITY);
        assert_eq!(capacity_for(1), 1);
        assert_eq!(capacity_for(10), 10);
    }
}
