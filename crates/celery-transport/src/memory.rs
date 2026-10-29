//! In-process FIFO transport with shared state per virtual host. Unacknowledged
//! messages are lost on close, but queues survive individual connection closures.

use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use compact_str::CompactString;
use futures_core::Stream;

use crate::message::{Delivery, DeliveryTag, Headers, Message};
use crate::routing::PreparedBinding;
use crate::transport::{
    BoxFuture, Consumer, ConsumerControl, DeliveryStream, Error, ExchangeDecl, PublishRequest,
    QueueBinding, QueueDecl, QueueInfo, Result, Transport,
};
use crate::url::TransportOptions;

static HOSTS: LazyLock<Mutex<HashMap<CompactString, Arc<Host>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Consumer panics leave broker invariants intact; recover the lock.
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn host_for(vhost: &str) -> Arc<Host> {
    let mut hosts = lock(&HOSTS);
    hosts
        .entry(CompactString::from(vhost))
        .or_insert_with(|| Arc::new(Host::default()))
        .clone()
}

#[derive(Debug, Default)]
struct Host {
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    exchanges: HashMap<CompactString, Exchange>,
    bindings: HashSet<BindingKey>,
    queues: HashMap<CompactString, VecDeque<Arc<Stored>>>,
    consumers: HashMap<u64, ConsumerState>,
    deliveries: HashMap<DeliveryTag, InFlight>,
    waiters: HashMap<u64, Waker>,
    next_consumer: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BindingKey {
    queue: CompactString,
    exchange: CompactString,
    routing_key: CompactString,
}

#[derive(Debug)]
struct Exchange {
    kind: crate::transport::ExchangeType,
    durable: bool,
    auto_delete: bool,
    arguments: Headers,
    table: Vec<PreparedBinding>,
}

impl Exchange {
    fn targets(&self, routing_key: &str) -> Vec<CompactString> {
        let mut seen = HashSet::new();
        let mut targets = Vec::new();
        for row in &self.table {
            if row.matches(self.kind, routing_key) && seen.insert(row.queue.clone()) {
                targets.push(row.queue.clone());
            }
        }
        targets
    }
}

#[derive(Debug, Clone)]
struct Stored {
    message: Message,
    exchange: CompactString,
    routing_key: CompactString,
    redelivered: bool,
}

#[derive(Debug)]
struct ConsumerState {
    in_flight: usize,
    queues: Vec<CompactString>,
    prefetch: usize,
    cursor: usize,
}

#[derive(Debug)]
struct InFlight {
    consumer: u64,
    queue: CompactString,
    stored: Arc<Stored>,
}

/// In-memory transport sharing state across connections to the same virtual host.
#[derive(Debug)]
pub struct MemoryTransport {
    host: Arc<Host>,
    vhost: CompactString,
    options: TransportOptions,
    closed: Arc<AtomicBool>,
    // Identifies this connection's get deliveries for cleanup on close.
    get_consumer: u64,
}

impl Default for MemoryTransport {
    fn default() -> Self {
        Self::new("/")
    }
}

impl MemoryTransport {
    pub fn new(vhost: impl Into<CompactString>) -> Self {
        Self::with_options(vhost, TransportOptions::default())
    }

    pub fn with_options(vhost: impl Into<CompactString>, options: TransportOptions) -> Self {
        let vhost = vhost.into();
        let host = host_for(&vhost);
        let get_consumer = {
            let mut guard = lock(&host.state);
            let state: &mut State = &mut guard;
            let id = state.next_consumer;
            state.next_consumer = id.wrapping_add(1);
            id
        };
        Self {
            host,
            vhost,
            options,
            closed: Arc::new(AtomicBool::new(false)),
            get_consumer,
        }
    }

    pub fn vhost(&self) -> &str {
        &self.vhost
    }

    pub fn options(&self) -> &TransportOptions {
        &self.options
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(Error::Closed);
        }
        Ok(())
    }

    fn not_found(&self, kind: &str, name: &str) -> Error {
        Error::NotFound(format!("no {kind} '{name}' in vhost '{}'", self.vhost))
    }

    // Enqueues publications and wakes waiting consumers.
    fn enqueue_all(&self, requests: Vec<PublishRequest>) -> Result<()> {
        self.ensure_open()?;
        let wakers = {
            let mut guard = lock(&self.host.state);
            let state: &mut State = &mut guard;
            for request in requests {
                enqueue(state, request, &self.options);
            }
            take_waiters(state)
        };
        wake(wakers);
        Ok(())
    }
}

fn enqueue(state: &mut State, request: PublishRequest, options: &TransportOptions) {
    let PublishRequest {
        exchange,
        routing_key,
        message,
    } = request;
    let exchange_name = exchange.clone().unwrap_or_default();

    let mut targets = if exchange_name.is_empty() {
        // The default exchange routes directly to the named queue.
        vec![routing_key.clone()]
    } else {
        match state.exchanges.get(&exchange_name) {
            Some(declared) => declared.targets(&routing_key),
            None => Vec::new(),
        }
    };

    if targets.is_empty() {
        // Match kombu's deadletter fallback; discard messages when no route exists.
        match &options.deadletter_queue {
            Some(deadletter) => targets.push(deadletter.clone()),
            None => return,
        }
    }

    let stored = Arc::new(Stored {
        message,
        exchange: exchange_name,
        routing_key,
        redelivered: false,
    });
    for queue in targets {
        state
            .queues
            .entry(queue)
            .or_default()
            .push_back(Arc::clone(&stored));
    }
}

fn take_waiters(state: &mut State) -> Vec<Waker> {
    state.waiters.drain().map(|(_, waker)| waker).collect()
}

// Wake outside the lock because wakers may reenter the transport.
fn wake(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}

// Removes a consumer, returning its waker for use outside the lock. Cancellation
// preserves pending acknowledgments; disconnection or stream drop discards them.
fn remove_consumer(state: &mut State, consumer_id: u64, drop_deliveries: bool) -> Option<Waker> {
    state.consumers.remove(&consumer_id);
    if drop_deliveries {
        state
            .deliveries
            .retain(|_, in_flight| in_flight.consumer != consumer_id);
    }
    state.waiters.remove(&consumer_id)
}

fn delivery_from(stored: &Stored, delivery_tag: DeliveryTag) -> Delivery {
    Delivery {
        message: stored.message.clone(),
        delivery_tag,
        exchange: stored.exchange.clone(),
        routing_key: stored.routing_key.clone(),
        redelivered: stored.redelivered,
    }
}

impl Transport for MemoryTransport {
    fn declare_exchange<'a>(&'a self, declaration: &'a ExchangeDecl) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.ensure_open()?;
            let mut guard = lock(&self.host.state);
            let state: &mut State = &mut guard;
            if declaration.passive {
                return match state.exchanges.contains_key(&declaration.name) {
                    true => Ok(()),
                    false => Err(self.not_found("exchange", &declaration.name)),
                };
            }
            match state.exchanges.get(&declaration.name) {
                Some(previous) => {
                    let equivalent = previous.kind == declaration.kind
                        && previous.durable == declaration.durable
                        && previous.auto_delete == declaration.auto_delete
                        && previous.arguments == declaration.arguments;
                    if !equivalent {
                        return Err(Error::ExchangeNotEquivalent(declaration.name.to_string()));
                    }
                }
                None => {
                    state.exchanges.insert(
                        declaration.name.clone(),
                        Exchange {
                            kind: declaration.kind,
                            durable: declaration.durable,
                            auto_delete: declaration.auto_delete,
                            arguments: declaration.arguments.clone(),
                            table: Vec::new(),
                        },
                    );
                }
            }
            Ok(())
        })
    }

    fn declare_queue<'a>(&'a self, declaration: &'a QueueDecl) -> BoxFuture<'a, Result<QueueInfo>> {
        Box::pin(async move {
            self.ensure_open()?;
            let name = declaration.name.clone().unwrap_or_else(|| {
                CompactString::from(format!("amq.gen-{}", uuid::Uuid::new_v4()))
            });
            let mut guard = lock(&self.host.state);
            let state: &mut State = &mut guard;
            if declaration.passive {
                let Some(messages) = state.queues.get(&name) else {
                    return Err(self.not_found("queue", &name));
                };
                return Ok(QueueInfo {
                    name,
                    message_count: messages.len() as u64,
                });
            }
            let messages = state.queues.entry(name.clone()).or_default();
            Ok(QueueInfo {
                name,
                message_count: messages.len() as u64,
            })
        })
    }

    fn bind<'a>(&'a self, binding: &'a QueueBinding) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.ensure_open()?;
            let mut guard = lock(&self.host.state);
            let state: &mut State = &mut guard;
            let Some(kind) = state
                .exchanges
                .get(&binding.exchange)
                .map(|exchange| exchange.kind)
            else {
                return Err(Error::UnknownExchange(binding.exchange.to_string()));
            };
            let prepared = PreparedBinding::new(kind, &binding.queue, &binding.routing_key)?;
            let key = BindingKey {
                queue: binding.queue.clone(),
                exchange: binding.exchange.clone(),
                routing_key: binding.routing_key.clone(),
            };
            if state.bindings.insert(key)
                && let Some(exchange) = state.exchanges.get_mut(&binding.exchange)
            {
                exchange.table.push(prepared);
            }
            Ok(())
        })
    }

    fn unbind<'a>(&'a self, binding: &'a QueueBinding) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.ensure_open()?;
            let mut guard = lock(&self.host.state);
            let state: &mut State = &mut guard;
            state.bindings.remove(&BindingKey {
                queue: binding.queue.clone(),
                exchange: binding.exchange.clone(),
                routing_key: binding.routing_key.clone(),
            });
            // kombu treats a missing exchange as a no-op for unbind.
            if let Some(exchange) = state.exchanges.get_mut(&binding.exchange) {
                exchange.table.retain(|row| {
                    !(row.queue == binding.queue && row.routing_key == binding.routing_key)
                });
            }
            Ok(())
        })
    }

    fn delete_exchange<'a>(&'a self, exchange: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.ensure_open()?;
            let mut guard = lock(&self.host.state);
            let state: &mut State = &mut guard;
            let Some(declared) = state.exchanges.get(exchange) else {
                return Err(self.not_found("exchange", exchange));
            };
            let queues: Vec<CompactString> =
                declared.table.iter().map(|row| row.queue.clone()).collect();
            // Match virtual.Channel.exchange_delete: remove empty queues and all their bindings.
            let mut seen = HashSet::new();
            for queue in queues {
                if !seen.insert(queue.clone()) {
                    continue;
                }
                if state.queues.get(&queue).is_none_or(VecDeque::is_empty) {
                    state.queues.remove(&queue);
                    state.bindings.retain(|key| key.queue != queue);
                    for other in state.exchanges.values_mut() {
                        other.table.retain(|row| row.queue != queue);
                    }
                }
            }
            state.bindings.retain(|key| key.exchange != exchange);
            state.exchanges.remove(exchange);
            Ok(())
        })
    }

    fn publish<'a>(&'a self, request: PublishRequest) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.enqueue_all(vec![request]) })
    }

    fn publish_batch<'a>(&'a self, requests: Vec<PublishRequest>) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.enqueue_all(requests) })
    }

    fn consume<'a>(
        &'a self,
        queues: &'a [CompactString],
        prefetch: u16,
    ) -> BoxFuture<'a, Result<Consumer>> {
        Box::pin(async move {
            self.ensure_open()?;
            if queues.is_empty() {
                return Err(Error::InvalidArgument(
                    "consume requires at least one queue".to_string(),
                ));
            }
            let consumer_id = {
                let mut guard = lock(&self.host.state);
                let state: &mut State = &mut guard;
                let id = state.next_consumer;
                state.next_consumer = id.wrapping_add(1);
                let mut unique: Vec<CompactString> = Vec::with_capacity(queues.len());
                for queue in queues {
                    if !unique.contains(queue) {
                        unique.push(queue.clone());
                    }
                }
                state.consumers.insert(
                    id,
                    ConsumerState {
                        in_flight: 0,
                        queues: unique,
                        prefetch: usize::from(prefetch),
                        cursor: 0,
                    },
                );
                id
            };
            let stream = MemoryStream {
                host: Arc::clone(&self.host),
                consumer_id,
                lost: Arc::clone(&self.closed),
                lost_reported: false,
                finished: false,
            };
            let control: Arc<dyn ConsumerControl> = Arc::new(MemoryConsumerControl {
                host: Arc::clone(&self.host),
                consumer_id,
            });
            Ok(Consumer {
                deliveries: Box::pin(stream) as DeliveryStream,
                control,
            })
        })
    }

    fn get<'a>(&'a self, queue: &'a str, no_ack: bool) -> BoxFuture<'a, Result<Option<Delivery>>> {
        Box::pin(async move {
            self.ensure_open()?;
            let mut guard = lock(&self.host.state);
            let state: &mut State = &mut guard;
            let Some(stored) = state.queues.get_mut(queue).and_then(VecDeque::pop_front) else {
                return Ok(None);
            };
            let delivery_tag = DeliveryTag::uuid();
            if !no_ack {
                state.deliveries.insert(
                    delivery_tag.clone(),
                    InFlight {
                        consumer: self.get_consumer,
                        queue: CompactString::from(queue),
                        stored: Arc::clone(&stored),
                    },
                );
            }
            Ok(Some(delivery_from(&stored, delivery_tag)))
        })
    }

    fn ack<'a>(&'a self, delivery_tag: &'a DeliveryTag) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.ensure_open()?;
            let wakers = {
                let mut guard = lock(&self.host.state);
                let state: &mut State = &mut guard;
                if let Some(in_flight) = state.deliveries.remove(delivery_tag) {
                    if let Some(consumer) = state.consumers.get_mut(&in_flight.consumer) {
                        consumer.in_flight = consumer.in_flight.saturating_sub(1);
                    }
                    take_waiters(state)
                } else {
                    Vec::new()
                }
            };
            wake(wakers);
            Ok(())
        })
    }

    fn reject<'a>(
        &'a self,
        delivery_tag: &'a DeliveryTag,
        requeue: bool,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.ensure_open()?;
            let wakers = {
                let mut guard = lock(&self.host.state);
                let state: &mut State = &mut guard;
                let Some(in_flight) = state.deliveries.remove(delivery_tag) else {
                    // kombu ignores unknown tags unless requeue is requested.
                    return if requeue {
                        Err(Error::UnknownDeliveryTag(delivery_tag.to_string()))
                    } else {
                        Ok(())
                    };
                };
                if let Some(consumer) = state.consumers.get_mut(&in_flight.consumer) {
                    consumer.in_flight = consumer.in_flight.saturating_sub(1);
                }
                if requeue {
                    let InFlight { queue, stored, .. } = in_flight;
                    let mut copy = match Arc::try_unwrap(stored) {
                        Ok(stored) => stored,
                        Err(shared) => (*shared).clone(),
                    };
                    copy.redelivered = true;
                    state
                        .queues
                        .entry(queue)
                        .or_default()
                        .push_back(Arc::new(copy));
                }
                take_waiters(state)
            };
            wake(wakers);
            Ok(())
        })
    }

    fn purge<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move {
            self.ensure_open()?;
            let mut guard = lock(&self.host.state);
            let state: &mut State = &mut guard;
            let purged = state
                .queues
                .get_mut(queue)
                .map(|messages| {
                    let count = messages.len() as u64;
                    messages.clear();
                    count
                })
                .unwrap_or(0);
            Ok(purged)
        })
    }

    fn queue_size<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move {
            self.ensure_open()?;
            let guard = lock(&self.host.state);
            Ok(guard
                .queues
                .get(queue)
                .map_or(0, |messages| messages.len() as u64))
        })
    }

    fn delete_queue<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.ensure_open()?;
            let mut guard = lock(&self.host.state);
            let state: &mut State = &mut guard;
            state.queues.remove(queue);
            state.bindings.retain(|key| key.queue != queue);
            for exchange in state.exchanges.values_mut() {
                exchange.table.retain(|row| row.queue != queue);
            }
            Ok(())
        })
    }

    fn close<'a>(&'a self) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.closed.store(true, Ordering::Relaxed);
            let wakers = {
                let mut guard = lock(&self.host.state);
                let state: &mut State = &mut guard;
                // Memory has do_restore=False; open streams discard their own pending deliveries
                // when they receive ConnectionLost.
                state
                    .deliveries
                    .retain(|_, in_flight| in_flight.consumer != self.get_consumer);
                take_waiters(state)
            };
            wake(wakers);
            Ok(())
        })
    }
}

#[derive(Debug)]
struct MemoryConsumerControl {
    host: Arc<Host>,
    consumer_id: u64,
}

impl ConsumerControl for MemoryConsumerControl {
    fn set_prefetch<'a>(&'a self, prefetch: u16) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let wakers = {
                let mut guard = lock(&self.host.state);
                let state: &mut State = &mut guard;
                let Some(consumer) = state.consumers.get_mut(&self.consumer_id) else {
                    return Err(Error::Closed);
                };
                consumer.prefetch = usize::from(prefetch);
                take_waiters(state)
            };
            wake(wakers);
            Ok(())
        })
    }

    fn add_queue<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let wakers = {
                let mut guard = lock(&self.host.state);
                let state: &mut State = &mut guard;
                let Some(consumer) = state.consumers.get_mut(&self.consumer_id) else {
                    return Err(Error::Closed);
                };
                if !consumer.queues.iter().any(|known| known == queue) {
                    consumer.queues.push(CompactString::from(queue));
                }
                take_waiters(state)
            };
            wake(wakers);
            Ok(())
        })
    }

    fn cancel_queue<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let wakers = {
                let mut guard = lock(&self.host.state);
                let state: &mut State = &mut guard;
                let Some(consumer) = state.consumers.get_mut(&self.consumer_id) else {
                    return Err(Error::Closed);
                };
                consumer.queues.retain(|known| known != queue);
                take_waiters(state)
            };
            wake(wakers);
            Ok(())
        })
    }

    fn cancel<'a>(&'a self) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let waiter = {
                let mut guard = lock(&self.host.state);
                let state: &mut State = &mut guard;
                // basic_cancel preserves pending acknowledgments.
                remove_consumer(state, self.consumer_id, false)
            };
            if let Some(waiter) = waiter {
                waiter.wake();
            }
            Ok(())
        })
    }
}

#[derive(Debug)]
struct MemoryStream {
    host: Arc<Host>,
    consumer_id: u64,
    lost: Arc<AtomicBool>,
    lost_reported: bool,
    finished: bool,
}

impl Stream for MemoryStream {
    type Item = Result<Delivery>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        if this.lost.load(Ordering::Relaxed) && !this.lost_reported {
            this.lost_reported = true;
            this.finished = true;
            let mut guard = lock(&this.host.state);
            let state: &mut State = &mut guard;
            remove_consumer(state, this.consumer_id, true);
            return Poll::Ready(Some(Err(Error::ConnectionLost(
                "memory transport closed".to_string(),
            ))));
        }
        let mut guard = lock(&this.host.state);
        let state: &mut State = &mut guard;
        let Some(consumer) = state.consumers.get_mut(&this.consumer_id) else {
            this.finished = true;
            return Poll::Ready(None);
        };
        if consumer.prefetch > 0 && consumer.in_flight >= consumer.prefetch {
            state.waiters.insert(this.consumer_id, cx.waker().clone());
            return Poll::Pending;
        }
        match pop_next(&consumer.queues, &mut consumer.cursor, &mut state.queues) {
            Some((queue, stored)) => {
                consumer.in_flight += 1;
                let delivery_tag = DeliveryTag::uuid();
                state.deliveries.insert(
                    delivery_tag.clone(),
                    InFlight {
                        consumer: this.consumer_id,
                        queue,
                        stored: Arc::clone(&stored),
                    },
                );
                Poll::Ready(Some(Ok(delivery_from(&stored, delivery_tag))))
            }
            None => {
                state.waiters.insert(this.consumer_id, cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl Drop for MemoryStream {
    fn drop(&mut self) {
        let mut guard = lock(&self.host.state);
        let state: &mut State = &mut guard;
        remove_consumer(state, self.consumer_id, true);
    }
}

fn pop_next(
    queues: &[CompactString],
    cursor: &mut usize,
    state: &mut HashMap<CompactString, VecDeque<Arc<Stored>>>,
) -> Option<(CompactString, Arc<Stored>)> {
    let total = queues.len();
    if total == 0 {
        return None;
    }
    for offset in 0..total {
        let index = (*cursor + offset) % total;
        let queue = &queues[index];
        if let Some(messages) = state.get_mut(queue)
            && let Some(stored) = messages.pop_front()
        {
            *cursor = (index + 1) % total;
            return Some((queue.clone(), stored));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;

    use super::*;
    use crate::message::{HeaderValue, Properties};
    use crate::transport::ExchangeType;

    const WAIT: Duration = Duration::from_secs(1);

    async fn next(stream: &mut DeliveryStream) -> Option<Result<Delivery>> {
        std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
    }

    async fn recv(stream: &mut DeliveryStream) -> Delivery {
        tokio::time::timeout(WAIT, next(stream))
            .await
            .expect("timed out waiting for a message")
            .expect("stream ended")
            .expect("delivery error")
    }

    async fn setup(
        vhost: &str,
        exchange: &str,
        kind: ExchangeType,
        queue: &str,
        routing_key: &str,
    ) -> MemoryTransport {
        let transport = MemoryTransport::new(vhost);
        transport
            .declare_exchange(&ExchangeDecl::new(exchange, kind))
            .await
            .expect("exchange");
        transport
            .declare_queue(&QueueDecl::new(queue))
            .await
            .expect("queue");
        transport
            .bind(&QueueBinding::new(queue, exchange, routing_key))
            .await
            .expect("binding");
        transport
    }

    fn message(body: &str) -> Message {
        Message::new(Bytes::copy_from_slice(body.as_bytes()))
            .with_content_type("application/json")
            .with_content_encoding("utf-8")
    }

    #[tokio::test]
    async fn publish_consume_ack() {
        let transport = setup("t1", "ex", ExchangeType::Direct, "q", "rk").await;
        let mut headers = Headers::new();
        headers.insert("task", HeaderValue::from("add"));
        headers.insert("retries", HeaderValue::Int(2));
        let mut properties = Properties::new();
        properties.set_delivery_mode(2);
        properties.set_priority(3);
        properties.set_correlation_id("id-1");
        let mut msg = message("{\"x\": 1}").with_headers(headers.clone());
        msg.properties = properties.clone();

        transport
            .publish(PublishRequest::new(Some("ex"), "rk", msg.clone()))
            .await
            .expect("publish");
        assert_eq!(transport.queue_size("q").await.expect("size"), 1);

        let mut stream = transport
            .consume(&[CompactString::from("q")], 10)
            .await
            .expect("consume")
            .into_stream();
        let delivery = recv(&mut stream).await;
        assert_eq!(delivery.message.body, msg.body);
        assert_eq!(delivery.message.content_type, msg.content_type);
        assert_eq!(delivery.message.content_encoding, msg.content_encoding);
        assert_eq!(delivery.message.headers, headers);
        assert_eq!(delivery.message.properties, properties);
        assert_eq!(delivery.exchange, "ex");
        assert_eq!(delivery.routing_key, "rk");
        assert!(!delivery.redelivered);
        assert!(delivery.delivery_tag.as_str().is_some());

        transport.ack(&delivery.delivery_tag).await.expect("ack");
        assert_eq!(transport.queue_size("q").await.expect("size"), 0);
    }

    #[tokio::test]
    async fn reject_without_requeue_discards() {
        let transport = setup("t2", "ex", ExchangeType::Direct, "q", "rk").await;
        transport
            .publish(PublishRequest::new(Some("ex"), "rk", message("a")))
            .await
            .expect("publish");
        let mut stream = transport
            .consume(&[CompactString::from("q")], 0)
            .await
            .expect("consume")
            .into_stream();
        let delivery = recv(&mut stream).await;
        transport
            .reject(&delivery.delivery_tag, false)
            .await
            .expect("reject");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), next(&mut stream))
                .await
                .is_err()
        );
        assert_eq!(transport.queue_size("q").await.expect("size"), 0);
    }

    #[tokio::test]
    async fn reject_with_requeue_appends_and_marks_redelivered() {
        let transport = setup("t3", "ex", ExchangeType::Direct, "q", "rk").await;
        for body in ["one", "two"] {
            transport
                .publish(PublishRequest::new(Some("ex"), "rk", message(body)))
                .await
                .expect("publish");
        }
        let mut stream = transport
            .consume(&[CompactString::from("q")], 0)
            .await
            .expect("consume")
            .into_stream();
        let first = recv(&mut stream).await;
        assert_eq!(first.message.body, Bytes::from_static(b"one"));
        transport
            .reject(&first.delivery_tag, true)
            .await
            .expect("requeue");

        // Memory requeues at the tail, matching Queue.put.
        let second = recv(&mut stream).await;
        assert_eq!(second.message.body, Bytes::from_static(b"two"));
        let third = recv(&mut stream).await;
        assert_eq!(third.message.body, Bytes::from_static(b"one"));
        assert!(third.redelivered);
    }

    #[tokio::test]
    async fn prefetch_limits_in_flight() {
        let transport = setup("t4", "ex", ExchangeType::Direct, "q", "rk").await;
        for index in 0..5 {
            let body = format!("m{index}");
            transport
                .publish(PublishRequest::new(Some("ex"), "rk", message(&body)))
                .await
                .expect("publish");
        }
        let mut stream = transport
            .consume(&[CompactString::from("q")], 2)
            .await
            .expect("consume")
            .into_stream();

        let first = recv(&mut stream).await;
        let second = recv(&mut stream).await;
        assert_eq!(first.message.body, Bytes::from_static(b"m0"));
        assert_eq!(second.message.body, Bytes::from_static(b"m1"));

        assert!(
            tokio::time::timeout(Duration::from_millis(50), next(&mut stream))
                .await
                .is_err()
        );

        transport.ack(&first.delivery_tag).await.expect("ack");
        let third = recv(&mut stream).await;
        assert_eq!(third.message.body, Bytes::from_static(b"m2"));
    }

    #[tokio::test]
    async fn fanout_delivers_to_every_bound_queue() {
        let transport = MemoryTransport::new("t5");
        transport
            .declare_exchange(&ExchangeDecl::new("fan", ExchangeType::Fanout))
            .await
            .expect("exchange");
        for queue in ["a", "b"] {
            transport
                .declare_queue(&QueueDecl::new(queue))
                .await
                .expect("queue");
            transport
                .bind(&QueueBinding::new(queue, "fan", ""))
                .await
                .expect("binding");
        }
        transport
            .publish(PublishRequest::new(Some("fan"), "", message("bc")))
            .await
            .expect("publish");
        assert_eq!(transport.queue_size("a").await.expect("size"), 1);
        assert_eq!(transport.queue_size("b").await.expect("size"), 1);

        for queue in ["a", "b"] {
            let mut stream = transport
                .consume(&[CompactString::from(queue)], 0)
                .await
                .expect("consume")
                .into_stream();
            let delivery = recv(&mut stream).await;
            assert_eq!(delivery.message.body, Bytes::from_static(b"bc"));
            assert_eq!(delivery.exchange, "fan");
        }
    }

    #[tokio::test]
    async fn topic_routing_matches_kombu_patterns() {
        let transport = MemoryTransport::new("t6");
        transport
            .declare_exchange(&ExchangeDecl::new("topics", ExchangeType::Topic))
            .await
            .expect("exchange");
        for (queue, routing_key) in [
            ("all", "#"),
            ("created", "*.created"),
            ("account", "account.#"),
            ("exact", "account.1.created"),
        ] {
            transport
                .declare_queue(&QueueDecl::new(queue))
                .await
                .expect("queue");
            transport
                .bind(&QueueBinding::new(queue, "topics", routing_key))
                .await
                .expect("binding");
        }

        transport
            .publish(PublishRequest::new(
                Some("topics"),
                "account.1.created",
                message("hit"),
            ))
            .await
            .expect("publish");
        transport
            .publish(PublishRequest::new(
                Some("topics"),
                "account.created",
                message("without-index"),
            ))
            .await
            .expect("publish");
        for queue in ["all", "created", "account", "exact"] {
            let expected = match queue {
                "created" => 1,
                "exact" => 1,
                _ => 2,
            };
            assert_eq!(
                transport.queue_size(queue).await.expect("size"),
                expected,
                "{queue}"
            );
        }

        // kombu requires the dot: `account.#` does not match `account`.
        transport
            .publish(PublishRequest::new(
                Some("topics"),
                "account",
                message("bare"),
            ))
            .await
            .expect("publish");
        assert_eq!(transport.queue_size("all").await.expect("size"), 3);
        assert_eq!(transport.queue_size("created").await.expect("size"), 1);
        assert_eq!(transport.queue_size("account").await.expect("size"), 2);
        assert_eq!(transport.queue_size("exact").await.expect("size"), 1);
    }

    #[tokio::test]
    async fn anonymous_exchange_routes_to_queue_name() {
        let transport = MemoryTransport::new("t7");
        transport
            .publish(PublishRequest::new(
                None::<&str>,
                "direct-q",
                message("anon"),
            ))
            .await
            .expect("publish");
        let mut stream = transport
            .consume(&[CompactString::from("direct-q")], 0)
            .await
            .expect("consume")
            .into_stream();
        let delivery = recv(&mut stream).await;
        assert_eq!(delivery.message.body, Bytes::from_static(b"anon"));
        assert_eq!(delivery.exchange, "");
        assert_eq!(delivery.routing_key, "direct-q");
    }

    #[tokio::test]
    async fn multiple_queues_are_served_round_robin() {
        let transport = MemoryTransport::new("t8");
        for queue in ["q1", "q2"] {
            transport
                .publish(PublishRequest::new(None::<&str>, queue, message("x")))
                .await
                .expect("publish");
            transport
                .publish(PublishRequest::new(None::<&str>, queue, message("y")))
                .await
                .expect("publish");
        }
        let queues = [CompactString::from("q1"), CompactString::from("q2")];
        let mut stream = transport
            .consume(&queues, 0)
            .await
            .expect("consume")
            .into_stream();
        let mut seen = Vec::new();
        for _ in 0..4 {
            let delivery = recv(&mut stream).await;
            seen.push(delivery.routing_key.to_string());
            transport.ack(&delivery.delivery_tag).await.expect("ack");
        }
        assert_eq!(seen, vec!["q1", "q2", "q1", "q2"]);
    }

    #[tokio::test]
    async fn priority_is_preserved_but_fifo() {
        let transport = setup("t9", "ex", ExchangeType::Direct, "q", "rk").await;
        for (body, priority) in [("low", 0), ("high", 9), ("mid", 4)] {
            let mut properties = Properties::new();
            properties.set_priority(priority);
            let msg = Message::new(Bytes::from_static(body.as_bytes())).with_properties(properties);
            transport
                .publish(PublishRequest::new(Some("ex"), "rk", msg))
                .await
                .expect("publish");
        }
        let mut stream = transport
            .consume(&[CompactString::from("q")], 0)
            .await
            .expect("consume")
            .into_stream();
        for (body, priority) in [("low", 0), ("high", 9), ("mid", 4)] {
            let delivery = recv(&mut stream).await;
            assert_eq!(delivery.message.body, Bytes::from_static(body.as_bytes()));
            assert_eq!(delivery.message.properties.priority(), Some(priority));
        }
    }

    #[tokio::test]
    async fn purge_size_and_delete() {
        let transport = setup("t10", "ex", ExchangeType::Direct, "q", "rk").await;
        for body in ["a", "b", "c"] {
            transport
                .publish(PublishRequest::new(Some("ex"), "rk", message(body)))
                .await
                .expect("publish");
        }
        assert_eq!(transport.queue_size("q").await.expect("size"), 3);
        assert_eq!(transport.purge("q").await.expect("purge"), 3);
        assert_eq!(transport.queue_size("q").await.expect("size"), 0);

        transport
            .publish(PublishRequest::new(Some("ex"), "rk", message("again")))
            .await
            .expect("publish");
        transport.delete_queue("q").await.expect("delete");
        assert_eq!(transport.queue_size("q").await.expect("size"), 0);

        transport
            .publish(PublishRequest::new(Some("ex"), "rk", message("lost")))
            .await
            .expect("publish");
        assert_eq!(transport.queue_size("q").await.expect("size"), 0);
    }

    #[tokio::test]
    async fn state_is_shared_between_connections_on_same_vhost() {
        let producer = setup("t11", "ex", ExchangeType::Direct, "q", "rk").await;
        let consumer = MemoryTransport::new("t11");
        producer
            .publish(PublishRequest::new(Some("ex"), "rk", message("shared")))
            .await
            .expect("publish");
        let mut stream = consumer
            .consume(&[CompactString::from("q")], 0)
            .await
            .expect("consume")
            .into_stream();
        let delivery = recv(&mut stream).await;
        assert_eq!(delivery.message.body, Bytes::from_static(b"shared"));
        consumer.ack(&delivery.delivery_tag).await.expect("ack");

        let other = MemoryTransport::new("t11-other");
        assert_eq!(other.queue_size("q").await.expect("size"), 0);
    }

    #[tokio::test]
    async fn duplicate_bindings_deliver_once() {
        let transport = setup("t12", "ex", ExchangeType::Topic, "q", "#").await;
        transport
            .bind(&QueueBinding::new("q", "ex", "a.#"))
            .await
            .expect("second binding");
        transport
            .publish(PublishRequest::new(Some("ex"), "a.b", message("once")))
            .await
            .expect("publish");
        assert_eq!(transport.queue_size("q").await.expect("size"), 1);
    }

    #[tokio::test]
    async fn exchange_declarations_must_be_equivalent() {
        let transport = MemoryTransport::new("t13");
        transport
            .declare_exchange(&ExchangeDecl::new("ex", ExchangeType::Direct))
            .await
            .expect("first declaration");
        assert!(matches!(
            transport
                .declare_exchange(&ExchangeDecl::new("ex", ExchangeType::Topic))
                .await,
            Err(Error::ExchangeNotEquivalent(_))
        ));
        assert!(matches!(
            transport
                .bind(&QueueBinding::new("q", "missing", "rk"))
                .await,
            Err(Error::UnknownExchange(_))
        ));
        assert!(matches!(
            transport.consume(&[], 0).await,
            Err(Error::InvalidArgument(_))
        ));
    }

    #[tokio::test]
    async fn dead_letter_queue_receives_unroutable_messages() {
        let options = TransportOptions {
            deadletter_queue: Some(CompactString::from("dead")),
            ..TransportOptions::default()
        };
        let transport = MemoryTransport::with_options("t14", options);
        transport
            .declare_exchange(&ExchangeDecl::new("ex", ExchangeType::Direct))
            .await
            .expect("exchange");
        transport
            .publish(PublishRequest::new(Some("ex"), "nothing", message("lost")))
            .await
            .expect("publish");
        assert_eq!(transport.queue_size("dead").await.expect("size"), 1);
    }

    #[tokio::test]
    async fn closed_transport_rejects_operations() {
        let transport = MemoryTransport::new("t15");
        transport.close().await.expect("close");
        assert!(matches!(
            transport.queue_size("q").await,
            Err(Error::Closed)
        ));
    }

    #[tokio::test]
    async fn publish_batch_enqueues_all() {
        let transport = setup("t17", "ex", ExchangeType::Direct, "q", "rk").await;
        transport
            .publish_batch(vec![
                PublishRequest::new(Some("ex"), "rk", message("a")),
                PublishRequest::new(Some("ex"), "rk", message("b")),
                PublishRequest::new(Some("ex"), "other", message("dropped")),
            ])
            .await
            .expect("publish_batch");
        assert_eq!(transport.queue_size("q").await.expect("size"), 2);

        let mut stream = transport
            .consume(&[CompactString::from("q")], 0)
            .await
            .expect("consume")
            .into_stream();
        let first = recv(&mut stream).await;
        let second = recv(&mut stream).await;
        assert_eq!(first.message.body, Bytes::from_static(b"a"));
        assert_eq!(second.message.body, Bytes::from_static(b"b"));
    }

    #[tokio::test]
    async fn dropping_stream_drops_unacked_messages() {
        let transport = setup("t16", "ex", ExchangeType::Direct, "q", "rk").await;
        transport
            .publish(PublishRequest::new(Some("ex"), "rk", message("gone")))
            .await
            .expect("publish");
        {
            let mut stream = transport
                .consume(&[CompactString::from("q")], 0)
                .await
                .expect("consume")
                .into_stream();
            let _ = recv(&mut stream).await;
            // Match the memory transport's do_restore=False.
        }
        assert_eq!(transport.queue_size("q").await.expect("size"), 0);
        let mut empty_stream = transport
            .consume(&[CompactString::from("q")], 0)
            .await
            .expect("consume")
            .into_stream();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), next(&mut empty_stream))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn get_pops_one_message_and_honours_no_ack() {
        let transport = setup("t18", "ex", ExchangeType::Direct, "q", "rk").await;
        for body in ["a", "b"] {
            transport
                .publish(PublishRequest::new(Some("ex"), "rk", message(body)))
                .await
                .expect("publish");
        }

        let first = transport
            .get("q", true)
            .await
            .expect("get")
            .expect("message");
        assert_eq!(first.message.body, Bytes::from_static(b"a"));
        assert_eq!(first.exchange, "ex");
        assert_eq!(first.routing_key, "rk");

        transport.ack(&first.delivery_tag).await.expect("ack no-op");
        assert_eq!(transport.queue_size("q").await.expect("size"), 1);

        let second = transport
            .get("q", false)
            .await
            .expect("get")
            .expect("message");
        assert_eq!(second.message.body, Bytes::from_static(b"b"));
        assert_eq!(transport.queue_size("q").await.expect("size"), 0);
        transport
            .reject(&second.delivery_tag, true)
            .await
            .expect("requeue");
        assert_eq!(transport.queue_size("q").await.expect("size"), 1);

        let third = transport
            .get("q", false)
            .await
            .expect("get")
            .expect("message");
        assert_eq!(third.message.body, Bytes::from_static(b"b"));
        assert!(third.redelivered);
        transport.ack(&third.delivery_tag).await.expect("ack");

        assert!(transport.get("q", true).await.expect("get").is_none());
        assert!(transport.get("missing", true).await.expect("get").is_none());
    }

    #[tokio::test]
    async fn consumer_control_changes_prefetch_and_queues() {
        let transport = MemoryTransport::new("t19");
        for index in 0..4 {
            transport
                .publish(PublishRequest::new(
                    None::<&str>,
                    "q1",
                    message(&format!("q1-{index}")),
                ))
                .await
                .expect("publish");
        }
        for index in 0..2 {
            transport
                .publish(PublishRequest::new(
                    None::<&str>,
                    "q2",
                    message(&format!("q2-{index}")),
                ))
                .await
                .expect("publish");
        }

        let consumer = transport
            .consume(&[CompactString::from("q1")], 2)
            .await
            .expect("consume");
        let mut stream = consumer.deliveries;
        let control = consumer.control;

        let first = recv(&mut stream).await;
        let second = recv(&mut stream).await;
        assert_eq!(first.message.body, Bytes::from_static(b"q1-0"));
        assert_eq!(second.message.body, Bytes::from_static(b"q1-1"));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), next(&mut stream))
                .await
                .is_err()
        );

        control.set_prefetch(4).await.expect("set_prefetch");
        let third = recv(&mut stream).await;
        let fourth = recv(&mut stream).await;
        assert_eq!(third.message.body, Bytes::from_static(b"q1-2"));
        assert_eq!(fourth.message.body, Bytes::from_static(b"q1-3"));
        transport.ack(&first.delivery_tag).await.expect("ack");
        transport.ack(&second.delivery_tag).await.expect("ack");

        control.add_queue("q2").await.expect("add_queue");
        let fifth = recv(&mut stream).await;
        assert_eq!(fifth.message.body, Bytes::from_static(b"q2-0"));
        assert_eq!(fifth.routing_key, "q2");

        control.set_prefetch(2).await.expect("set_prefetch");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), next(&mut stream))
                .await
                .is_err()
        );
        transport.ack(&third.delivery_tag).await.expect("ack");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), next(&mut stream))
                .await
                .is_err()
        );
        transport.ack(&fourth.delivery_tag).await.expect("ack");
        let sixth = recv(&mut stream).await;
        assert_eq!(sixth.message.body, Bytes::from_static(b"q2-1"));

        control.cancel_queue("q2").await.expect("cancel_queue");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), next(&mut stream))
                .await
                .is_err()
        );

        control.cancel().await.expect("cancel");
        assert!(
            tokio::time::timeout(WAIT, next(&mut stream))
                .await
                .expect("no timeout")
                .is_none()
        );
        assert!(matches!(control.set_prefetch(1).await, Err(Error::Closed)));
    }

    #[tokio::test]
    async fn cancel_keeps_in_flight_ackable() {
        let transport = setup("t20", "ex", ExchangeType::Direct, "q", "rk").await;
        transport
            .publish(PublishRequest::new(Some("ex"), "rk", message("inflight")))
            .await
            .expect("publish");
        let consumer = transport
            .consume(&[CompactString::from("q")], 0)
            .await
            .expect("consume");
        let mut stream = consumer.deliveries;
        let delivery = recv(&mut stream).await;
        consumer.control.cancel().await.expect("cancel");
        assert!(
            tokio::time::timeout(WAIT, next(&mut stream))
                .await
                .expect("no timeout")
                .is_none()
        );
        // basic_cancel preserves pending acknowledgments without requeuing.
        assert_eq!(transport.queue_size("q").await.expect("size"), 0);
        transport
            .ack(&delivery.delivery_tag)
            .await
            .expect("ack after cancel");

        let mut stream = transport
            .consume(&[CompactString::from("q")], 0)
            .await
            .expect("consume")
            .into_stream();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), next(&mut stream))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn unbind_removes_routing() {
        let transport = setup("t21", "ex", ExchangeType::Direct, "q", "rk").await;
        transport
            .publish(PublishRequest::new(Some("ex"), "rk", message("first")))
            .await
            .expect("publish");
        assert_eq!(transport.queue_size("q").await.expect("size"), 1);
        transport
            .unbind(&QueueBinding::new("q", "ex", "rk"))
            .await
            .expect("unbind");
        transport
            .publish(PublishRequest::new(Some("ex"), "rk", message("second")))
            .await
            .expect("publish");
        assert_eq!(transport.queue_size("q").await.expect("size"), 1);

        // kombu ignores unbind on a missing exchange.
        transport
            .unbind(&QueueBinding::new("q", "missing", "rk"))
            .await
            .expect("unbind no-op");

        let first = transport.get("q", true).await.expect("get").expect("msg");
        assert_eq!(first.message.body, Bytes::from_static(b"first"));
        assert!(transport.get("q", true).await.expect("get").is_none());
    }

    #[tokio::test]
    async fn delete_exchange_removes_empty_queues_and_bindings() {
        let transport = MemoryTransport::new("t22");
        transport
            .declare_exchange(&ExchangeDecl::new("ex", ExchangeType::Direct))
            .await
            .expect("exchange");
        for queue in ["empty", "full"] {
            transport
                .declare_queue(&QueueDecl::new(queue))
                .await
                .expect("queue");
            transport
                .bind(&QueueBinding::new(queue, "ex", queue))
                .await
                .expect("binding");
        }
        transport
            .publish(PublishRequest::new(Some("ex"), "full", message("keep")))
            .await
            .expect("publish");

        transport
            .delete_exchange("ex")
            .await
            .expect("delete_exchange");

        assert!(matches!(
            transport
                .declare_exchange(&ExchangeDecl {
                    passive: true,
                    ..ExchangeDecl::new("ex", ExchangeType::Direct)
                })
                .await,
            Err(Error::NotFound(_))
        ));

        assert!(matches!(
            transport
                .declare_queue(&QueueDecl {
                    passive: true,
                    ..QueueDecl::new("empty")
                })
                .await,
            Err(Error::NotFound(_))
        ));
        let full = transport
            .declare_queue(&QueueDecl {
                passive: true,
                ..QueueDecl::new("full")
            })
            .await
            .expect("nonempty queue");
        assert_eq!(full.message_count, 1);

        transport
            .publish(PublishRequest::new(Some("ex"), "full", message("lost")))
            .await
            .expect("publish");
        assert_eq!(transport.queue_size("full").await.expect("size"), 1);
        assert!(matches!(
            transport.delete_exchange("ex").await,
            Err(Error::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn passive_declarations_check_existence() {
        let transport = MemoryTransport::new("t23");
        assert!(matches!(
            transport
                .declare_exchange(&ExchangeDecl {
                    passive: true,
                    ..ExchangeDecl::new("missing", ExchangeType::Direct)
                })
                .await,
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            transport
                .declare_queue(&QueueDecl {
                    passive: true,
                    ..QueueDecl::new("missing")
                })
                .await,
            Err(Error::NotFound(_))
        ));

        transport
            .declare_exchange(&ExchangeDecl::new("ex", ExchangeType::Topic))
            .await
            .expect("exchange");
        transport
            .declare_queue(&QueueDecl::new("q"))
            .await
            .expect("queue");
        transport
            .publish(PublishRequest::new(None::<&str>, "q", message("x")))
            .await
            .expect("publish");

        // Passive declarations do not check parameter equivalence.
        transport
            .declare_exchange(&ExchangeDecl {
                passive: true,
                ..ExchangeDecl::new("ex", ExchangeType::Direct)
            })
            .await
            .expect("passive exchange");
        let info = transport
            .declare_queue(&QueueDecl {
                passive: true,
                ..QueueDecl::new("q")
            })
            .await
            .expect("passive queue");
        assert_eq!(info.name, "q");
        assert_eq!(info.message_count, 1);
    }

    #[tokio::test]
    async fn closing_transport_emits_connection_lost() {
        let transport = setup("t24", "ex", ExchangeType::Direct, "q", "rk").await;
        let mut stream = transport
            .consume(&[CompactString::from("q")], 0)
            .await
            .expect("consume")
            .into_stream();

        assert!(
            tokio::time::timeout(Duration::from_millis(50), next(&mut stream))
                .await
                .is_err()
        );

        transport.close().await.expect("close");
        match tokio::time::timeout(WAIT, next(&mut stream))
            .await
            .expect("no timeout")
        {
            Some(Err(Error::ConnectionLost(_))) => {}
            Some(Err(other)) => panic!("expected ConnectionLost, got {other}"),
            Some(Ok(delivery)) => panic!("unexpected delivery: {delivery:?}"),
            None => panic!("stream ended without ConnectionLost"),
        }

        assert!(
            tokio::time::timeout(WAIT, next(&mut stream))
                .await
                .expect("no timeout")
                .is_none()
        );
    }

    #[tokio::test]
    async fn closing_one_connection_does_not_affect_others() {
        let transport = setup("t25", "ex", ExchangeType::Direct, "q", "rk").await;
        let other = MemoryTransport::new("t25");
        let mut stream = transport
            .consume(&[CompactString::from("q")], 0)
            .await
            .expect("consume")
            .into_stream();
        other.close().await.expect("close");
        other
            .publish(PublishRequest::new(Some("ex"), "rk", message("still")))
            .await
            .expect_err("closed transport rejects publications");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), next(&mut stream))
                .await
                .is_err()
        );

        transport
            .publish(PublishRequest::new(Some("ex"), "rk", message("alive")))
            .await
            .expect("publish");
        let delivery = recv(&mut stream).await;
        assert_eq!(delivery.message.body, Bytes::from_static(b"alive"));
    }

    #[tokio::test]
    async fn closing_a_transport_keeps_other_get_deliveries_ackable() {
        let publisher = setup("t26", "ex", ExchangeType::Direct, "q", "rk").await;
        let other = MemoryTransport::new("t26");
        publisher
            .publish(PublishRequest::new(Some("ex"), "rk", message("pending")))
            .await
            .expect("publish");
        let delivery = other.get("q", false).await.expect("get").expect("message");
        publisher.close().await.expect("close");
        other
            .ack(&delivery.delivery_tag)
            .await
            .expect("ack on open connection");
    }
}
