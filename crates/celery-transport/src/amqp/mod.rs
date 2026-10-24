//! Trasporto AMQP 0.9.1 su `lapin`.
//!
//! Mappa il contratto di [`crate::transport`] sulle primitive AMQP:
//! un canale dedicato per ogni consumatore (prefetch e delivery tag sono per
//! canale), un canale per le dichiarazioni, uno per le pubblicazioni e uno per
//! `basic_get`. Il tag esposto è `(channel_id << 40) | delivery_tag`: il worker
//! vede un `u64` stabile e `ack`/`reject` sanno su quale canale operare.
//!
//! I messaggi usano le proprietà native (`content_type`, `headers`,
//! `delivery_mode`, …) e le tabelle di header sono convertite negli stessi tipi
//! che py-amqp restituisce a kombu.
//!
//! Opzioni (`transport_options`): `confirm_publish` (publisher confirms),
//! `qos_global` (flag `global` di `basic.qos`; default `false`, il valore che
//! kombu usa su RabbitMQ >= 4.0, sovrascrivibile perché lapin non espone le
//! `server_properties`) e `connection_name` (nome mostrato dall'API di
//! management).

mod convert;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use compact_str::CompactString;
use futures_core::Stream;
use lapin::message::Delivery as LapinDelivery;
use lapin::options::{
    BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicGetOptions, BasicPublishOptions,
    BasicQosOptions, BasicRejectOptions, ConfirmSelectOptions, ExchangeDeclareOptions,
    ExchangeDeleteOptions, QueueBindOptions, QueueDeclareOptions, QueueDeleteOptions,
    QueuePurgeOptions,
};
use lapin::types::{FieldTable, LongString, ShortString};
use lapin::uri::{AMQPAuthority, AMQPQueryString, AMQPScheme, AMQPUri, AMQPUserInfo};
use lapin::{
    Channel, Confirmation, Connection, ConnectionProperties, Consumer as LapinConsumer, ErrorKind,
    ExchangeKind,
};

use crate::message::{Delivery, DeliveryTag};
use crate::transport::{
    BoxFuture, Consumer, ConsumerControl, DeliveryStream, Error, ExchangeDecl, ExchangeType,
    PublishRequest, QueueBinding, QueueDecl, QueueInfo, Result, RetryPolicy, Transport,
};
use crate::url::{TransportKind, TransportOptions, Url};

use convert::{message_from_amqp, message_to_properties, short_string, to_field_table};

/// Bit riservati all'id del canale nel [`DeliveryTag`] esposto.
const TAG_SHIFT: u32 = 40;
const TAG_MASK: u64 = (1_u64 << TAG_SHIFT) - 1;

fn encode_tag(channel_id: u16, delivery_tag: u64) -> DeliveryTag {
    DeliveryTag::Number((u64::from(channel_id) << TAG_SHIFT) | (delivery_tag & TAG_MASK))
}

fn decode_tag(tag: &DeliveryTag) -> Option<(u16, u64)> {
    match tag {
        DeliveryTag::Number(encoded) => Some(((encoded >> TAG_SHIFT) as u16, encoded & TAG_MASK)),
        DeliveryTag::Uuid(_) => None,
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Un panic in un task non deve rendere il trasporto inutilizzabile.
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn is_protocol_id(error: &lapin::Error, id: u16) -> bool {
    match error.kind() {
        ErrorKind::ProtocolError(protocol) => protocol.get_id() == id,
        _ => false,
    }
}

fn map_lapin(error: lapin::Error) -> Error {
    if error.is_amqp_soft_error() || error.is_amqp_hard_error() {
        return Error::Channel(error.to_string());
    }
    if error.is_io_error()
        || matches!(
            error.kind(),
            ErrorKind::MissingHeartbeatError | ErrorKind::InvalidConnectionState(_)
        )
    {
        return Error::Connection(error.to_string());
    }
    Error::Channel(error.to_string())
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn map_bind_error(error: lapin::Error, exchange: &str) -> Error {
    if is_protocol_id(&error, 404) {
        let text = error.to_string();
        return if text.contains("exchange") {
            Error::UnknownExchange(exchange.to_string())
        } else {
            Error::NotFound(text)
        };
    }
    map_lapin(error)
}

/// Canale con l'insieme dei tag in volo: ack/reject non devono chiudere il
/// canale quando il tag è sconosciuto (il broker risponderebbe con un errore
/// di protocollo).
struct ChannelState {
    id: u16,
    channel: Channel,
    in_flight: Mutex<HashSet<u64>>,
}

impl ChannelState {
    fn new(channel: Channel) -> Self {
        Self {
            id: channel.id(),
            channel,
            in_flight: Mutex::new(HashSet::new()),
        }
    }
}

/// Stato condiviso da trasporto, consumatori e controllo.
struct Shared {
    connection: Connection,
    confirm_publish: bool,
    /// `basic.qos(global)` per i cambi di prefetch. lapin 4.12 non espone le
    /// `server_properties`, quindi il default non può essere calcolato come
    /// `qos_semantics_matches_spec` di kombu: è `false`, il valore che kombu
    /// usa su RabbitMQ >= 4.0 (su RabbitMQ 3.3–3.x va impostato `true`). Si
    /// sovrascrive con `transport_options["qos_global"]` o `?qos_global=true`.
    /// Con code quorum il QoS globale non è supportato: il limite resta
    /// per-consumer e si applica dal consumer successivo.
    qos_global: bool,
    closed: AtomicBool,
    control_channel: Mutex<Option<Channel>>,
    publish_channel: Mutex<Option<Channel>>,
    get_channel: Mutex<Option<Arc<ChannelState>>>,
    /// Canali che possono avere consegne in volo (weak: alla chiusura del
    /// consumatore il canale si chiude e il broker rimette in coda).
    channels: Mutex<HashMap<u16, Weak<ChannelState>>>,
    streams: Mutex<HashMap<u64, Arc<StreamState>>>,
    next_stream: AtomicU64,
}

impl Shared {
    fn ensure_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(Error::Closed);
        }
        Ok(())
    }

    fn register(&self, state: &Arc<ChannelState>) {
        let mut channels = lock(&self.channels);
        // I canali chiusi non vanno accumulati nel registro.
        channels.retain(|_, weak| weak.strong_count() > 0);
        channels.insert(state.id, Arc::downgrade(state));
    }

    async fn channel_from(&self, slot: &Mutex<Option<Channel>>) -> Result<Channel> {
        {
            let guard = lock(slot);
            if let Some(channel) = guard.as_ref()
                && channel.status().connected()
            {
                return Ok(channel.clone());
            }
        }
        let channel = self.connection.create_channel().await.map_err(map_lapin)?;
        *lock(slot) = Some(channel.clone());
        Ok(channel)
    }

    async fn control_channel(&self) -> Result<Channel> {
        self.channel_from(&self.control_channel).await
    }

    async fn publish_channel(&self) -> Result<Channel> {
        {
            let guard = lock(&self.publish_channel);
            if let Some(channel) = guard.as_ref()
                && channel.status().connected()
            {
                return Ok(channel.clone());
            }
        }
        let channel = self.connection.create_channel().await.map_err(map_lapin)?;
        if self.confirm_publish {
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await
                .map_err(map_lapin)?;
        }
        *lock(&self.publish_channel) = Some(channel.clone());
        Ok(channel)
    }

    async fn get_channel_state(&self) -> Result<Arc<ChannelState>> {
        {
            let guard = lock(&self.get_channel);
            if let Some(state) = guard.as_ref()
                && state.channel.status().connected()
            {
                return Ok(Arc::clone(state));
            }
        }
        let channel = self.connection.create_channel().await.map_err(map_lapin)?;
        let state = Arc::new(ChannelState::new(channel));
        self.register(&state);
        *lock(&self.get_channel) = Some(Arc::clone(&state));
        Ok(state)
    }

    fn wake_streams(&self) {
        let wakers: Vec<Waker> = {
            let streams = lock(&self.streams);
            streams
                .values()
                .filter_map(|state| lock(&state.waker).take())
                .collect()
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

/// Trasporto AMQP 0.9.1.
pub struct AmqpTransport {
    shared: Arc<Shared>,
}

impl AmqpTransport {
    /// Connette al broker. `retry` è la politica di Celery per il tentativo
    /// iniziale (le riconnessioni successive sono del worker, che crea un
    /// nuovo trasporto).
    pub async fn connect(
        url: &Url,
        options: TransportOptions,
        retry: &RetryPolicy,
    ) -> Result<Self> {
        let info = url.info();
        let secure = url.kind == TransportKind::Amqps;
        let mut query = AMQPQueryString::default();
        if let Some(heartbeat) = url.query_param("heartbeat") {
            query.heartbeat = heartbeat.parse::<u16>().ok();
        }
        if let Some(timeout) = url.query_param("connection_timeout") {
            query.connection_timeout = timeout.parse::<u64>().ok();
        }
        let uri = AMQPUri {
            scheme: if secure {
                AMQPScheme::AMQPS
            } else {
                AMQPScheme::AMQP
            },
            authority: AMQPAuthority {
                userinfo: AMQPUserInfo {
                    username: info.userid.as_deref().unwrap_or("guest").to_string(),
                    password: info.password.as_deref().unwrap_or("guest").to_string(),
                },
                host: info.hostname.to_string(),
                port: info.port.unwrap_or(if secure { 5671 } else { 5672 }),
            },
            vhost: info.virtual_host.to_string(),
            query,
        };
        let connection_name = options
            .extra
            .get_str("connection_name")
            .map(str::to_string)
            .unwrap_or_else(|| format!("celery-rs-{}", uuid::Uuid::new_v4()));
        let properties = ConnectionProperties::default()
            .with_connection_name(LongString::from(connection_name.into_bytes()));
        let mut attempt = 0_u32;
        let connection = loop {
            match Connection::connect_uri(uri.clone(), properties.clone()).await {
                Ok(connection) => break connection,
                Err(error) => {
                    if !retry.should_retry(attempt) {
                        return Err(Error::Connection(error.to_string()));
                    }
                    tokio::time::sleep(retry.delay(attempt)).await;
                    attempt = attempt.saturating_add(1);
                }
            }
        };
        let confirm_publish = options.extra.get_bool("confirm_publish").unwrap_or(false);
        let qos_global = options
            .extra
            .get_bool("qos_global")
            .or_else(|| url.query_param("qos_global").and_then(parse_bool))
            .unwrap_or(false);
        Ok(Self {
            shared: Arc::new(Shared {
                connection,
                confirm_publish,
                qos_global,
                closed: AtomicBool::new(false),
                control_channel: Mutex::new(None),
                publish_channel: Mutex::new(None),
                get_channel: Mutex::new(None),
                channels: Mutex::new(HashMap::new()),
                streams: Mutex::new(HashMap::new()),
                next_stream: AtomicU64::new(0),
            }),
        })
    }

    async fn declare_queue_state(&self, declaration: &QueueDecl) -> Result<QueueInfo> {
        let channel = self.shared.control_channel().await?;
        let name = declaration.name.as_deref().unwrap_or("");
        let options = QueueDeclareOptions {
            passive: declaration.passive,
            durable: declaration.durable,
            exclusive: declaration.exclusive,
            auto_delete: declaration.auto_delete,
            nowait: false,
        };
        let arguments = to_field_table(&declaration.arguments)?;
        match channel
            .queue_declare(short_string(name)?, options, arguments)
            .await
        {
            Ok(queue) => Ok(QueueInfo {
                name: CompactString::from(queue.name().as_str()),
                message_count: u64::from(queue.message_count()),
            }),
            Err(error) if declaration.passive && is_protocol_id(&error, 404) => {
                Err(Error::NotFound(error.to_string()))
            }
            Err(error) => Err(map_lapin(error)),
        }
    }

    async fn delete_queue_inner(&self, queue: &str) -> Result<()> {
        let channel = self.shared.control_channel().await?;
        channel
            .queue_delete(short_string(queue)?, QueueDeleteOptions::default())
            .await
            .map_err(map_lapin)?;
        Ok(())
    }
}

impl Transport for AmqpTransport {
    fn declare_exchange<'a>(&'a self, declaration: &'a ExchangeDecl) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            if declaration.name.is_empty() {
                // L'exchange anonimo esiste sempre e non si dichiara.
                return Ok(());
            }
            let channel = self.shared.control_channel().await?;
            let kind = match declaration.kind {
                ExchangeType::Direct => ExchangeKind::Direct,
                ExchangeType::Topic => ExchangeKind::Topic,
                ExchangeType::Fanout => ExchangeKind::Fanout,
                ExchangeType::Headers => ExchangeKind::Headers,
            };
            let options = ExchangeDeclareOptions {
                passive: declaration.passive,
                durable: declaration.durable,
                auto_delete: declaration.auto_delete,
                internal: false,
                nowait: false,
            };
            let arguments = to_field_table(&declaration.arguments)?;
            match channel
                .exchange_declare(short_string(&declaration.name)?, kind, options, arguments)
                .await
            {
                Ok(()) => Ok(()),
                Err(error) if declaration.passive && is_protocol_id(&error, 404) => {
                    Err(Error::NotFound(error.to_string()))
                }
                Err(error) if is_protocol_id(&error, 406) => {
                    Err(Error::ExchangeNotEquivalent(declaration.name.to_string()))
                }
                Err(error) => Err(map_lapin(error)),
            }
        })
    }

    fn declare_queue<'a>(&'a self, declaration: &'a QueueDecl) -> BoxFuture<'a, Result<QueueInfo>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            self.declare_queue_state(declaration).await
        })
    }

    fn bind<'a>(&'a self, binding: &'a QueueBinding) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            let channel = self.shared.control_channel().await?;
            let arguments = to_field_table(&binding.arguments)?;
            channel
                .queue_bind(
                    short_string(&binding.queue)?,
                    short_string(&binding.exchange)?,
                    short_string(&binding.routing_key)?,
                    QueueBindOptions::default(),
                    arguments,
                )
                .await
                .map_err(|error| map_bind_error(error, &binding.exchange))?;
            Ok(())
        })
    }

    fn unbind<'a>(&'a self, binding: &'a QueueBinding) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            let channel = self.shared.control_channel().await?;
            match channel
                .queue_unbind(
                    short_string(&binding.queue)?,
                    short_string(&binding.exchange)?,
                    short_string(&binding.routing_key)?,
                    FieldTable::default(),
                )
                .await
            {
                Ok(()) => {}
                // Queue/exchange assenti: come `queue_unbind` di kombu è no-op.
                Err(error) if is_protocol_id(&error, 404) => {}
                Err(error) => return Err(map_lapin(error)),
            }
            Ok(())
        })
    }

    fn delete_exchange<'a>(&'a self, exchange: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            // AMQP puro: `exchange.delete(if_unused=false)` elimina l'exchange
            // e i binding; le code restano (a differenza dei trasporti virtual).
            let channel = self.shared.control_channel().await?;
            channel
                .exchange_delete(short_string(exchange)?, ExchangeDeleteOptions::default())
                .await
                .map_err(map_lapin)?;
            Ok(())
        })
    }

    fn publish<'a>(&'a self, request: PublishRequest) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            let channel = self.shared.publish_channel().await?;
            let exchange = request.exchange.as_deref().unwrap_or("");
            let properties = message_to_properties(&request.message)?;
            let confirm = channel
                .basic_publish(
                    short_string(exchange)?,
                    short_string(&request.routing_key)?,
                    BasicPublishOptions::default(),
                    &request.message.body,
                    properties,
                )
                .await
                .map_err(map_lapin)?;
            if self.shared.confirm_publish {
                match confirm.await.map_err(map_lapin)? {
                    Confirmation::Ack(_) | Confirmation::NotRequested => {}
                    Confirmation::Nack(message) => {
                        return Err(Error::Channel(format!(
                            "publish non confermato dal broker: {message:?}"
                        )));
                    }
                }
            }
            Ok(())
        })
    }

    fn publish_batch<'a>(&'a self, requests: Vec<PublishRequest>) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            for request in requests {
                self.publish(request).await?;
            }
            Ok(())
        })
    }

    fn consume<'a>(
        &'a self,
        queues: &'a [CompactString],
        prefetch: u16,
    ) -> BoxFuture<'a, Result<Consumer>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            if queues.is_empty() {
                return Err(Error::InvalidArgument(
                    "consume richiede almeno una coda".to_string(),
                ));
            }
            let channel = self
                .shared
                .connection
                .create_channel()
                .await
                .map_err(map_lapin)?;
            if prefetch > 0 {
                channel
                    .basic_qos(
                        prefetch,
                        BasicQosOptions {
                            global: self.shared.qos_global,
                        },
                    )
                    .await
                    .map_err(map_lapin)?;
            }
            let state = Arc::new(ChannelState::new(channel));
            self.shared.register(&state);
            let mut entries = Vec::with_capacity(queues.len());
            let mut unique = Vec::with_capacity(queues.len());
            for queue in queues {
                if unique.contains(queue) {
                    continue;
                }
                let consumer = state
                    .channel
                    .basic_consume(
                        short_string(queue)?,
                        ShortString::from(""),
                        BasicConsumeOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                    .map_err(map_lapin)?;
                entries.push(ConsumerEntry {
                    queue: queue.clone(),
                    tag: consumer.tag(),
                    deliveries: Box::pin(consumer),
                });
                unique.push(queue.clone());
            }
            let stream_id = self.shared.next_stream.fetch_add(1, Ordering::Relaxed);
            let stream_state = Arc::new(StreamState {
                cancelled: AtomicBool::new(false),
                waker: Mutex::new(None),
            });
            lock(&self.shared.streams).insert(stream_id, Arc::clone(&stream_state));
            let set = Arc::new(Mutex::new(ConsumerSet {
                entries,
                queues: unique,
                closed: false,
            }));
            let deliveries = AmqpDeliveryStream {
                shared: Arc::clone(&self.shared),
                stream_id,
                state: Arc::clone(&stream_state),
                channel: Arc::clone(&state),
                set: Arc::clone(&set),
                cursor: 0,
                health: Box::pin(tokio::time::sleep(HEALTH_CHECK)),
                lost_reported: false,
                finished: false,
            };
            Ok(Consumer {
                deliveries: Box::pin(deliveries) as DeliveryStream,
                control: Arc::new(AmqpConsumerControl {
                    state: stream_state,
                    channel: state,
                    set,
                    qos_global: self.shared.qos_global,
                }),
            })
        })
    }

    fn get<'a>(&'a self, queue: &'a str, no_ack: bool) -> BoxFuture<'a, Result<Option<Delivery>>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            let state = self.shared.get_channel_state().await?;
            let options = BasicGetOptions { no_ack };
            let message = state.channel.basic_get(short_string(queue)?, options).await;
            let message = match message {
                Ok(message) => message,
                // Coda inesistente: come il trasporto memory non c'è nulla.
                Err(error) if is_protocol_id(&error, 404) => return Ok(None),
                Err(error) => return Err(map_lapin(error)),
            };
            let Some(message) = message else {
                return Ok(None);
            };
            let delivery = message.delivery;
            let raw_tag = delivery.delivery_tag;
            if !no_ack {
                lock(&state.in_flight).insert(raw_tag);
            }
            Ok(Some(delivery_from_amqp(&state, delivery)))
        })
    }

    fn ack<'a>(&'a self, delivery_tag: &'a DeliveryTag) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            let Some((channel_id, raw_tag)) = decode_tag(delivery_tag) else {
                return Ok(());
            };
            let state = {
                let channels = lock(&self.shared.channels);
                channels.get(&channel_id).and_then(Weak::upgrade)
            };
            let Some(state) = state else {
                return Ok(());
            };
            if !lock(&state.in_flight).remove(&raw_tag) {
                return Ok(());
            }
            state
                .channel
                .basic_ack(raw_tag, BasicAckOptions::default())
                .await
                .map_err(map_lapin)
        })
    }

    fn reject<'a>(
        &'a self,
        delivery_tag: &'a DeliveryTag,
        requeue: bool,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            let Some((channel_id, raw_tag)) = decode_tag(delivery_tag) else {
                return if requeue {
                    Err(Error::UnknownDeliveryTag(delivery_tag.to_string()))
                } else {
                    Ok(())
                };
            };
            let state = {
                let channels = lock(&self.shared.channels);
                channels.get(&channel_id).and_then(Weak::upgrade)
            };
            let Some(state) = state else {
                return if requeue {
                    Err(Error::UnknownDeliveryTag(delivery_tag.to_string()))
                } else {
                    Ok(())
                };
            };
            if !lock(&state.in_flight).remove(&raw_tag) {
                return if requeue {
                    Err(Error::UnknownDeliveryTag(delivery_tag.to_string()))
                } else {
                    Ok(())
                };
            }
            let options = BasicRejectOptions { requeue };
            state
                .channel
                .basic_reject(raw_tag, options)
                .await
                .map_err(map_lapin)
        })
    }

    fn purge<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            let channel = self.shared.control_channel().await?;
            match channel
                .queue_purge(short_string(queue)?, QueuePurgeOptions::default())
                .await
            {
                Ok(purged) => Ok(u64::from(purged)),
                // Coda inesistente: come il trasporto memory non c'è nulla da svuotare.
                Err(error) if is_protocol_id(&error, 404) => Ok(0),
                Err(error) => Err(map_lapin(error)),
            }
        })
    }

    fn queue_size<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            let declaration = QueueDecl {
                name: Some(CompactString::from(queue)),
                passive: true,
                ..QueueDecl::default()
            };
            match self.declare_queue_state(&declaration).await {
                Ok(info) => Ok(info.message_count),
                Err(Error::NotFound(_)) => Ok(0),
                Err(error) => Err(error),
            }
        })
    }

    fn delete_queue<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            self.delete_queue_inner(queue).await?;
            Ok(())
        })
    }

    fn close<'a>(&'a self) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if self.shared.closed.swap(true, Ordering::AcqRel) {
                return Ok(());
            }
            self.shared.wake_streams();
            *lock(&self.shared.get_channel) = None;
            *lock(&self.shared.control_channel) = None;
            *lock(&self.shared.publish_channel) = None;
            // Se la connessione è già caduta non c'è nulla da chiudere.
            let _ = self
                .shared
                .connection
                .close(200, ShortString::from("Goodbye"))
                .await;
            Ok(())
        })
    }
}

fn delivery_from_amqp(state: &ChannelState, delivery: LapinDelivery) -> Delivery {
    let LapinDelivery {
        delivery_tag,
        exchange,
        routing_key,
        redelivered,
        properties,
        data,
        ..
    } = delivery;
    Delivery {
        message: message_from_amqp(data, &properties),
        delivery_tag: encode_tag(state.id, delivery_tag),
        exchange: CompactString::from(exchange.as_str()),
        routing_key: CompactString::from(routing_key.as_str()),
        redelivered,
    }
}

struct StreamState {
    cancelled: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

struct ConsumerEntry {
    queue: CompactString,
    tag: ShortString,
    deliveries: Pin<Box<LapinConsumer>>,
}

struct ConsumerSet {
    entries: Vec<ConsumerEntry>,
    queues: Vec<CompactString>,
    closed: bool,
}

/// Svuota i messaggi già nel buffer locale di un consumatore senza attenderne
/// di nuovi. Le consegne restituite sono già state inviate dal broker.
fn drain_buffered(consumer: &mut Pin<Box<LapinConsumer>>, out: &mut Vec<LapinDelivery>) {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    loop {
        match consumer.as_mut().poll_next(&mut context) {
            Poll::Ready(Some(Ok(delivery))) => out.push(delivery),
            Poll::Ready(Some(Err(_))) | Poll::Ready(None) => break,
            Poll::Pending => break,
        }
    }
}

/// Controllo di un consumatore AMQP: agisce sul canale dedicato.
struct AmqpConsumerControl {
    state: Arc<StreamState>,
    channel: Arc<ChannelState>,
    set: Arc<Mutex<ConsumerSet>>,
    qos_global: bool,
}

impl AmqpConsumerControl {
    fn is_closed(&self) -> bool {
        lock(&self.set).closed
    }

    fn wake(&self) {
        if let Some(waker) = lock(&self.state.waker).take() {
            waker.wake();
        }
    }
}

impl ConsumerControl for AmqpConsumerControl {
    fn set_prefetch<'a>(&'a self, prefetch: u16) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if self.is_closed() {
                return Err(Error::Closed);
            }
            // Stessa chiamata di Celery (`task_consumer.qos(apply_global=...)`):
            // nessuna ricreazione dei consumatori. Con RabbitMQ < 3.3 o 4.x le
            // nuove impostazioni si applicano ai consumatori creati dopo, come
            // documentato dal broker; con code quorum il global QoS non è
            // supportato e il limite resta per-consumer (default `qos_global`
            // false, vedi `qos_global` nelle opzioni del trasporto).
            self.channel
                .channel
                .basic_qos(
                    prefetch,
                    BasicQosOptions {
                        global: self.qos_global,
                    },
                )
                .await
                .map_err(map_lapin)?;
            let set = lock(&self.set);
            if set.closed {
                return Err(Error::Closed);
            }
            Ok(())
        })
    }

    fn add_queue<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            {
                let set = lock(&self.set);
                if set.closed {
                    return Err(Error::Closed);
                }
                if set.queues.iter().any(|known| known == queue) {
                    return Ok(());
                }
            }
            let consumer = self
                .channel
                .channel
                .basic_consume(
                    short_string(queue)?,
                    ShortString::from(""),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(map_lapin)?;
            let tag = consumer.tag();
            let mut set = lock(&self.set);
            if set.closed {
                // Il consumatore appena creato viene cancellato dal Drop.
                return Err(Error::Closed);
            }
            set.entries.push(ConsumerEntry {
                queue: CompactString::from(queue),
                tag,
                deliveries: Box::pin(consumer),
            });
            set.queues.push(CompactString::from(queue));
            drop(set);
            self.wake();
            Ok(())
        })
    }

    fn cancel_queue<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let entry = {
                let mut set = lock(&self.set);
                if set.closed {
                    return Err(Error::Closed);
                }
                let position = set.entries.iter().position(|entry| entry.queue == queue);
                position.map(|position| set.entries.remove(position))
            };
            let Some(mut entry) = entry else {
                return Ok(());
            };
            self.channel
                .channel
                .basic_cancel(entry.tag.clone(), BasicCancelOptions::default())
                .await
                .map_err(map_lapin)?;
            // Le consegne mai restituite all'applicazione tornano in coda.
            let mut buffered = Vec::new();
            drain_buffered(&mut entry.deliveries, &mut buffered);
            for delivery in buffered {
                let _ = self
                    .channel
                    .channel
                    .basic_reject(delivery.delivery_tag, BasicRejectOptions { requeue: true })
                    .await;
            }
            let mut set = lock(&self.set);
            set.queues.retain(|known| known != queue);
            drop(set);
            self.wake();
            Ok(())
        })
    }

    fn cancel<'a>(&'a self) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let entries: Vec<ConsumerEntry> = {
                let mut set = lock(&self.set);
                if set.closed {
                    return Ok(());
                }
                set.closed = true;
                std::mem::take(&mut set.entries)
            };
            self.state.cancelled.store(true, Ordering::Release);
            self.wake();
            // Come `basic_cancel`: i messaggi già consegnati restano
            // confermabili sul canale, che resta aperto. Quelli rimasti nel
            // buffer locale non sono mai stati visti dall'app: tornano in coda.
            for mut entry in entries {
                let _ = self
                    .channel
                    .channel
                    .basic_cancel(entry.tag.clone(), BasicCancelOptions::default())
                    .await;
                let mut buffered = Vec::new();
                drain_buffered(&mut entry.deliveries, &mut buffered);
                for delivery in buffered {
                    let _ = self
                        .channel
                        .channel
                        .basic_reject(delivery.delivery_tag, BasicRejectOptions { requeue: true })
                        .await;
                }
            }
            Ok(())
        })
    }
}

/// Periodo del controllo di salute del canale quando lo stream è in attesa:
/// lapin non risveglia i consumatori quando la connessione cade sotto di loro.
const HEALTH_CHECK: Duration = Duration::from_millis(250);

/// Stream di consegne AMQP. Termina con `None` dopo `cancel` o dopo una
/// `ConnectionLost` (emessa una sola volta).
struct AmqpDeliveryStream {
    shared: Arc<Shared>,
    stream_id: u64,
    state: Arc<StreamState>,
    channel: Arc<ChannelState>,
    set: Arc<Mutex<ConsumerSet>>,
    cursor: usize,
    health: Pin<Box<tokio::time::Sleep>>,
    lost_reported: bool,
    finished: bool,
}

impl AmqpDeliveryStream {
    fn finished_with_lost(&mut self, detail: &str) -> Option<Result<Delivery>> {
        self.finished = true;
        self.lost_reported = true;
        Some(Err(Error::ConnectionLost(detail.to_string())))
    }
}

impl Stream for AmqpDeliveryStream {
    type Item = Result<Delivery>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        *lock(&this.state.waker) = Some(cx.waker().clone());
        if this.state.cancelled.load(Ordering::Acquire) {
            this.finished = true;
            return Poll::Ready(None);
        }
        if this.shared.closed.load(Ordering::Acquire) {
            return Poll::Ready(this.finished_with_lost("trasporto AMQP chiuso"));
        }
        // lapin non propaga ai consumatori la chiusura della connessione
        // decisa dal broker: il timer ci sveglia e controlliamo lo stato.
        if this.health.as_mut().poll(cx).is_ready() {
            this.health
                .as_mut()
                .reset(tokio::time::Instant::now() + HEALTH_CHECK);
            if !this.shared.connection.status().connected()
                || !this.channel.channel.status().connected()
            {
                return Poll::Ready(this.finished_with_lost("connessione AMQP al broker caduta"));
            }
        }

        let mut set = lock(&this.set);
        let total = set.entries.len();
        if total == 0 {
            return Poll::Pending;
        }
        for offset in 0..total {
            let index = (this.cursor + offset) % total;
            match set.entries[index].deliveries.as_mut().poll_next(cx) {
                Poll::Pending => {}
                Poll::Ready(Some(Ok(delivery))) => {
                    this.cursor = (index + 1) % total;
                    let raw_tag = delivery.delivery_tag;
                    lock(&this.channel.in_flight).insert(raw_tag);
                    return Poll::Ready(Some(Ok(delivery_from_amqp(&this.channel, delivery))));
                }
                Poll::Ready(Some(Err(error))) => {
                    drop(set);
                    return Poll::Ready(this.finished_with_lost(&error.to_string()));
                }
                Poll::Ready(None) => {
                    drop(set);
                    if this.state.cancelled.load(Ordering::Acquire) {
                        this.finished = true;
                        return Poll::Ready(None);
                    }
                    return Poll::Ready(this.finished_with_lost("consumatore AMQP terminato"));
                }
            }
        }
        Poll::Pending
    }
}

impl Drop for AmqpDeliveryStream {
    fn drop(&mut self) {
        lock(&self.shared.streams).remove(&self.stream_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_tags_encode_channel_and_counter() {
        let tag = encode_tag(7, 42);
        assert_eq!(tag, DeliveryTag::Number((7 << TAG_SHIFT) | 42));
        assert_eq!(decode_tag(&tag), Some((7, 42)));
        assert_eq!(decode_tag(&DeliveryTag::from("uuid")), None);
    }

    #[test]
    fn encoded_tag_stays_in_i64_range_for_max_channel() {
        let tag = encode_tag(u16::MAX, u64::MAX);
        let value = tag.as_u64().expect("numero");
        assert!(i64::try_from(value).is_ok());
        assert_eq!(decode_tag(&tag), Some((u16::MAX, TAG_MASK)));
    }
}
