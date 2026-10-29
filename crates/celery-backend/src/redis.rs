//! Redis commands, pipelines and dedicated pub/sub for the result backend. Values
//! remain Bytes until the Python boundary; pooled connections can be reset after fork.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ::redis::aio::{MultiplexedConnection, PubSubSink, PubSubStream};
use ::redis::{AsyncConnectionConfig, IntoConnectionInfo, Value};
use bytes::Bytes;
use futures_core::Stream;

// Bounds pub/sub memory when callers stop reading while results continue arriving.
const PUBSUB_QUEUE_LIMIT: usize = 1024;

const POOL_LIMIT: usize = 64;

/// Redis server address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Address {
    Tcp {
        host: String,
        port: u16,
    },
    Tls {
        host: String,
        port: u16,
        insecure: bool,
    },
    Unix(PathBuf),
}

/// Connection settings resolved by the Python backend.
#[derive(Clone, Debug)]
pub struct Config {
    pub address: Address,
    pub db: i64,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Maximum response wait, matching redis-py's socket_timeout.
    pub response_timeout: Option<Duration>,
    /// Maximum time to establish a connection.
    pub connect_timeout: Option<Duration>,
    /// Client name sent with CLIENT SETNAME on connection.
    pub client_name: Option<String>,
    /// Pool connection limit; None uses redis-py's default.
    pub max_connections: Option<usize>,
    /// Idle interval after which a PING precedes the next command.
    pub health_check_interval: Option<Duration>,
}

/// Client error mapped to redis-py exceptions by the binding.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Connection(String),
    #[error("{0}")]
    Timeout(String),
    #[error("{0}")]
    Response(String),
    #[error("{0}")]
    Authentication(String),
    #[error("{0}")]
    Invalid(String),
}

impl Error {
    fn from_redis(error: ::redis::RedisError) -> Self {
        use ::redis::ErrorKind;
        let message = error.to_string();
        match error.kind() {
            ErrorKind::AuthenticationFailed => Error::Authentication(message),
            ErrorKind::Io if error.is_timeout() => Error::Timeout(message),
            ErrorKind::Io => Error::Connection(message),
            ErrorKind::InvalidClientConfig | ErrorKind::Client => Error::Invalid(message),
            _ => Error::Response(message),
        }
    }
}

/// Command result using redis-py value types.
#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    Nil,
    Int(i64),
    Bool(bool),
    Bytes(Bytes),
    Array(Vec<Reply>),
}

impl Reply {
    fn from_value(value: Value) -> Self {
        match value {
            Value::Nil => Reply::Nil,
            Value::Int(number) => Reply::Int(number),
            Value::BulkString(data) => Reply::Bytes(Bytes::from(data)),
            Value::SimpleString(text) => Reply::Bytes(Bytes::from(text.into_bytes())),
            Value::Okay => Reply::Bool(true),
            Value::Array(items) => Reply::Array(items.into_iter().map(Reply::from_value).collect()),
            Value::Map(pairs) => Reply::Array(
                pairs
                    .into_iter()
                    .flat_map(|(key, value)| [Reply::from_value(key), Reply::from_value(value)])
                    .collect(),
            ),
            Value::Attribute { data, .. } => Reply::from_value(*data),
            Value::Set(items) => Reply::Array(items.into_iter().map(Reply::from_value).collect()),
            Value::Double(value) => Reply::Bytes(Bytes::from(format!("{value}"))),
            Value::Boolean(value) => Reply::Bool(value),
            Value::BigNumber(_) | Value::VerbatimString { .. } | Value::Push { .. } => {
                // RESP2 backend commands never produce these types; report rather than discard them.
                Reply::Nil
            }
            Value::ServerError(error) => Reply::Bytes(Bytes::from(error.to_string())),
            _ => Reply::Nil,
        }
    }
}

/// Backend command subset. Python formats numeric arguments using repr to preserve wire data.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Get {
        key: Bytes,
    },
    MGet {
        keys: Vec<Bytes>,
    },
    Set {
        key: Bytes,
        value: Bytes,
        ex: Option<u64>,
    },
    SetEx {
        key: Bytes,
        seconds: u64,
        value: Bytes,
    },
    Delete {
        keys: Vec<Bytes>,
    },
    Incr {
        key: Bytes,
        amount: Option<i64>,
    },
    Expire {
        key: Bytes,
        seconds: i64,
    },
    Ttl {
        key: Bytes,
    },
    Publish {
        channel: Bytes,
        message: Bytes,
    },
    ZAdd {
        key: Bytes,
        pairs: Vec<(Bytes, Bytes)>,
    },
    ZCount {
        key: Bytes,
        min: Bytes,
        max: Bytes,
    },
    ZRange {
        key: Bytes,
        start: i64,
        stop: i64,
    },
    RPush {
        key: Bytes,
        values: Vec<Bytes>,
    },
    LLen {
        key: Bytes,
    },
    LRange {
        key: Bytes,
        start: i64,
        stop: i64,
    },
}

impl Command {
    fn has_large_payload(&self) -> bool {
        let payload = match self {
            Self::Set { value, .. } | Self::SetEx { value, .. } => value,
            Self::Publish { message, .. } => message,
            _ => return false,
        };
        payload.len() >= 1024 * 1024
    }

    // Expected response type for redis-py-compatible conversion.
    fn is_set(&self) -> bool {
        matches!(self, Command::Set { .. } | Command::SetEx { .. })
    }

    fn is_bool(&self) -> bool {
        matches!(self, Command::Expire { .. })
    }

    fn is_array(&self) -> bool {
        matches!(
            self,
            Command::MGet { .. } | Command::ZRange { .. } | Command::LRange { .. }
        )
    }

    fn is_nilable(&self) -> bool {
        matches!(self, Command::Get { .. })
    }

    fn append(&self, pipe: &mut ::redis::Pipeline) {
        match self {
            Command::Get { key } => {
                pipe.cmd("GET").arg(key.as_ref());
            }
            Command::MGet { keys } => {
                pipe.cmd("MGET")
                    .arg(keys.iter().map(Bytes::as_ref).collect::<Vec<_>>());
            }
            Command::Set { key, value, ex } => {
                pipe.cmd("SET").arg(key.as_ref()).arg(value.as_ref());
                if let Some(ex) = ex {
                    // redis-py's set(ex=) sends SET EX; SETEX is a separate command.
                    pipe.arg("EX").arg(*ex);
                }
            }
            Command::SetEx {
                key,
                seconds,
                value,
            } => {
                pipe.cmd("SETEX")
                    .arg(key.as_ref())
                    .arg(*seconds)
                    .arg(value.as_ref());
            }
            Command::Delete { keys } => {
                pipe.cmd("DEL")
                    .arg(keys.iter().map(Bytes::as_ref).collect::<Vec<_>>());
            }
            Command::Incr { key, amount } => {
                // redis-py 8 aliases incr to incrby, so incr(key) sends INCRBY key 1.
                pipe.cmd("INCRBY")
                    .arg(key.as_ref())
                    .arg(amount.unwrap_or(1));
            }
            Command::Expire { key, seconds } => {
                pipe.cmd("EXPIRE").arg(key.as_ref()).arg(*seconds);
            }
            Command::Ttl { key } => {
                pipe.cmd("TTL").arg(key.as_ref());
            }
            Command::Publish { channel, message } => {
                pipe.cmd("PUBLISH")
                    .arg(channel.as_ref())
                    .arg(message.as_ref());
            }
            Command::ZAdd { key, pairs } => {
                pipe.cmd("ZADD").arg(key.as_ref());
                for (score, member) in pairs {
                    pipe.arg(score.as_ref()).arg(member.as_ref());
                }
            }
            Command::ZCount { key, min, max } => {
                pipe.cmd("ZCOUNT")
                    .arg(key.as_ref())
                    .arg(min.as_ref())
                    .arg(max.as_ref());
            }
            Command::ZRange { key, start, stop } => {
                pipe.cmd("ZRANGE").arg(key.as_ref()).arg(*start).arg(*stop);
            }
            Command::RPush { key, values } => {
                pipe.cmd("RPUSH")
                    .arg(key.as_ref())
                    .arg(values.iter().map(Bytes::as_ref).collect::<Vec<_>>());
            }
            Command::LLen { key } => {
                pipe.cmd("LLEN").arg(key.as_ref());
            }
            Command::LRange { key, start, stop } => {
                pipe.cmd("LRANGE").arg(key.as_ref()).arg(*start).arg(*stop);
            }
        }
    }

    fn decode(&self, value: Value) -> Reply {
        if self.is_set() {
            return Reply::Bool(!matches!(value, Value::Nil));
        }
        if self.is_bool() {
            return match Reply::from_value(value) {
                Reply::Int(number) => Reply::Bool(number != 0),
                Reply::Nil => Reply::Bool(false),
                other => other,
            };
        }
        if self.is_array() {
            return match Reply::from_value(value) {
                Reply::Array(items) => Reply::Array(items),
                Reply::Nil => Reply::Array(Vec::new()),
                other => Reply::Array(vec![other]),
            };
        }
        if self.is_nilable() {
            return Reply::from_value(value);
        }
        Reply::from_value(value)
    }
}

/// Command pipeline executed in one round trip.
#[derive(Debug, Clone, Default)]
pub struct Pipeline {
    pub transaction: bool,
    pub commands: Vec<Command>,
}

impl Pipeline {
    pub fn new(transaction: bool) -> Self {
        Self {
            transaction,
            commands: Vec::new(),
        }
    }

    pub fn push(&mut self, command: Command) {
        self.commands.push(command);
    }

    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    pub fn clear(&mut self) {
        self.commands.clear();
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
thread_local! {
    static RECLAIM_PENDING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static LARGE_TASK_IO: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn release_scratch_pages(large: bool) {
    // glibc retains large temporary RESP buffers after Drop. Release them only after
    // the inner future, commands and packed pipelines have been destroyed.
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        if large {
            LARGE_TASK_IO.set(true);
        }
        if RECLAIM_PENDING.replace(large) || large {
            // Also reclaim Python copies freed after the previous command.
            trim_free_pages();
        }
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    let _ = large;
}

/// Releases temporary pages after a child finishes a large task.
pub fn release_task_memory() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    if LARGE_TASK_IO.replace(false) {
        RECLAIM_PENDING.set(false);
        trim_free_pages();
    }
}

/// Releases free pages before fork or after child warmup.
pub fn release_idle_memory() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    trim_free_pages();
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn trim_free_pages() {
    // SAFETY: malloc_trim is thread-safe, retains no pointers and touches only free allocator pages.
    unsafe {
        libc::malloc_trim(0);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Redis client with a multiplexed connection pool.
pub struct Client {
    config: Config,
    redis: ::redis::Client,
    connection_config: AsyncConnectionConfig,
    idle: Mutex<Vec<MultiplexedConnection>>,
    last_activity: Mutex<Instant>,
    pid: AtomicU32,
}

impl Client {
    /// Creates a client without opening sockets, matching redis-py.
    pub fn open(config: Config) -> Result<Self, Error> {
        install_crypto_provider_if_needed(&config.address);
        let mut redis_info = ::redis::RedisConnectionInfo::default().set_db(config.db);
        if let Some(username) = config.username.as_deref() {
            redis_info = redis_info.set_username(username);
        }
        if let Some(password) = config.password.as_deref() {
            redis_info = redis_info.set_password(password);
        }
        let address = match &config.address {
            Address::Tcp { host, port } => ::redis::ConnectionAddr::Tcp(host.clone(), *port),
            Address::Tls {
                host,
                port,
                insecure,
            } => ::redis::ConnectionAddr::TcpTls {
                host: host.clone(),
                port: *port,
                insecure: *insecure,
                tls_params: None,
            },
            Address::Unix(path) => ::redis::ConnectionAddr::Unix(path.clone()),
        };
        let info = address
            .into_connection_info()
            .map_err(|error| Error::Invalid(error.to_string()))?
            .set_redis_settings(redis_info);
        let redis =
            ::redis::Client::open(info).map_err(|error| Error::Invalid(error.to_string()))?;
        let mut connection_config = AsyncConnectionConfig::new();
        if let Some(timeout) = config.response_timeout {
            connection_config = connection_config.set_response_timeout(Some(
                Duration::from_secs_f64(timeout.as_secs_f64().max(0.001)),
            ));
        } else {
            // redis-py waits indefinitely without socket_timeout; redis-rs defaults to 500 ms.
            connection_config = connection_config.set_response_timeout(None);
        }
        if let Some(timeout) = config.connect_timeout {
            connection_config = connection_config.set_connection_timeout(Some(
                Duration::from_secs_f64(timeout.as_secs_f64().max(0.001)),
            ));
        }
        Ok(Self {
            config,
            redis,
            connection_config,
            idle: Mutex::new(Vec::new()),
            last_activity: Mutex::new(Instant::now()),
            pid: AtomicU32::new(std::process::id()),
        })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    // Abandons inherited connections on PID change. Dropping them would wake the
    // parent's absent tokio driver and abort the child. Their descriptors are already
    // closed, so the one-time leak is bounded to each child.
    fn ensure_process(&self) {
        let pid = std::process::id();
        if self.pid.load(Ordering::Relaxed) != pid {
            let stale: Vec<MultiplexedConnection> = std::mem::take(&mut *lock(&self.idle));
            for connection in stale {
                std::mem::forget(connection);
            }
            self.pid.store(pid, Ordering::Relaxed);
        }
    }

    async fn create_connection(&self) -> Result<MultiplexedConnection, Error> {
        let mut connection = self
            .redis
            .get_multiplexed_async_connection_with_config(&self.connection_config)
            .await
            .map_err(Error::from_redis)?;
        if let Some(name) = self.config.client_name.as_deref() {
            ::redis::cmd("CLIENT")
                .arg("SETNAME")
                .arg(name)
                .query_async::<()>(&mut connection)
                .await
                .map_err(Error::from_redis)?;
        }
        Ok(connection)
    }

    async fn acquire(&self) -> Result<MultiplexedConnection, Error> {
        self.ensure_process();
        if let Some(connection) = lock(&self.idle).pop() {
            return Ok(connection);
        }
        self.create_connection().await
    }

    // Pings idle connections before a command, matching redis-py's health check interval.
    async fn check_health(&self, connection: &mut MultiplexedConnection) -> Result<(), Error> {
        let Some(interval) = self.config.health_check_interval else {
            return Ok(());
        };
        let due = {
            let mut last = lock(&self.last_activity);
            let now = Instant::now();
            let due = now.duration_since(*last) >= interval;
            *last = now;
            due
        };
        if due {
            let _: Value = ::redis::cmd("PING")
                .query_async(connection)
                .await
                .map_err(Error::from_redis)?;
        }
        Ok(())
    }

    fn release(&self, connection: MultiplexedConnection) {
        let limit = self
            .config
            .max_connections
            .map_or(POOL_LIMIT, |limit| limit.clamp(1, POOL_LIMIT));
        let mut idle = lock(&self.idle);
        if idle.len() < limit {
            idle.push(connection);
        }
    }

    /// Discards idle connections, matching redis-py's pool reset.
    pub fn reset(&self) {
        lock(&self.idle).clear();
    }

    /// Executes a command on a pooled connection.
    pub async fn execute(&self, command: Command) -> Result<Reply, Error> {
        let large = command.has_large_payload();
        let reply = self.execute_inner(command).await;
        release_scratch_pages(large);
        reply
    }

    async fn execute_inner(&self, command: Command) -> Result<Reply, Error> {
        let mut connection = self.acquire().await?;
        self.check_health(&mut connection).await?;
        let mut pipeline = ::redis::pipe();
        command.append(&mut pipeline);
        let values: Vec<Value> = match pipeline.query_async(&mut connection).await {
            Ok(values) => values,
            Err(error) => return Err(Error::from_redis(error)),
        };
        self.release(connection);
        values
            .into_iter()
            .next()
            .map(|value| command.decode(value))
            .ok_or_else(|| Error::Response("empty Redis response".to_string()))
    }

    /// Executes a pipeline in one round trip, wrapping transactions in MULTI/EXEC.
    pub async fn execute_pipeline(&self, pipeline: Pipeline) -> Result<Vec<Reply>, Error> {
        let large = pipeline.commands.iter().any(Command::has_large_payload);
        let reply = self.execute_pipeline_inner(pipeline).await;
        release_scratch_pages(large);
        reply
    }

    async fn execute_pipeline_inner(&self, pipeline: Pipeline) -> Result<Vec<Reply>, Error> {
        if pipeline.commands.is_empty() {
            return Ok(Vec::new());
        }
        let mut connection = self.acquire().await?;
        self.check_health(&mut connection).await?;
        let mut packed = ::redis::pipe();
        if pipeline.transaction {
            packed.atomic();
        }
        for command in &pipeline.commands {
            command.append(&mut packed);
        }
        let values: Vec<Value> = match packed.query_async(&mut connection).await {
            Ok(values) => values,
            Err(error) => return Err(Error::from_redis(error)),
        };
        self.release(connection);
        if values.len() != pipeline.commands.len() {
            return Err(Error::Response(format!(
                "pipeline: expected {} results, got {}",
                pipeline.commands.len(),
                values.len()
            )));
        }
        Ok(pipeline
            .commands
            .iter()
            .zip(values)
            .map(|(command, value)| command.decode(value))
            .collect())
    }

    /// Opens a dedicated pub/sub connection.
    pub async fn pubsub(&self) -> Result<PubSubConnection, Error> {
        self.ensure_process();
        let pubsub = self
            .redis
            .get_async_pubsub()
            .await
            .map_err(Error::from_redis)?;
        let (sink, stream) = pubsub.split();
        Ok(PubSubConnection {
            sink: Mutex::new(sink),
            stream: Mutex::new(Some(stream)),
            queue: Mutex::new(VecDeque::new()),
            closed: AtomicBool::new(false),
            pid: AtomicU32::new(std::process::id()),
        })
    }
}

/// Pub/sub channel message; pattern subscriptions are not used by the backend.
#[derive(Debug, Clone, PartialEq)]
pub struct PubSubMessage {
    pub channel: String,
    pub payload: Bytes,
}

/// Pub/sub connection with a dedicated socket.
pub struct PubSubConnection {
    sink: Mutex<PubSubSink>,
    stream: Mutex<Option<PubSubStream>>,
    queue: Mutex<VecDeque<PubSubMessage>>,
    closed: AtomicBool,
    pid: AtomicU32,
}

impl PubSubConnection {
    fn ensure_open(&self) -> Result<(), Error> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(Error::Connection("pub/sub connection closed".to_string()));
        }
        let pid = std::process::id();
        if self.pid.load(Ordering::Relaxed) != pid {
            return Err(Error::Connection(
                "pub/sub connection inherited from another process".to_string(),
            ));
        }
        Ok(())
    }

    /// Queues SUBSCRIBE without waiting for its acknowledgment, matching redis-py.
    pub fn subscribe(&self, channels: &[String]) -> Result<(), Error> {
        self.ensure_open()?;
        if channels.is_empty() {
            return Ok(());
        }
        let mut sink = lock(&self.sink);
        poll_send(sink.subscribe(channels))
    }

    /// Waits for acknowledgment so the runtime sends the command before closure.
    pub async fn unsubscribe(&self, channels: &[String]) -> Result<(), Error> {
        self.ensure_open()?;
        if channels.is_empty() {
            return Ok(());
        }
        let mut sink = lock(&self.sink).clone();
        sink.unsubscribe(channels).await.map_err(Error::from_redis)
    }

    /// Closes the socket without sending commands, matching redis-py PubSub.close.
    pub fn close(&self) {
        if self.closed.swap(true, Ordering::Relaxed) {
            return;
        }
        lock(&self.queue).clear();

        lock(&self.stream).take();
    }

    fn pop_queued(&self) -> Option<PubSubMessage> {
        lock(&self.queue).pop_front()
    }

    // Waits up to the timeout for a message; None waits indefinitely.
    async fn read_once(&self, timeout: Option<Duration>) -> Result<Option<PubSubMessage>, Error> {
        self.ensure_open()?;
        if let Some(message) = self.pop_queued() {
            return Ok(Some(message));
        }
        let mut stream = lock(&self.stream).take();
        let Some(mut stream) = stream.take() else {
            return Ok(None);
        };
        let result: Option<Option<PubSubMessage>> = match timeout {
            None => Some(wait_message(&mut stream).await),
            Some(duration) if duration.is_zero() => poll_now(&mut stream),

            Some(duration) => tokio::time::timeout(duration, wait_message(&mut stream))
                .await
                .ok(),
        };
        if !self.closed.load(Ordering::Relaxed) {
            *lock(&self.stream) = Some(stream);
        }
        match result {
            None => Ok(None),
            Some(Some(message)) => Ok(Some(message)),
            // An unexpected stream end means server disconnection; raise a connection error
            // so Python's _reconnect_pubsub can reconnect.
            Some(None) => Err(Error::Connection(
                "pub/sub connection closed by server".to_string(),
            )),
        }
    }

    /// Returns an already available message, matching can_read(timeout=0).
    pub async fn can_read(&self, timeout: Option<Duration>) -> Result<bool, Error> {
        self.ensure_open()?;
        if !lock(&self.queue).is_empty() {
            return Ok(true);
        }
        let message = self.read_once(timeout).await?;
        if let Some(message) = message {
            self.buffer(message);
            return Ok(true);
        }
        Ok(false)
    }

    /// Gets a message with redis-py's timeout semantics.
    pub async fn get_message(
        &self,
        timeout: Option<Duration>,
    ) -> Result<Option<PubSubMessage>, Error> {
        self.read_once(timeout).await
    }

    fn buffer(&self, message: PubSubMessage) {
        let mut queue = lock(&self.queue);
        if queue.len() >= PUBSUB_QUEUE_LIMIT {
            // Bound memory by dropping the oldest message when the caller falls behind.
            queue.pop_front();
        }
        queue.push_back(message);
    }
}

// Queues a command without waiting for acknowledgment; Ready indicates rejection.
fn poll_send(
    future: impl std::future::Future<Output = ::redis::RedisResult<()>>,
) -> Result<(), Error> {
    let mut future = Box::pin(future);
    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(result) => result.map_err(Error::from_redis),
        Poll::Pending => Ok(()),
    }
}

async fn wait_message(stream: &mut PubSubStream) -> Option<PubSubMessage> {
    let message = std::future::poll_fn(|cx| Pin::new(&mut *stream).poll_next(cx)).await?;
    message_to_pubsub(message)
}

// Distinguishes pending, terminated and ready message states.
fn poll_now(stream: &mut PubSubStream) -> Option<Option<PubSubMessage>> {
    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);
    match Pin::new(stream).poll_next(&mut context) {
        Poll::Ready(Some(message)) => Some(message_to_pubsub(message)),
        Poll::Ready(None) => Some(None),
        Poll::Pending => None,
    }
}

fn message_to_pubsub(message: ::redis::Msg) -> Option<PubSubMessage> {
    let channel = message.get_channel_name().to_string();
    let payload = Bytes::copy_from_slice(message.get_payload_bytes());
    Some(PubSubMessage { channel, payload })
}

fn install_crypto_provider_if_needed(address: &Address) {
    if matches!(address, Address::Tls { .. }) {
        // Installing a provider twice is harmless; another library may have installed it.
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_encoding_matches_redis_py() {
        let mut pipeline = ::redis::Pipeline::new();
        Command::Set {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            ex: None,
        }
        .append(&mut pipeline);
        Command::Publish {
            channel: Bytes::from_static(b"k"),
            message: Bytes::from_static(b"v"),
        }
        .append(&mut pipeline);
        let packed = pipeline.get_packed_pipeline();
        assert_eq!(
            packed,
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*3\r\n$7\r\nPUBLISH\r\n$1\r\nk\r\n$1\r\nv\r\n"
        );
    }

    #[test]
    fn set_with_expiry_uses_set_ex() {
        let mut pipeline = ::redis::Pipeline::new();
        Command::Set {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            ex: Some(10),
        }
        .append(&mut pipeline);
        assert_eq!(
            pipeline.get_packed_pipeline(),
            b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\r\n10\r\n"
        );
    }

    #[test]
    fn incr_always_uses_incrby() {
        let mut pipeline = ::redis::Pipeline::new();
        Command::Incr {
            key: Bytes::from_static(b"n"),
            amount: None,
        }
        .append(&mut pipeline);
        assert_eq!(
            pipeline.get_packed_pipeline(),
            b"*3\r\n$6\r\nINCRBY\r\n$1\r\nn\r\n$1\r\n1\r\n"
        );
    }

    #[test]
    fn atomic_pipeline_uses_multi_exec() {
        let mut pipeline = ::redis::Pipeline::new();
        pipeline.atomic();
        Command::SetEx {
            key: Bytes::from_static(b"k"),
            seconds: 10,
            value: Bytes::from_static(b"v"),
        }
        .append(&mut pipeline);
        let packed = pipeline.get_packed_pipeline();
        assert!(packed.starts_with(b"*1\r\n$5\r\nMULTI\r\n"));
        assert!(packed.windows(6).any(|window| window == b"SETEX\r"));
        assert!(packed.ends_with(b"*1\r\n$4\r\nEXEC\r\n"));
    }

    #[test]
    fn zadd_preserves_pair_order() {
        let mut pipeline = ::redis::Pipeline::new();
        Command::ZAdd {
            key: Bytes::from_static(b"z"),
            pairs: vec![
                (Bytes::from_static(b"1"), Bytes::from_static(b"a")),
                (Bytes::from_static(b"+inf"), Bytes::from_static(b"b")),
            ],
        }
        .append(&mut pipeline);
        let packed = pipeline.get_packed_pipeline();
        let text = String::from_utf8_lossy(&packed);
        assert_eq!(
            text,
            "*6\r\n$4\r\nZADD\r\n$1\r\nz\r\n$1\r\n1\r\n$1\r\na\r\n$4\r\n+inf\r\n$1\r\nb\r\n"
        );
    }

    #[test]
    fn reply_types_follow_redis_py() {
        for ttl in [-2, -1, 60] {
            assert_eq!(
                Command::Ttl {
                    key: Bytes::from_static(b"k")
                }
                .decode(Value::Int(ttl)),
                Reply::Int(ttl)
            );
        }
        assert_eq!(
            Command::Set {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                ex: None,
            }
            .decode(Value::Okay),
            Reply::Bool(true)
        );
        assert_eq!(
            Command::Expire {
                key: Bytes::from_static(b"k"),
                seconds: 1
            }
            .decode(Value::Int(1)),
            Reply::Bool(true)
        );
        assert_eq!(
            Command::Get {
                key: Bytes::from_static(b"k")
            }
            .decode(Value::Nil),
            Reply::Nil
        );
        assert_eq!(
            Command::MGet {
                keys: vec![Bytes::from_static(b"k")]
            }
            .decode(Value::Array(vec![
                Value::BulkString(b"v".to_vec()),
                Value::Nil
            ])),
            Reply::Array(vec![Reply::Bytes(Bytes::from_static(b"v")), Reply::Nil])
        );
    }

    #[test]
    fn error_kinds_map_to_redis_py_exceptions() {
        let error: ::redis::RedisError =
            (::redis::ErrorKind::AuthenticationFailed, "no auth").into();
        assert!(matches!(Error::from_redis(error), Error::Authentication(_)));
        let error: ::redis::RedisError = (
            ::redis::ErrorKind::Server(::redis::ServerErrorKind::ResponseError),
            "bad type",
        )
            .into();
        assert!(matches!(Error::from_redis(error), Error::Response(_)));
        let error: ::redis::RedisError = (::redis::ErrorKind::InvalidClientConfig, "bad").into();
        assert!(matches!(Error::from_redis(error), Error::Invalid(_)));
    }

    // Real Redis tests use unique keys in database 14 without flushing.

    fn test_url() -> String {
        std::env::var("CELERY_RS_REDIS_URL")
            .or_else(|_| std::env::var("CELERY_RS_TEST_REDIS"))
            .unwrap_or_else(|_| "redis://127.0.0.1:6390".into())
    }

    fn test_config(db: i64) -> Config {
        let url = test_url();
        let rest = url.split("://").nth(1).unwrap_or("127.0.0.1:6390");
        let (authority, _) = rest.split_once('/').unwrap_or((rest, ""));
        let (credentials, host_port) = match authority.rsplit_once('@') {
            Some((credentials, host_port)) => (Some(credentials), host_port),
            None => (None, authority),
        };
        let (username, password) = match credentials {
            Some(credentials) => match credentials.split_once(':') {
                Some((user, password)) => (
                    (!user.is_empty()).then(|| user.to_string()),
                    Some(password.to_string()),
                ),
                None => (Some(credentials.to_string()), None),
            },
            None => (None, None),
        };
        let (host, port) = match host_port.rsplit_once(':') {
            Some((host, port)) => (host.to_string(), port.parse().unwrap_or(6379)),
            None => (host_port.to_string(), 6379),
        };
        Config {
            address: Address::Tcp { host, port },
            db,
            username,
            password,
            response_timeout: None,
            connect_timeout: None,
            client_name: None,
            max_connections: None,
            health_check_interval: None,
        }
    }

    fn unique(prefix: &str) -> Bytes {
        Bytes::from(format!("{prefix}-{}", uuid::Uuid::new_v4().simple()))
    }

    #[tokio::test]
    async fn commands_and_pipeline_against_real_redis() {
        let client = Client::open(test_config(14)).expect("client");
        let key = unique("rs-backend-key");
        let bytes = |value: &'static [u8]| Bytes::from_static(value);

        assert_eq!(
            client
                .execute(Command::Set {
                    key: key.clone(),
                    value: bytes(b"value"),
                    ex: None
                })
                .await
                .expect("SET"),
            Reply::Bool(true)
        );
        assert_eq!(
            client
                .execute(Command::Get { key: key.clone() })
                .await
                .expect("GET"),
            Reply::Bytes(bytes(b"value"))
        );
        assert_eq!(
            client
                .execute(Command::MGet {
                    keys: vec![key.clone(), unique("rs-missing")]
                })
                .await
                .expect("MGET"),
            Reply::Array(vec![Reply::Bytes(bytes(b"value")), Reply::Nil])
        );
        assert_eq!(
            client
                .execute(Command::Expire {
                    key: key.clone(),
                    seconds: 30
                })
                .await
                .expect("EXPIRE"),
            Reply::Bool(true)
        );

        let counter = unique("rs-counter");
        assert_eq!(
            client
                .execute(Command::Incr {
                    key: counter.clone(),
                    amount: None
                })
                .await
                .expect("INCR"),
            Reply::Int(1)
        );
        assert_eq!(
            client
                .execute(Command::Incr {
                    key: counter.clone(),
                    amount: Some(2)
                })
                .await
                .expect("INCRBY"),
            Reply::Int(3)
        );

        let mut pipeline = Pipeline::new(true);
        pipeline.push(Command::SetEx {
            key: key.clone(),
            seconds: 60,
            value: bytes(b"pipeline"),
        });
        pipeline.push(Command::Publish {
            channel: key.clone(),
            message: bytes(b"pipeline"),
        });
        pipeline.push(Command::Get { key: key.clone() });
        pipeline.push(Command::Delete {
            keys: vec![key.clone()],
        });
        assert_eq!(
            client.execute_pipeline(pipeline).await.expect("pipeline"),
            vec![
                Reply::Bool(true),
                Reply::Int(0),
                Reply::Bytes(bytes(b"pipeline")),
                Reply::Int(1)
            ]
        );

        let zset = unique("rs-zset");
        assert_eq!(
            client
                .execute(Command::ZAdd {
                    key: zset.clone(),
                    pairs: vec![(bytes(b"1"), bytes(b"a")), (bytes(b"+inf"), bytes(b"b")),],
                })
                .await
                .expect("ZADD"),
            Reply::Int(2)
        );
        assert_eq!(
            client
                .execute(Command::ZCount {
                    key: zset.clone(),
                    min: bytes(b"-inf"),
                    max: bytes(b"+inf"),
                })
                .await
                .expect("ZCOUNT"),
            Reply::Int(2)
        );
        assert_eq!(
            client
                .execute(Command::ZRange {
                    key: zset.clone(),
                    start: 0,
                    stop: -1
                })
                .await
                .expect("ZRANGE"),
            Reply::Array(vec![Reply::Bytes(bytes(b"a")), Reply::Bytes(bytes(b"b"))])
        );

        let list = unique("rs-list");
        assert_eq!(
            client
                .execute(Command::RPush {
                    key: list.clone(),
                    values: vec![bytes(b"x"), bytes(b"y")]
                })
                .await
                .expect("RPUSH"),
            Reply::Int(2)
        );
        assert_eq!(
            client
                .execute(Command::LLen { key: list.clone() })
                .await
                .expect("LLEN"),
            Reply::Int(2)
        );
        assert_eq!(
            client
                .execute(Command::LRange {
                    key: list.clone(),
                    start: 0,
                    stop: -1
                })
                .await
                .expect("LRANGE"),
            Reply::Array(vec![Reply::Bytes(bytes(b"x")), Reply::Bytes(bytes(b"y"))])
        );

        client.reset();
    }

    #[tokio::test]
    async fn pubsub_delivers_messages() {
        let client = Client::open(test_config(14)).expect("client");
        let channel = unique("rs-pubsub");
        let subscriber = client.pubsub().await.expect("pubsub");
        subscriber
            .subscribe(&[String::from_utf8_lossy(&channel).into_owned()])
            .expect("subscribe");
        assert!(
            !subscriber
                .can_read(Some(Duration::from_millis(0)))
                .await
                .expect("can_read")
        );
        assert_eq!(
            client
                .execute(Command::Publish {
                    channel: channel.clone(),
                    message: Bytes::from_static(b"payload")
                })
                .await
                .expect("PUBLISH"),
            Reply::Int(1)
        );
        assert!(
            subscriber
                .can_read(Some(Duration::from_secs(5)))
                .await
                .expect("can_read")
        );
        let message = subscriber
            .get_message(Some(Duration::from_secs(1)))
            .await
            .expect("get_message")
            .expect("message");
        assert_eq!(message.payload, Bytes::from_static(b"payload"));
        assert_eq!(message.channel.as_bytes(), channel.as_ref());
        subscriber
            .unsubscribe(&[String::from_utf8_lossy(&channel).into_owned()])
            .await
            .expect("unsubscribe");
        assert_eq!(
            client
                .execute(Command::Publish {
                    channel,
                    message: Bytes::from_static(b"after unsubscribe"),
                })
                .await
                .expect("PUBLISH"),
            Reply::Int(0),
        );
        subscriber.close();
        assert!(
            subscriber
                .get_message(Some(Duration::from_millis(1)))
                .await
                .is_err()
        );
    }
}
