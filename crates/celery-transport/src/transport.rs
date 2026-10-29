//! Shared transport contract, errors and reconnection policy. Delivery streams emit
//! one connection error before termination; pending tags belong to their original connection.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use compact_str::CompactString;
use futures_core::Stream;

use crate::message::{Delivery, DeliveryTag, Headers, Message};
use crate::url::{TransportOptions, Url};

use crate::control::{ControlOutcome, ControlRequest};

/// Result of a transport operation.
pub type Result<T> = std::result::Result<T, Error>;

/// Boxed future preserving trait object safety.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Delivery stream returned by [`Transport::consume`].
pub type DeliveryStream = Pin<Box<dyn Stream<Item = Result<Delivery>> + Send>>;

/// Client library whose connection error formatting must be reproduced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientKind {
    Redis,
    Amqp,
}

/// Connection failure cause in client library terms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectFailure {
    /// System error number and strerror text.
    Errno { code: i32, text: String },
    /// Connection timeout.
    Timeout,
    /// Failure without an errno, such as TLS or protocol errors.
    Other(String),
}

impl ConnectFailure {
    /// Classifies an error message when the client library does not expose an I/O cause.
    pub fn from_message(text: &str) -> Self {
        let text = text.trim();
        let lowered = text.to_ascii_lowercase();
        if lowered == "timed out" || lowered.ends_with("timed out") {
            return Self::Timeout;
        }
        if let Some(rest) = text.strip_prefix("failed to lookup address information: ") {
            // getaddrinfo uses -2 and gai_strerror text rather than a positive errno.
            return Self::Errno {
                code: -2,
                text: rest.trim_end_matches('.').to_string(),
            };
        }
        if let Some(start) = text.rfind("(os error ")
            && text.ends_with(')')
            && let Ok(code) = text[start + "(os error ".len()..text.len() - 1].parse::<i32>()
        {
            return Self::Errno {
                code,
                text: text[..start].trim_end().to_string(),
            };
        }
        Self::Other(text.to_string())
    }

    /// Classifies an error through its source chain.
    pub fn from_error(error: &(dyn std::error::Error + 'static)) -> Self {
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
        while let Some(current) = source {
            if let Some(io) = current.downcast_ref::<std::io::Error>() {
                if io.kind() == std::io::ErrorKind::TimedOut {
                    return Self::Timeout;
                }
                if io.raw_os_error().is_some() {
                    return Self::from_message(&io.to_string());
                }
                let text = io.to_string();
                if text.starts_with("failed to lookup address information: ") {
                    return Self::from_message(&text);
                }
                return Self::from_message(&text);
            }
            source = current.source();
        }
        Self::from_message(&error.to_string())
    }
}

/// Errors shared by broker transports.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid broker URL: {0}")]
    InvalidUrl(String),
    #[error("unsupported transport: {0}")]
    UnsupportedTransport(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("invalid transport option: {0}")]
    InvalidTransportOption(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("exchange differs from its existing declaration: {0}")]
    ExchangeNotEquivalent(String),
    /// Declaration rejected with 406 PRECONDITION_FAILED.
    #[error("{method_name}: ({reply_code}) {reply_text}")]
    PreconditionFailed {
        reply_code: u16,
        reply_text: String,
        // AMQP class and method IDs, matching py-amqp.
        method_sig: (u16, u16),
        method_name: &'static str,
    },
    /// Connection failure details used to reproduce the reference client's message.
    #[error("connection to {host}:{port} failed: {message}")]
    Connect {
        host: CompactString,
        port: u16,
        errno: Option<i32>,
        message: String,
        timeout: bool,
        client: ClientKind,
    },
    #[error("unsupported exchange type: {0}")]
    UnsupportedExchangeType(String),
    #[error("exchange not declared: {0}")]
    UnknownExchange(String),
    #[error("invalid routing pattern: {0}")]
    InvalidPattern(String),
    #[error("connection error: {0}")]
    Connection(String),
    #[error("channel error: {0}")]
    Channel(String),
    #[error("unknown delivery tag: {0}")]
    UnknownDeliveryTag(String),
    /// Entity missing during a passive declaration.
    #[error("not_found - entity not found: {0}")]
    NotFound(String),
    /// Connection lost; emitted once before the stream terminates.
    #[error("broker connection lost: {0}")]
    ConnectionLost(String),
    #[error("transport closed")]
    Closed,
}

impl Error {
    /// Builds a connection failure with host, port and client error details.
    pub fn connect(url: &Url, client: ClientKind, failure: ConnectFailure) -> Self {
        let info = url.info();
        let port = info.port.unwrap_or_default();
        let (errno, message, timeout) = match failure {
            ConnectFailure::Errno { code, text } => (Some(code), text, false),
            ConnectFailure::Timeout => (None, "timeout".to_string(), true),
            ConnectFailure::Other(text) => (None, text, false),
        };
        Self::Connect {
            host: CompactString::from(info.hostname.as_str()),
            port,
            errno,
            message,
            timeout,
            client,
        }
    }

    /// Returns the reference client's connection error text, or None for other errors.
    pub fn client_message(&self) -> Option<String> {
        let Self::Connect {
            host,
            port,
            errno,
            message,
            timeout,
            client,
        } = self
        else {
            return None;
        };
        if *timeout {
            return Some(
                match client {
                    ClientKind::Redis => "Timeout connecting to server",
                    ClientKind::Amqp => "timed out",
                }
                .to_string(),
            );
        }
        Some(match client {
            ClientKind::Redis => match errno {
                Some(code) => format!("Error {code} connecting to {host}:{port}. {message}."),
                None => message.clone(),
            },
            ClientKind::Amqp => match errno {
                Some(code) => format!("[Errno {code}] {message}"),
                None => message.clone(),
            },
        })
    }
}

/// Standard AMQP 0.9.1 exchange type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ExchangeType {
    #[default]
    Direct,
    Topic,
    Fanout,
    Headers,
}

impl ExchangeType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Topic => "topic",
            Self::Fanout => "fanout",
            Self::Headers => "headers",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "direct" => Ok(Self::Direct),
            "topic" => Ok(Self::Topic),
            "fanout" => Ok(Self::Fanout),
            "headers" => Ok(Self::Headers),
            other => Err(Error::UnsupportedExchangeType(other.to_string())),
        }
    }

    /// Returns whether routing is resolved by the client.
    pub fn is_virtual(self) -> bool {
        matches!(self, Self::Direct | Self::Topic | Self::Fanout)
    }
}

/// Exchange declaration.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExchangeDecl {
    pub name: CompactString,
    pub kind: ExchangeType,
    pub durable: bool,
    pub auto_delete: bool,
    pub arguments: Headers,
    /// Checks existence without creating the exchange or validating its parameters.
    pub passive: bool,
}

impl ExchangeDecl {
    pub fn new(name: impl Into<CompactString>, kind: ExchangeType) -> Self {
        Self {
            name: name.into(),
            kind,
            ..Self::default()
        }
    }
}

/// Queue declaration; a missing name requests a broker-generated name.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueueDecl {
    pub name: Option<CompactString>,
    pub durable: bool,
    pub auto_delete: bool,
    pub exclusive: bool,
    pub arguments: Headers,
    /// Checks existence without creating the queue.
    pub passive: bool,
}

impl QueueDecl {
    pub fn new(name: impl Into<CompactString>) -> Self {
        Self {
            name: Some(name.into()),
            ..Self::default()
        }
    }
}

/// Queue declaration result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueInfo {
    /// Effective queue name, generated when omitted.
    pub name: CompactString,
    /// Ready message count at declaration time, excluding pending deliveries.
    pub message_count: u64,
}

/// Queue-to-exchange binding.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueueBinding {
    pub queue: CompactString,
    pub exchange: CompactString,
    pub routing_key: CompactString,
    pub arguments: Headers,
}

impl QueueBinding {
    pub fn new(
        queue: impl Into<CompactString>,
        exchange: impl Into<CompactString>,
        routing_key: impl Into<CompactString>,
    ) -> Self {
        Self {
            queue: queue.into(),
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            arguments: Headers::new(),
        }
    }
}

/// Publication request.
#[derive(Debug, Clone, PartialEq)]
pub struct PublishRequest {
    /// None or an empty string selects the default exchange and routes to the named queue.
    pub exchange: Option<CompactString>,
    pub routing_key: CompactString,
    pub message: Message,
}

impl PublishRequest {
    pub fn new(
        exchange: Option<impl Into<CompactString>>,
        routing_key: impl Into<CompactString>,
        message: Message,
    ) -> Self {
        Self {
            exchange: exchange.map(Into::into),
            routing_key: routing_key.into(),
            message,
        }
    }
}

/// Bounded linear reconnection backoff with Celery defaults: 100 retries,
/// starting at two seconds, increasing by two seconds up to thirty seconds.
#[derive(Debug, Clone, PartialEq)]
pub struct RetryPolicy {
    pub enabled: bool,
    /// None allows unlimited retries.
    pub max_retries: Option<u32>,
    pub interval_start: Duration,
    pub interval_step: Duration,
    pub interval_max: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_retries: Some(100),
            interval_start: Duration::from_secs(2),
            interval_step: Duration::from_secs(2),
            interval_max: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// Returns whether another attempt is allowed after the given number of failures.
    pub fn should_retry(&self, retries: u32) -> bool {
        if !self.enabled {
            return false;
        }
        match self.max_retries {
            Some(max) => retries < max,
            None => true,
        }
    }

    /// Returns the delay after the given number of failed attempts.
    pub fn delay(&self, retries: u32) -> Duration {
        let extra = self.interval_step.saturating_mul(retries);
        self.interval_start
            .saturating_add(extra)
            .min(self.interval_max)
    }
}

/// Connection options and retry policy for socket transports.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConnectConfig {
    pub transport_options: TransportOptions,
    pub retry_policy: RetryPolicy,
}

/// Controls a consumer from any thread, waking pending polls when settings change.
pub trait ConsumerControl: Send + Sync {
    /// Sets an absolute prefetch limit immediately; zero is unlimited.
    ///
    /// # Errors
    /// Returns [`Error::Closed`] if the consumer was cancelled.
    fn set_prefetch<'a>(&'a self, prefetch: u16) -> BoxFuture<'a, Result<()>>;

    /// Adds a queue to the consumer.
    fn add_queue<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<()>>;

    /// Stops consuming a queue while preserving pending acknowledgments.
    fn cancel_queue<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<()>>;

    /// Cancels the stream while preserving pending acknowledgments until connection close.
    fn cancel<'a>(&'a self) -> BoxFuture<'a, Result<()>>;
}

/// Active delivery stream and its control handle.
pub struct Consumer {
    /// Stream ending after cancellation or one connection error.
    pub deliveries: DeliveryStream,
    /// Handle for changing prefetch, queues or cancellation state.
    pub control: Arc<dyn ConsumerControl>,
}

impl Consumer {
    /// Returns the delivery stream, discarding the control handle.
    pub fn into_stream(self) -> DeliveryStream {
        self.deliveries
    }
}

/// Asynchronous broker connection with independent consumer prefetch windows.
/// Boxed futures allow use through a trait object.
pub trait Transport: Send + Sync {
    /// Declares an exchange or passively checks its existence without creating it.
    ///
    /// # Errors
    /// Returns [`Error::NotFound`] if a passive exchange is missing.
    fn declare_exchange<'a>(&'a self, declaration: &'a ExchangeDecl) -> BoxFuture<'a, Result<()>>;

    /// Declares a queue or passively checks its existence without creating it.
    fn declare_queue<'a>(&'a self, declaration: &'a QueueDecl) -> BoxFuture<'a, Result<QueueInfo>>;

    fn bind<'a>(&'a self, binding: &'a QueueBinding) -> BoxFuture<'a, Result<()>>;

    /// Removes a binding; missing bindings or exchanges are ignored.
    fn unbind<'a>(&'a self, binding: &'a QueueBinding) -> BoxFuture<'a, Result<()>>;

    /// Deletes an exchange and bound empty queues, matching kombu's virtual transport.
    fn delete_exchange<'a>(&'a self, exchange: &'a str) -> BoxFuture<'a, Result<()>>;

    fn publish<'a>(&'a self, request: PublishRequest) -> BoxFuture<'a, Result<()>>;

    fn publish_batch<'a>(&'a self, requests: Vec<PublishRequest>) -> BoxFuture<'a, Result<()>>;

    // Creates a consumer with an initial prefetch limit; zero is unlimited.
    fn consume<'a>(
        &'a self,
        queues: &'a [CompactString],
        prefetch: u16,
    ) -> BoxFuture<'a, Result<Consumer>>;

    // Creates a no_ack consumer. The default implementation requires callers to
    // acknowledge deliveries immediately; native implementations omit acknowledgments.
    fn consume_no_ack<'a>(
        &'a self,
        queues: &'a [CompactString],
        prefetch: u16,
    ) -> BoxFuture<'a, Result<Consumer>> {
        self.consume(queues, prefetch)
    }

    // Collects temporary pidbox replies without QoS or acknowledgments, cancelling
    // the consumer before returning. May share the declaration channel.
    fn consume_control_reply<'a>(
        &'a self,
        queue: &'a CompactString,
    ) -> BoxFuture<'a, Result<Consumer>> {
        self.consume_no_ack(std::slice::from_ref(queue), 0)
    }

    /// Cleans up temporary replies; AMQP relies on broker auto-delete as kombu does.
    fn after_reply_message_received<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<()>> {
        self.delete_queue(queue)
    }

    /// Fetches one message independently of consumers and prefetch. Unless no_ack is set,
    /// the caller must acknowledge or reject the delivery.
    fn get<'a>(&'a self, queue: &'a str, no_ack: bool) -> BoxFuture<'a, Result<Option<Delivery>>>;

    // Publishes a mailbox command and optionally collects replies using kombu semantics.
    // Redis overrides the default to preserve binding scripts and BRPOP wire commands.
    fn control_request<'a>(
        &'a self,
        request: ControlRequest,
    ) -> BoxFuture<'a, Result<ControlOutcome>> {
        Box::pin(crate::control::control_request_default(self, request))
    }

    /// Acknowledges a delivery, ignoring unknown tags so late acknowledgments are harmless.
    fn ack<'a>(&'a self, delivery_tag: &'a DeliveryTag) -> BoxFuture<'a, Result<()>>;

    // Rejects or requeues a delivery.
    //
    // # Errors
    // Returns [`Error::UnknownDeliveryTag`] when requeuing an unknown tag; otherwise ignores it.
    fn reject<'a>(
        &'a self,
        delivery_tag: &'a DeliveryTag,
        requeue: bool,
    ) -> BoxFuture<'a, Result<()>>;

    fn purge<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<u64>>;

    fn queue_size<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<u64>>;

    fn delete_queue<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<()>>;

    /// Closes the connection; its streams emit one connection error and terminate.
    /// Pending deliveries follow the transport's restoration rules.
    fn close<'a>(&'a self) -> BoxFuture<'a, Result<()>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_failure_classifies_client_messages() {
        assert_eq!(
            ConnectFailure::from_message("Connection refused (os error 111)"),
            ConnectFailure::Errno {
                code: 111,
                text: "Connection refused".to_string()
            }
        );
        assert_eq!(
            ConnectFailure::from_message(
                "failed to lookup address information: Name or service not known"
            ),
            ConnectFailure::Errno {
                code: -2,
                text: "Name or service not known".to_string()
            }
        );
        assert_eq!(
            ConnectFailure::from_message("IO error: timed out"),
            ConnectFailure::Timeout
        );
        assert_eq!(
            ConnectFailure::from_message("something else"),
            ConnectFailure::Other("something else".to_string())
        );
    }

    #[test]
    fn connect_error_formats_like_redis_py_and_pyamqp() {
        let url = Url::parse("redis://127.0.0.1:6399/0").expect("url");
        let redis = Error::connect(
            &url,
            ClientKind::Redis,
            ConnectFailure::Errno {
                code: 111,
                text: "Connection refused".to_string(),
            },
        );
        assert_eq!(
            redis.client_message().as_deref(),
            Some("Error 111 connecting to 127.0.0.1:6399. Connection refused.")
        );
        let url = Url::parse("amqp://guest:guest@127.0.0.1:5699//").expect("url");
        let amqp = Error::connect(&url, ClientKind::Amqp, ConnectFailure::Timeout);
        assert_eq!(amqp.client_message().as_deref(), Some("timed out"));
        let redis = Error::connect(&url, ClientKind::Redis, ConnectFailure::Timeout);
        assert_eq!(
            redis.client_message().as_deref(),
            Some("Timeout connecting to server")
        );
        assert_eq!(Error::Closed.client_message(), None);
    }

    #[test]
    fn retry_policy_defaults_match_celery() {
        let policy = RetryPolicy::default();
        assert!(policy.enabled);
        assert_eq!(policy.max_retries, Some(100));
        assert_eq!(policy.interval_start, Duration::from_secs(2));
        assert_eq!(policy.interval_step, Duration::from_secs(2));
        assert_eq!(policy.interval_max, Duration::from_secs(30));
    }

    #[test]
    fn retry_backoff_grows_and_is_clamped() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.delay(0), Duration::from_secs(2));
        assert_eq!(policy.delay(1), Duration::from_secs(4));
        assert_eq!(policy.delay(14), Duration::from_secs(30));
        assert_eq!(policy.delay(1_000), Duration::from_secs(30));
    }

    #[test]
    fn retry_policy_exhaustion() {
        let policy = RetryPolicy::default();
        assert!(policy.should_retry(0));
        assert!(policy.should_retry(99));
        assert!(!policy.should_retry(100));

        let unlimited = RetryPolicy {
            max_retries: None,
            ..RetryPolicy::default()
        };
        assert!(unlimited.should_retry(u32::MAX));

        let disabled = RetryPolicy {
            enabled: false,
            ..RetryPolicy::default()
        };
        assert!(!disabled.should_retry(0));
    }

    #[test]
    fn exchange_type_parse() {
        for (text, kind) in [
            ("direct", ExchangeType::Direct),
            ("topic", ExchangeType::Topic),
            ("fanout", ExchangeType::Fanout),
            ("headers", ExchangeType::Headers),
        ] {
            match ExchangeType::parse(text) {
                Ok(parsed) => assert_eq!(parsed, kind),
                Err(err) => panic!("{text}: {err}"),
            }
            assert_eq!(kind.as_str(), text);
        }
        assert!(matches!(
            ExchangeType::parse("x-delayed-message"),
            Err(Error::UnsupportedExchangeType(_))
        ));
    }

    #[test]
    fn declarations_builders() {
        let exchange = ExchangeDecl::new("ex", ExchangeType::Topic);
        assert_eq!(exchange.name, "ex");
        assert_eq!(exchange.kind, ExchangeType::Topic);
        assert!(!exchange.durable);
        let queue = QueueDecl::new("q");
        assert_eq!(queue.name.as_deref(), Some("q"));
        let binding = QueueBinding::new("q", "ex", "rk");
        assert_eq!(binding.routing_key, "rk");
    }
}
