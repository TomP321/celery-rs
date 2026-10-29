//! Broker transports for memory, Redis and AMQP URLs.

pub mod amqp;
pub mod control;
pub mod memory;
pub mod message;
pub mod redis;
pub mod routing;
pub mod transport;
pub mod url;

pub use amqp::AmqpTransport;
pub use control::{
    ControlOutcome, ControlReply, ControlRequest, ReplyKind, ReplyRoute, collect_replies,
    control_request_default, now_seconds,
};
pub use memory::MemoryTransport;
pub use message::{Decimal, Delivery, DeliveryTag, HeaderValue, Headers, Message, Properties};
pub use redis::RedisTransport;
pub use transport::{
    BoxFuture, ClientKind, ConnectConfig, ConnectFailure, Consumer, ConsumerControl,
    DeliveryStream, Error, ExchangeDecl, ExchangeType, PublishRequest, QueueBinding, QueueDecl,
    QueueInfo, Result, RetryPolicy, Transport,
};
pub use url::{FanoutPrefix, QueueOrderStrategy, TransportKind, TransportOptions, Url, UrlInfo};

/// Connects to a broker URL with default transport options.
pub async fn connect(url: &str) -> Result<Box<dyn Transport>> {
    connect_with(url, TransportOptions::default()).await
}

/// Connects to a broker URL with transport options.
pub async fn connect_with(
    url: &str,
    transport_options: TransportOptions,
) -> Result<Box<dyn Transport>> {
    connect_with_config(
        url,
        &ConnectConfig {
            transport_options,
            retry_policy: RetryPolicy::default(),
        },
    )
    .await
}

/// Connects to a broker URL with a complete configuration.
pub async fn connect_with_config(url: &str, config: &ConnectConfig) -> Result<Box<dyn Transport>> {
    let parsed = Url::parse(url)?;
    connect_parsed_with_config(&parsed, config).await
}

/// Connects to a parsed URL to avoid parsing twice in Python bindings.
pub async fn connect_parsed(
    url: &Url,
    transport_options: TransportOptions,
) -> Result<Box<dyn Transport>> {
    connect_parsed_with_config(
        url,
        &ConnectConfig {
            transport_options,
            retry_policy: RetryPolicy::default(),
        },
    )
    .await
}

/// Connects to a parsed URL with a complete configuration.
pub async fn connect_parsed_with_config(
    url: &Url,
    config: &ConnectConfig,
) -> Result<Box<dyn Transport>> {
    let options = config.transport_options.clone();
    match url.kind {
        TransportKind::Memory => Ok(Box::new(memory::MemoryTransport::with_options(
            url.virtual_host(),
            options,
        ))),
        TransportKind::Redis | TransportKind::Rediss => {
            Ok(Box::new(redis::connect(url, config).await?))
        }
        TransportKind::Amqp | TransportKind::Amqps | TransportKind::PyAmqp => Ok(Box::new(
            amqp::AmqpTransport::connect(url, options, &config.retry_policy).await?,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connects_memory_and_isolates_vhosts() {
        let first = connect("memory:///connect-test-a").await.expect("memory");
        let second = connect("memory:///connect-test-b").await.expect("memory");
        first
            .declare_queue(&QueueDecl::new("q"))
            .await
            .expect("declare");
        assert_eq!(first.queue_size("q").await.expect("size"), 0);
        assert_eq!(second.queue_size("q").await.expect("size"), 0);
    }

    #[test]
    fn redis_database_is_parsed_from_virtual_host() {
        use crate::redis::parse_db;
        assert_eq!(parse_db("/").expect("default"), 0);
        assert_eq!(parse_db("").expect("empty"), 0);
        assert_eq!(parse_db("3").expect("db"), 3);
        assert_eq!(parse_db("/7").expect("db with slash"), 7);
        assert!(parse_db("nope").is_err());
        // kombu removes only one slash, so `//` is not a number.
        assert!(parse_db("//").is_err());
    }

    #[tokio::test]
    async fn connect_config_defaults_are_celery_style() {
        let config = ConnectConfig::default();
        assert_eq!(config.retry_policy, RetryPolicy::default());
        assert!(config.retry_policy.enabled);
        assert_eq!(config.retry_policy.max_retries, Some(100));
        assert_eq!(
            connect_with_config("memory:///connect-test-config", &config)
                .await
                .expect("memory")
                .queue_size("missing")
                .await
                .expect("size"),
            0
        );
    }

    #[tokio::test]
    async fn invalid_url_is_reported() {
        assert!(matches!(
            connect("not-a-url").await,
            Err(Error::InvalidUrl(_))
        ));
    }
}
