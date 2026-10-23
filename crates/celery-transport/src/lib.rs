//! Trasporti broker: memory, redis, amqp (vedi PLAN.md, fase 2).
//!
//! `connect` restituisce un `Box<dyn Transport>`; per ora solo `memory://` è
//! implementato, mentre `redis://`/`amqp://` rispondono [`Error::NotImplemented`]
//! (le spec 02b e 02c aggiungono i rispettivi moduli senza toccare questa API).

pub mod memory;
pub mod message;
pub mod routing;
pub mod transport;
pub mod url;

pub use memory::MemoryTransport;
pub use message::{Decimal, Delivery, DeliveryTag, HeaderValue, Headers, Message, Properties};
pub use transport::{
    BoxFuture, ConnectConfig, Consumer, ConsumerControl, DeliveryStream, Error, ExchangeDecl,
    ExchangeType, PublishRequest, QueueBinding, QueueDecl, QueueInfo, Result, RetryPolicy,
    Transport,
};
pub use url::{FanoutPrefix, QueueOrderStrategy, TransportKind, TransportOptions, Url, UrlInfo};

/// Connessione al broker da URL, con le opzioni di trasporto di default.
pub async fn connect(url: &str) -> Result<Box<dyn Transport>> {
    connect_with(url, TransportOptions::default()).await
}

/// Connessione al broker da URL con `transport_options`.
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

/// Connessione al broker da URL con configurazione completa.
pub async fn connect_with_config(url: &str, config: &ConnectConfig) -> Result<Box<dyn Transport>> {
    let parsed = Url::parse(url)?;
    connect_parsed_with_config(&parsed, config).await
}

/// Connessione da URL già scomposto (evita un secondo parsing nel binding Python).
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

/// Connessione da URL già scomposto con configurazione completa.
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
        TransportKind::Redis | TransportKind::Rediss => Err(Error::NotImplemented(
            "trasporto Redis: implementato in 02b (crates/celery-transport/src/redis/)".to_string(),
        )),
        TransportKind::Amqp | TransportKind::Amqps | TransportKind::PyAmqp => {
            Err(Error::NotImplemented(
                "trasporto AMQP: implementato in 02c (crates/celery-transport/src/amqp/)"
                    .to_string(),
            ))
        }
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

    #[tokio::test]
    async fn redis_and_amqp_report_not_implemented() {
        for url in [
            "redis://127.0.0.1:6390/0",
            "rediss://127.0.0.1:6390/0",
            "amqp://guest:guest@127.0.0.1:5673//",
            "amqps://127.0.0.1:5673//",
            "pyamqp://guest:guest@127.0.0.1:5673//",
        ] {
            match connect(url).await {
                Err(Error::NotImplemented(message)) => {
                    assert!(!message.is_empty(), "messaggio chiaro per {url}");
                }
                Err(other) => panic!("{url}: atteso NotImplemented, ottenuto {other}"),
                Ok(_) => panic!("{url}: atteso NotImplemented, connessione riuscita"),
            }
        }
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
