//! Dichiarazioni del mailbox del worker (pidbox) e della coda eventi (gossip).
//!
//! I nomi, i tipi di exchange e gli argomenti TTL replicano quelli che
//! costruiscono `kombu.pidbox.Mailbox.get_queue` e
//! `celery.events.receiver.EventReceiver`: stessi byte sul broker, quindi il
//! traffico resta identico a quello del worker di riferimento.

use celery_transport::{ExchangeDecl, ExchangeType, QueueBinding, QueueDecl};
use celery_transport::message::{HeaderValue, Headers};

/// Binding completo di una coda di servizio.
#[derive(Debug, Clone)]
pub struct MailboxBinding {
    pub exchange: ExchangeDecl,
    pub queue: QueueDecl,
    pub binding: QueueBinding,
}

/// `Mailbox.exchange_fmt % namespace`.
pub fn pidbox_exchange(namespace: &str) -> String {
    format!("{namespace}.pidbox")
}

/// `Mailbox.reply_exchange_fmt % namespace`.
pub fn reply_exchange(namespace: &str) -> String {
    format!("reply.{namespace}.pidbox")
}

/// `Mailbox.get_queue(hostname)`: `{hostname}.{namespace}.pidbox`.
pub fn pidbox_queue(hostname: &str, namespace: &str) -> String {
    format!("{hostname}.{namespace}.pidbox")
}

/// `EventReceiver.queue`: `{queue_prefix}.{node_id}`.
pub fn event_queue(prefix: &str, node_id: &str) -> String {
    format!("{prefix}.{node_id}")
}

/// Argomenti RabbitMQ/Redis di una coda con scadenza
/// (`to_rabbitmq_queue_arguments`): secondi → millisecondi interi.
fn queue_arguments(expires: Option<f64>, message_ttl: Option<f64>) -> Headers {
    let mut arguments = Headers::new();
    if let Some(expires) = expires {
        arguments.insert("x-expires", HeaderValue::Int(milliseconds(expires)));
    }
    if let Some(message_ttl) = message_ttl {
        arguments.insert(
            "x-message-ttl",
            HeaderValue::Int(milliseconds(message_ttl)),
        );
    }
    arguments
}

fn milliseconds(seconds: f64) -> i64 {
    // `int(seconds * 1000)` di Python, senza `unwrap` su valori non finiti.
    if seconds.is_finite() {
        (seconds * 1000.0) as i64
    } else {
        0
    }
}

/// Coda del nodo pidbox e binding sull'exchange fanout.
pub fn pidbox(
    hostname: &str,
    namespace: &str,
    expires: Option<f64>,
    message_ttl: Option<f64>,
) -> MailboxBinding {
    let exchange_name = pidbox_exchange(namespace);
    let queue_name = pidbox_queue(hostname, namespace);
    MailboxBinding {
        exchange: ExchangeDecl::new(exchange_name.clone(), ExchangeType::Fanout),
        queue: QueueDecl {
            name: Some(queue_name.clone().into()),
            durable: false,
            auto_delete: true,
            exclusive: true,
            arguments: queue_arguments(expires, message_ttl),
            passive: false,
        },
        // Il fanout non usa la routing key: kombu la lascia vuota.
        binding: QueueBinding::new(queue_name, exchange_name, ""),
    }
}

/// Coda del gossip e binding sull'exchange eventi.
#[allow(clippy::too_many_arguments)]
pub fn events(
    prefix: &str,
    node_id: &str,
    exchange: &str,
    exchange_type: ExchangeType,
    routing_key: &str,
    expires: Option<f64>,
    message_ttl: Option<f64>,
    queue_exclusive: bool,
    queue_durable: bool,
) -> MailboxBinding {
    let queue_name = event_queue(prefix, node_id);
    let mut exchange_decl = ExchangeDecl::new(exchange, exchange_type);
    // `celery.events.event_exchange`: Exchange('celeryev', type='topic') ha i
    // default di kombu (durable=True, auto_delete=False).
    exchange_decl.durable = true;
    MailboxBinding {
        exchange: exchange_decl,
        queue: QueueDecl {
            name: Some(queue_name.clone().into()),
            durable: queue_durable,
            auto_delete: !queue_durable,
            exclusive: queue_exclusive,
            arguments: queue_arguments(expires, message_ttl),
            passive: false,
        },
        binding: QueueBinding::new(queue_name, exchange, routing_key),
    }
}

/// Messaggio di risposta del pidbox: exchange direct transiente.
pub fn reply_binding(namespace: &str) -> ExchangeDecl {
    ExchangeDecl::new(reply_exchange(namespace), ExchangeType::Direct)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pidbox_names_match_kombu() {
        assert_eq!(pidbox_exchange("celery"), "celery.pidbox");
        assert_eq!(reply_exchange("celery"), "reply.celery.pidbox");
        assert_eq!(
            pidbox_queue("celery@node", "celery"),
            "celery@node.celery.pidbox"
        );
    }

    #[test]
    fn pidbox_queue_is_fanout_exclusive_with_expiry() {
        let binding = pidbox("w1", "celery", Some(10.0), Some(300.0));
        assert_eq!(binding.exchange.kind, ExchangeType::Fanout);
        assert!(!binding.exchange.durable);
        assert!(binding.queue.exclusive);
        assert!(binding.queue.auto_delete);
        assert!(!binding.queue.durable);
        assert_eq!(
            binding.queue.arguments.get("x-expires"),
            Some(&HeaderValue::Int(10_000))
        );
        assert_eq!(
            binding.queue.arguments.get("x-message-ttl"),
            Some(&HeaderValue::Int(300_000))
        );
        assert_eq!(binding.binding.exchange.as_str(), "celery.pidbox");
        assert_eq!(binding.binding.routing_key.as_str(), "");
    }

    #[test]
    fn without_ttl_there_are_no_arguments() {
        let binding = pidbox("w1", "celery", None, None);
        assert!(binding.queue.arguments.is_empty());
    }

    #[test]
    fn event_queue_matches_receiver_and_keeps_ttl() {
        let binding = events(
            "celeryev",
            "node-1",
            "celeryev",
            ExchangeType::Topic,
            "worker.#",
            Some(60.0),
            Some(2.0),
            true,
            false,
        );
        assert_eq!(binding.queue.name.as_deref(), Some("celeryev.node-1"));
        assert_eq!(binding.exchange.kind, ExchangeType::Topic);
        assert_eq!(binding.binding.routing_key.as_str(), "worker.#");
        assert!(binding.queue.auto_delete);
        assert_eq!(
            binding.queue.arguments.get("x-expires"),
            Some(&HeaderValue::Int(60_000))
        );
        assert_eq!(
            binding.queue.arguments.get("x-message-ttl"),
            Some(&HeaderValue::Int(2_000))
        );
    }

    #[test]
    fn durable_event_queue_is_not_auto_deleted() {
        let binding = events(
            "celeryev",
            "node-1",
            "celeryev",
            ExchangeType::Fanout,
            "worker.#",
            None,
            None,
            false,
            true,
        );
        assert!(binding.queue.durable);
        assert!(!binding.queue.auto_delete);
        assert!(!binding.queue.exclusive);
    }

    #[test]
    fn fractional_ttl_truncates_like_int() {
        let binding = pidbox("w1", "celery", Some(0.1234), Some(1.9999));
        assert_eq!(
            binding.queue.arguments.get("x-expires"),
            Some(&HeaderValue::Int(123))
        );
        assert_eq!(
            binding.queue.arguments.get("x-message-ttl"),
            Some(&HeaderValue::Int(1999))
        );
    }
}
