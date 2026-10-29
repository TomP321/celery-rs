//! Remote control requests with kombu mailbox semantics. Reply limits count received
//! messages, including other tickets; the timeout restarts after each delivery.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use compact_str::CompactString;

use crate::message::{Delivery, HeaderValue, Headers, Message, Properties};
use crate::transport::{
    ExchangeDecl, ExchangeType, PublishRequest, QueueBinding, QueueDecl, Result,
};

/// Routing for a mailbox reply queue.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplyRoute {
    pub exchange: CompactString,
    pub exchange_kind: ExchangeType,
    pub name: CompactString,
    pub routing_key: CompactString,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    /// Queue arguments with expiry and TTL converted to milliseconds.
    pub arguments: Headers,
}

/// Remote control request with an optional reply queue.
#[derive(Debug, Clone, PartialEq)]
pub struct ControlRequest {
    pub exchange: CompactString,
    pub exchange_kind: ExchangeType,
    pub body: Bytes,
    pub content_type: Option<CompactString>,
    pub content_encoding: Option<CompactString>,
    pub headers: Headers,
    pub reply: Option<ReplyRoute>,
    pub timeout: f64,
    /// `None` waits until timeout without a reply limit.
    pub limit: Option<usize>,
    /// Client-generated ticket, required when `reply` is set.
    pub ticket: Option<CompactString>,
}

impl ControlRequest {
    /// Message ready for publication.
    pub fn message(&self) -> Message {
        Message {
            body: self.body.clone(),
            content_type: self.content_type.clone(),
            content_encoding: self.content_encoding.clone(),
            headers: self.headers.clone(),
            properties: self.properties(),
        }
    }

    // Returns kombu producer properties: `delivery_mode=2`, `priority=0`.
    fn properties(&self) -> Properties {
        let mut properties = Properties::new();
        properties.set_delivery_mode(2);
        properties.set_priority(0);
        properties
    }

    /// Builds a pidbox publication with an empty fanout routing key.
    pub fn publish_request(&self) -> PublishRequest {
        PublishRequest::new(Some(self.exchange.clone()), "", self.message())
    }

    /// Returns the mailbox fanout exchange declaration.
    pub fn exchange_declaration(&self) -> ExchangeDecl {
        ExchangeDecl {
            name: self.exchange.clone(),
            kind: self.exchange_kind,
            durable: false,
            auto_delete: false,
            arguments: Headers::new(),
            passive: false,
        }
    }

    /// Returns the reply exchange declaration.
    pub fn reply_exchange_declaration(&self, reply: &ReplyRoute) -> ExchangeDecl {
        ExchangeDecl {
            name: reply.exchange.clone(),
            kind: reply.exchange_kind,
            durable: false,
            auto_delete: false,
            arguments: Headers::new(),
            passive: false,
        }
    }

    /// Returns the reply queue declaration with its precomputed auto-delete setting.
    pub fn reply_queue_declaration(&self, reply: &ReplyRoute) -> QueueDecl {
        QueueDecl {
            name: Some(reply.name.clone()),
            durable: reply.durable,
            auto_delete: reply.auto_delete,
            exclusive: reply.exclusive,
            arguments: reply.arguments.clone(),
            passive: false,
        }
    }

    pub fn reply_binding(&self, reply: &ReplyRoute) -> QueueBinding {
        QueueBinding::new(
            reply.name.clone(),
            reply.exchange.clone(),
            reply.routing_key.clone(),
        )
    }
}

/// Reply with its original headers.
#[derive(Debug, Clone, PartialEq)]
pub struct ControlReply {
    pub body: Bytes,
    pub content_type: Option<CompactString>,
    pub content_encoding: Option<CompactString>,
    pub headers: Headers,
}

/// Collected replies and messages belonging to other tickets.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ControlOutcome {
    pub replies: Vec<ControlReply>,
    /// Replies retained for a later collection of the same ticket.
    pub unclaimed: Vec<(CompactString, ControlReply)>,
}

/// Classification of a received reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyKind {
    /// Reply matching the requested ticket.
    Matched,
    /// Reply for another ticket, or an expired reply.
    Other,
}

impl ControlOutcome {
    /// Classifies a delivery as kombu does; expired matching replies still count toward the limit.
    pub fn push(&mut self, delivery: Delivery, ticket: &str, now: f64) -> ReplyKind {
        let headers = delivery.message.headers;
        if is_expired(&headers, now) {
            return ReplyKind::Other;
        }
        let this_id = headers
            .get("ticket")
            .and_then(HeaderValue::as_str)
            .map_or_else(|| CompactString::from(ticket), CompactString::from);
        let reply = ControlReply {
            body: delivery.message.body,
            content_type: delivery.message.content_type,
            content_encoding: delivery.message.content_encoding,
            headers,
        };
        if this_id == ticket {
            self.replies.push(reply);
            ReplyKind::Matched
        } else {
            self.unclaimed.push((this_id, reply));
            ReplyKind::Other
        }
    }

    /// Removes and returns replies retained for a ticket.
    pub fn take_unclaimed(&mut self, ticket: &str) -> Option<Vec<ControlReply>> {
        if !self.unclaimed.iter().any(|(id, _)| id == ticket) {
            return None;
        }
        let (mine, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.unclaimed)
            .into_iter()
            .partition(|(id, _)| id == ticket);
        self.unclaimed = rest;
        Some(mine.into_iter().map(|(_, reply)| reply).collect())
    }
}

fn is_expired(headers: &Headers, now: f64) -> bool {
    let Some(expires) = headers.get("expires").and_then(HeaderValue::as_f64) else {
        return false;
    };
    expires != 0.0 && now > expires
}

/// Returns epoch seconds with `time.time()` precision.
pub fn now_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |elapsed| elapsed.as_secs_f64())
}

/// Collects replies after a no_ack consumer is created. Every message counts toward
/// the limit, and each delivery restarts the timeout, matching `Mailbox._collect`.
pub async fn collect_replies<S>(
    mut deliveries: S,
    ticket: &str,
    timeout: f64,
    limit: Option<usize>,
) -> Result<ControlOutcome>
where
    S: futures_core::Stream<Item = Result<Delivery>> + Unpin,
{
    use std::future::poll_fn;
    let timeout = if timeout.is_finite() && timeout > 0.0 {
        Duration::from_secs_f64(timeout)
    } else {
        Duration::ZERO
    };
    let mut deadline = Instant::now() + timeout;
    let mut outcome = ControlOutcome::default();
    let mut iterations = 0usize;
    loop {
        if limit.is_some_and(|limit| iterations >= limit) {
            break;
        }
        let next = if timeout.is_zero() {
            poll_once_now(&mut deliveries)
        } else {
            match tokio::time::timeout_at(
                deadline.into(),
                poll_fn(|cx| std::pin::Pin::new(&mut deliveries).poll_next(cx)),
            )
            .await
            {
                Ok(value) => value,
                Err(_) => break,
            }
        };
        match next {
            Some(Ok(delivery)) => {
                iterations += 1;
                outcome.push(delivery, ticket, now_seconds());
                deadline = Instant::now() + timeout;
            }
            Some(Err(error)) => return Err(error),
            None => break,
        }
    }
    Ok(outcome)
}

fn poll_once_now<S>(deliveries: &mut S) -> Option<Result<Delivery>>
where
    S: futures_core::Stream<Item = Result<Delivery>> + Unpin,
{
    use std::task::{Context, Poll};
    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);
    match std::pin::Pin::new(deliveries).poll_next(&mut context) {
        Poll::Ready(item) => item,
        Poll::Pending => None,
    }
}

/// Sends and collects a control request for memory or AMQP transports. Redis overrides
/// this to match kombu's binding scripts and BRPOP commands.
pub async fn control_request_default<T>(
    transport: &T,
    request: ControlRequest,
) -> Result<ControlOutcome>
where
    T: crate::Transport + ?Sized,
{
    if let Some(reply) = &request.reply {
        transport
            .declare_exchange(&request.reply_exchange_declaration(reply))
            .await?;
        transport
            .declare_queue(&request.reply_queue_declaration(reply))
            .await?;
        transport.bind(&request.reply_binding(reply)).await?;
    }
    transport
        .declare_exchange(&request.exchange_declaration())
        .await?;
    transport.publish(request.publish_request()).await?;

    let Some(reply) = &request.reply else {
        return Ok(ControlOutcome::default());
    };
    // kombu redeclares the queue and binding when creating the reply consumer.
    transport
        .declare_queue(&request.reply_queue_declaration(reply))
        .await?;
    transport.bind(&request.reply_binding(reply)).await?;

    let ticket = request.ticket.clone().unwrap_or_default();
    let consumer = transport.consume_control_reply(&reply.name).await?;
    let outcome =
        collect_replies(consumer.deliveries, &ticket, request.timeout, request.limit).await?;
    // Match kombu's `after_reply_message_received` cleanup.
    let _ = consumer.control.cancel().await;
    let _ = transport.after_reply_message_received(&reply.name).await;
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Properties;

    fn delivery(headers: Headers, body: &'static [u8]) -> Delivery {
        Delivery {
            message: Message {
                body: Bytes::from_static(body),
                content_type: Some(CompactString::from("application/json")),
                content_encoding: Some(CompactString::from("utf-8")),
                headers,
                properties: Properties::new(),
            },
            exchange: CompactString::new(""),
            routing_key: CompactString::new(""),
            delivery_tag: crate::DeliveryTag::Number(1),
            redelivered: false,
        }
    }

    fn headers(ticket: Option<&str>, expires: Option<f64>) -> Headers {
        let mut headers = Headers::new();
        if let Some(ticket) = ticket {
            headers.insert("ticket", HeaderValue::Str(CompactString::from(ticket)));
        }
        if let Some(expires) = expires {
            headers.insert("expires", HeaderValue::Float(expires));
        }
        headers
    }

    #[test]
    fn classifies_matching_ticket() {
        let mut outcome = ControlOutcome::default();
        let kind = outcome.push(
            delivery(headers(Some("mine"), None), b"{\"ok\":1}"),
            "mine",
            1000.0,
        );
        assert_eq!(kind, ReplyKind::Matched);
        assert_eq!(outcome.replies.len(), 1);
        assert!(outcome.unclaimed.is_empty());
        assert_eq!(outcome.replies[0].body, Bytes::from_static(b"{\"ok\":1}"));
    }

    #[test]
    fn keeps_replies_of_other_tickets_as_unclaimed() {
        let mut outcome = ControlOutcome::default();
        assert_eq!(
            outcome.push(delivery(headers(Some("other"), None), b"1"), "mine", 1000.0),
            ReplyKind::Other
        );
        assert!(outcome.replies.is_empty());
        assert_eq!(outcome.unclaimed.len(), 1);
        assert_eq!(outcome.unclaimed[0].0.as_str(), "other");

        let mine = outcome.take_unclaimed("mine");
        assert!(mine.is_none());
        let other = outcome.take_unclaimed("other").expect("unclaimed");
        assert_eq!(other.len(), 1);
        assert!(outcome.unclaimed.is_empty());
    }

    #[test]
    fn missing_ticket_defaults_to_current_like_kombu() {
        let mut outcome = ControlOutcome::default();
        assert_eq!(
            outcome.push(delivery(headers(None, None), b"1"), "mine", 1000.0),
            ReplyKind::Matched
        );
        assert_eq!(outcome.replies.len(), 1);
    }

    #[test]
    fn drops_expired_replies() {
        let mut outcome = ControlOutcome::default();
        let kind = outcome.push(
            delivery(headers(Some("mine"), Some(10.0)), b"1"),
            "mine",
            11.0,
        );
        assert_eq!(kind, ReplyKind::Other);
        assert!(outcome.replies.is_empty());

        let kind = outcome.push(
            delivery(headers(Some("mine"), Some(0.0)), b"1"),
            "mine",
            11.0,
        );
        assert_eq!(kind, ReplyKind::Matched);
    }

    #[test]
    fn reply_route_uses_kombu_argument_names() {
        let request = ControlRequest {
            exchange: CompactString::from("celery.pidbox"),
            exchange_kind: ExchangeType::Fanout,
            body: Bytes::from_static(b"{}"),
            content_type: Some(CompactString::from("application/json")),
            content_encoding: Some(CompactString::from("utf-8")),
            headers: Headers::new(),
            reply: None,
            timeout: 1.0,
            limit: None,
            ticket: None,
        };
        let publish = request.publish_request();
        assert_eq!(publish.exchange.as_deref(), Some("celery.pidbox"));
        assert_eq!(publish.routing_key, "");
        assert_eq!(publish.message.properties.delivery_mode(), Some(2));
        assert_eq!(publish.message.properties.priority(), Some(0));
        assert_eq!(request.exchange_declaration().kind, ExchangeType::Fanout);
    }

    struct FakeStream {
        items: std::collections::VecDeque<Result<Delivery>>,
    }

    impl futures_core::Stream for FakeStream {
        type Item = Result<Delivery>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            match self.items.pop_front() {
                Some(item) => std::task::Poll::Ready(Some(item)),
                None => std::task::Poll::Pending,
            }
        }
    }

    fn fake(items: Vec<(Option<&str>, &'static [u8])>) -> FakeStream {
        FakeStream {
            items: items
                .into_iter()
                .map(|(ticket, body)| Ok(delivery(headers(ticket, None), body)))
                .collect(),
        }
    }

    #[tokio::test]
    async fn collect_waits_the_full_timeout_without_replies() {
        let started = std::time::Instant::now();
        let outcome = collect_replies(
            FakeStream {
                items: Default::default(),
            },
            "mine",
            0.05,
            None,
        )
        .await
        .expect("collect");
        assert!(outcome.replies.is_empty());
        assert!(started.elapsed() >= Duration::from_millis(45));
    }

    #[tokio::test]
    async fn collect_stops_at_limit_counting_every_message() {
        // The other ticket still consumes one iteration of the limit.
        let stream = fake(vec![
            (Some("mine"), b"1"),
            (Some("other"), b"2"),
            (Some("mine"), b"3"),
        ]);
        let outcome = collect_replies(stream, "mine", 5.0, Some(2))
            .await
            .expect("collect");
        assert_eq!(outcome.replies.len(), 1);
        assert_eq!(outcome.replies[0].body, Bytes::from_static(b"1"));
        assert_eq!(outcome.unclaimed.len(), 1);
        assert_eq!(outcome.unclaimed[0].0.as_str(), "other");
    }

    #[tokio::test]
    async fn collect_returns_every_matching_reply_in_order() {
        let stream = fake(vec![
            (Some("mine"), b"a"),
            (Some("other"), b"x"),
            (Some("mine"), b"b"),
        ]);
        let outcome = collect_replies(stream, "mine", 0.05, None)
            .await
            .expect("collect");
        let bodies: Vec<&[u8]> = outcome
            .replies
            .iter()
            .map(|reply| reply.body.as_ref())
            .collect();
        assert_eq!(bodies, vec![b"a".as_slice(), b"b".as_slice()]);
        assert_eq!(outcome.unclaimed.len(), 1);
    }
}
