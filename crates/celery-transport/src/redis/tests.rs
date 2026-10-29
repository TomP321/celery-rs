//! Redis transport tests against a real server. Tests serialize access because
//! unacked keys are shared; CELERY_RS_TEST_REDIS selects the broker.

use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use bytes::Bytes;

use super::*;
use crate::message::{HeaderValue, Properties};
use crate::transport::{
    Consumer, ExchangeDecl, ExchangeType, PublishRequest, QueueBinding, QueueDecl, Transport,
};

const WAIT: Duration = Duration::from_secs(5);

static SERIAL: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

// Serializes tests because Redis unacked state is shared.
async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    SERIAL.lock().await
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
}

async fn connect(url: &str) -> Box<dyn Transport> {
    crate::connect(url)
        .await
        .unwrap_or_else(|error| panic!("redis unavailable at {url}: {error}"))
}

async fn connect_with(url: &str, extra: &[(&str, HeaderValue)]) -> Box<dyn Transport> {
    let mut headers = Headers::new();
    for (key, value) in extra {
        headers.insert(*key, value.clone());
    }
    let options = TransportOptions::from_headers(&headers).expect("opzioni");
    crate::connect_with(url, options)
        .await
        .unwrap_or_else(|error| panic!("redis unavailable at {url}: {error}"))
}

async fn flush() {
    let client = ::redis::Client::open(test_url()).expect("client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("connection");
    ::redis::cmd("FLUSHDB")
        .exec_async(&mut conn)
        .await
        .expect("FLUSHDB");
}

fn url() -> String {
    test_url()
}

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

async fn expect_empty(stream: &mut DeliveryStream, within: Duration) {
    match tokio::time::timeout(within, next(stream)).await {
        Err(_) => {}
        Ok(Some(Ok(delivery))) => {
            panic!("unexpected delivery: {:?}", delivery.message.body)
        }
        Ok(Some(Err(error))) => panic!("unexpected error: {error}"),
        Ok(None) => panic!("stream ended"),
    }
}

fn message(body: &str) -> Message {
    Message::new(Bytes::copy_from_slice(body.as_bytes()))
        .with_content_type("application/json")
        .with_content_encoding("utf-8")
}

async fn setup(
    transport: &dyn Transport,
    exchange: &str,
    kind: ExchangeType,
    queue: &str,
    routing_key: &str,
) {
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
}

async fn keys(pattern: &str) -> Vec<String> {
    let client = ::redis::Client::open(url()).expect("client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("connection");
    let values: Vec<String> = ::redis::cmd("KEYS")
        .arg(pattern)
        .query_async(&mut conn)
        .await
        .expect("KEYS");
    values
}

#[tokio::test]
async fn publish_consume_ack() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("direct");
    let queue = unique("q");
    setup(&*transport, &exchange, ExchangeType::Direct, &queue, "rk").await;

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
        .publish(PublishRequest::new(Some(&exchange), "rk", msg.clone()))
        .await
        .expect("publish");
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 1);

    let mut stream = transport
        .consume(&[CompactString::from(&queue)], 10)
        .await
        .expect("consume")
        .into_stream();
    let delivery = recv(&mut stream).await;
    assert_eq!(delivery.message.body, msg.body);
    assert_eq!(delivery.message.content_type, msg.content_type);
    assert_eq!(delivery.message.content_encoding, msg.content_encoding);
    assert_eq!(delivery.message.headers, headers);
    assert_eq!(delivery.message.properties.delivery_mode(), Some(2));
    assert_eq!(delivery.message.properties.priority(), Some(3));
    assert_eq!(delivery.message.properties.correlation_id(), Some("id-1"));
    assert_eq!(delivery.exchange, exchange);
    assert_eq!(delivery.routing_key, "rk");
    assert!(!delivery.redelivered);
    assert!(delivery.delivery_tag.as_str().is_some());

    let client = ::redis::Client::open(url()).expect("client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("conn");
    let unacked: i64 = ::redis::cmd("HLEN")
        .arg("unacked")
        .query_async(&mut conn)
        .await
        .expect("HLEN");
    assert_eq!(unacked, 1);

    transport.ack(&delivery.delivery_tag).await.expect("ack");
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 0);
    let unacked: i64 = ::redis::cmd("HLEN")
        .arg("unacked")
        .query_async(&mut conn)
        .await
        .expect("HLEN");
    assert_eq!(unacked, 0);
    let indexed: i64 = ::redis::cmd("ZCARD")
        .arg("unacked_index")
        .query_async(&mut conn)
        .await
        .expect("ZCARD");
    assert_eq!(indexed, 0);
}

#[tokio::test]
async fn redelivered_messages_are_marked() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("redeliver");
    let queue = unique("q");
    setup(&*transport, &exchange, ExchangeType::Direct, &queue, "rk").await;
    transport
        .publish(PublishRequest::new(Some(&exchange), "rk", message("one")))
        .await
        .expect("publish");

    let mut stream = transport
        .consume(&[CompactString::from(&queue)], 0)
        .await
        .expect("consume")
        .into_stream();
    let delivery = recv(&mut stream).await;
    transport
        .reject(&delivery.delivery_tag, true)
        .await
        .expect("requeue");

    let mut stream = transport
        .consume(&[CompactString::from(&queue)], 0)
        .await
        .expect("consume")
        .into_stream();
    let again = recv(&mut stream).await;
    assert_eq!(again.message.body, Bytes::from_static(b"one"));
    assert!(again.redelivered);
    assert_eq!(again.message.headers.get_bool("redelivered"), Some(true));
    transport.ack(&again.delivery_tag).await.expect("ack");
}

#[tokio::test]
async fn reject_without_requeue_discards() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("reject");
    let queue = unique("q");
    setup(&*transport, &exchange, ExchangeType::Direct, &queue, "rk").await;
    transport
        .publish(PublishRequest::new(Some(&exchange), "rk", message("a")))
        .await
        .expect("publish");
    let mut stream = transport
        .consume(&[CompactString::from(&queue)], 0)
        .await
        .expect("consume")
        .into_stream();
    let delivery = recv(&mut stream).await;
    transport
        .reject(&delivery.delivery_tag, false)
        .await
        .expect("reject");
    expect_empty(&mut stream, Duration::from_millis(1500)).await;
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 0);
}

#[tokio::test]
async fn reject_with_requeue_goes_to_the_back_like_a_new_publish() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("requeue");
    let queue = unique("q");
    setup(&*transport, &exchange, ExchangeType::Direct, &queue, "rk").await;
    for body in ["one", "two"] {
        transport
            .publish(PublishRequest::new(Some(&exchange), "rk", message(body)))
            .await
            .expect("publish");
    }
    let mut stream = transport
        .consume(&[CompactString::from(&queue)], 0)
        .await
        .expect("consume")
        .into_stream();
    let first = recv(&mut stream).await;
    assert_eq!(first.message.body, Bytes::from_static(b"one"));
    transport
        .reject(&first.delivery_tag, true)
        .await
        .expect("requeue");

    // _restore_at_beginning uses LPUSH, so restored messages are consumed last.
    let second = recv(&mut stream).await;
    assert_eq!(second.message.body, Bytes::from_static(b"two"));
    let third = recv(&mut stream).await;
    assert_eq!(third.message.body, Bytes::from_static(b"one"));
    assert!(third.redelivered);
    transport.ack(&second.delivery_tag).await.expect("ack");
    transport.ack(&third.delivery_tag).await.expect("ack");

    // kombu raises KeyError when requeuing an unknown tag.
    assert!(matches!(
        transport.reject(&DeliveryTag::from("missing"), true).await,
        Err(Error::UnknownDeliveryTag(_))
    ));
}

#[tokio::test]
async fn prefetch_limits_in_flight() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("prefetch");
    let queue = unique("q");
    setup(&*transport, &exchange, ExchangeType::Direct, &queue, "rk").await;
    for index in 0..5 {
        transport
            .publish(PublishRequest::new(
                Some(&exchange),
                "rk",
                message(&format!("m{index}")),
            ))
            .await
            .expect("publish");
    }
    let mut stream = transport
        .consume(&[CompactString::from(&queue)], 2)
        .await
        .expect("consume")
        .into_stream();
    let first = recv(&mut stream).await;
    let second = recv(&mut stream).await;
    assert_eq!(first.message.body, Bytes::from_static(b"m0"));
    assert_eq!(second.message.body, Bytes::from_static(b"m1"));
    expect_empty(&mut stream, Duration::from_millis(300)).await;

    transport.ack(&first.delivery_tag).await.expect("ack");
    let third = recv(&mut stream).await;
    assert_eq!(third.message.body, Bytes::from_static(b"m2"));
    transport.ack(&second.delivery_tag).await.expect("ack");
    transport.ack(&third.delivery_tag).await.expect("ack");
}

#[tokio::test]
async fn fanout_delivers_to_every_bound_queue() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("fan");
    transport
        .declare_exchange(&ExchangeDecl::new(&exchange, ExchangeType::Fanout))
        .await
        .expect("exchange");
    let mut queues = Vec::new();
    for suffix in ["a", "b"] {
        let queue = unique(suffix);
        transport
            .declare_queue(&QueueDecl::new(&queue))
            .await
            .expect("queue");
        transport
            .bind(&QueueBinding::new(&queue, &exchange, ""))
            .await
            .expect("binding");
        queues.push(queue);
    }
    // Subscribe before publishing to avoid losing fanout messages.
    let mut first = transport
        .consume(&[CompactString::from(&queues[0])], 0)
        .await
        .expect("consume")
        .into_stream();
    let mut second = transport
        .consume(&[CompactString::from(&queues[1])], 0)
        .await
        .expect("consume")
        .into_stream();
    let wait = Duration::from_millis(200);
    let _ = tokio::time::timeout(wait, next(&mut first)).await;
    let _ = tokio::time::timeout(wait, next(&mut second)).await;

    transport
        .publish(PublishRequest::new(Some(&exchange), "", message("bc")))
        .await
        .expect("publish");
    let delivery = recv(&mut first).await;
    assert_eq!(delivery.message.body, Bytes::from_static(b"bc"));
    assert_eq!(delivery.exchange, exchange);
    let other = recv(&mut second).await;
    assert_eq!(other.message.body, Bytes::from_static(b"bc"));
    transport.ack(&delivery.delivery_tag).await.expect("ack");
    transport.ack(&other.delivery_tag).await.expect("ack");

    assert!(keys(&format!("*{}*", queues[0])).await.is_empty());
}

#[tokio::test]
async fn fanout_patterns_use_the_routing_key() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("fanpat");
    transport
        .declare_exchange(&ExchangeDecl::new(&exchange, ExchangeType::Fanout))
        .await
        .expect("exchange");
    let queue = unique("q");
    transport
        .declare_queue(&QueueDecl::new(&queue))
        .await
        .expect("queue");
    transport
        .bind(&QueueBinding::new(&queue, &exchange, "task.#"))
        .await
        .expect("binding");
    let mut stream = transport
        .consume(&[CompactString::from(&queue)], 0)
        .await
        .expect("consume")
        .into_stream();
    let _ = tokio::time::timeout(Duration::from_millis(200), next(&mut stream)).await;

    transport
        .publish(PublishRequest::new(
            Some(&exchange),
            "task.one",
            message("x"),
        ))
        .await
        .expect("publish");
    let delivery = recv(&mut stream).await;
    assert_eq!(delivery.message.body, Bytes::from_static(b"x"));
    assert_eq!(delivery.exchange, exchange);
    assert_eq!(delivery.routing_key, "task.one");
    transport.ack(&delivery.delivery_tag).await.expect("ack");
}

#[tokio::test]
async fn topic_routing_matches_kombu_patterns() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("topics");
    transport
        .declare_exchange(&ExchangeDecl::new(&exchange, ExchangeType::Topic))
        .await
        .expect("exchange");
    for (suffix, routing_key) in [
        ("all", "#"),
        ("created", "*.created"),
        ("account", "account.#"),
        ("exact", "account.1.created"),
    ] {
        let queue = unique(suffix);
        transport
            .declare_queue(&QueueDecl::new(&queue))
            .await
            .expect("queue");
        transport
            .bind(&QueueBinding::new(&queue, &exchange, routing_key))
            .await
            .expect("binding");
        queues_insert(suffix, queue);
    }

    transport
        .publish(PublishRequest::new(
            Some(&exchange),
            "account.1.created",
            message("hit"),
        ))
        .await
        .expect("publish");
    transport
        .publish(PublishRequest::new(
            Some(&exchange),
            "account.created",
            message("without-index"),
        ))
        .await
        .expect("publish");
    for (suffix, expected) in [("all", 2), ("created", 1), ("account", 2), ("exact", 1)] {
        assert_eq!(
            transport.queue_size(&queue_of(suffix)).await.expect("size"),
            expected,
            "{suffix}"
        );
    }

    // kombu requires the dot: `account.#` does not match `account`.
    transport
        .publish(PublishRequest::new(
            Some(&exchange),
            "account",
            message("bare"),
        ))
        .await
        .expect("publish");
    assert_eq!(
        transport.queue_size(&queue_of("all")).await.expect("size"),
        3
    );
    assert_eq!(
        transport
            .queue_size(&queue_of("account"))
            .await
            .expect("size"),
        2
    );
}

static QUEUES: LazyLock<Mutex<HashMap<String, String>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn queues_insert(suffix: &str, queue: String) {
    lock(&QUEUES).insert(suffix.to_string(), queue);
}

fn queue_of(suffix: &str) -> String {
    lock(&QUEUES)
        .get(suffix)
        .cloned()
        .unwrap_or_else(|| panic!("missing queue {suffix}"))
}

#[tokio::test]
async fn anonymous_exchange_routes_to_queue_name() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let queue = unique("anon");
    transport
        .publish(PublishRequest::new(None::<&str>, &queue, message("anon")))
        .await
        .expect("publish");
    let mut stream = transport
        .consume(&[CompactString::from(&queue)], 0)
        .await
        .expect("consume")
        .into_stream();
    let delivery = recv(&mut stream).await;
    assert_eq!(delivery.message.body, Bytes::from_static(b"anon"));
    assert_eq!(delivery.exchange, "");
    assert_eq!(delivery.routing_key, queue);
    transport.ack(&delivery.delivery_tag).await.expect("ack");
}

#[tokio::test]
async fn multiple_queues_are_served_round_robin() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let first = unique("qa");
    let second = unique("qb");
    for queue in [&first, &second] {
        for body in ["x", "y"] {
            transport
                .publish(PublishRequest::new(
                    None::<&str>,
                    queue.as_str(),
                    message(body),
                ))
                .await
                .expect("publish");
        }
    }
    let queues = [CompactString::from(&first), CompactString::from(&second)];
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
    assert_eq!(seen, vec![first.clone(), second.clone(), first, second]);
}

#[tokio::test]
async fn priority_selects_the_same_lists_as_kombu() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let queue = unique("pri");
    for (body, priority) in [("low", 0), ("high", 9), ("mid", 4), ("mid2", 5)] {
        let mut properties = Properties::new();
        properties.set_priority(priority);
        let msg = Message::new(Bytes::copy_from_slice(body.as_bytes())).with_properties(properties);
        transport
            .publish(PublishRequest::new(None::<&str>, &queue, msg))
            .await
            .expect("publish");
    }
    // kombu consumes priority steps in ascending order, starting at zero.
    assert_eq!(keys(&format!("{}*", queue)).await.len(), 3);
    let mut stream = transport
        .consume(&[CompactString::from(&queue)], 0)
        .await
        .expect("consume")
        .into_stream();
    let mut bodies = Vec::new();
    for _ in 0..4 {
        let delivery = recv(&mut stream).await;
        bodies.push(String::from_utf8_lossy(&delivery.message.body).into_owned());
        transport.ack(&delivery.delivery_tag).await.expect("ack");
    }
    assert_eq!(bodies, vec!["low", "mid", "mid2", "high"]);
}

#[tokio::test]
async fn purge_size_and_delete() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("purge");
    let queue = unique("q");
    setup(&*transport, &exchange, ExchangeType::Direct, &queue, "rk").await;
    for body in ["a", "b", "c"] {
        transport
            .publish(PublishRequest::new(Some(&exchange), "rk", message(body)))
            .await
            .expect("publish");
    }
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 3);
    assert_eq!(transport.purge(&queue).await.expect("purge"), 3);
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 0);

    transport
        .publish(PublishRequest::new(Some(&exchange), "rk", message("again")))
        .await
        .expect("publish");
    transport.delete_queue(&queue).await.expect("delete");
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 0);
    assert!(keys(&format!("{}*", queue)).await.is_empty());

    transport
        .publish(PublishRequest::new(Some(&exchange), "rk", message("lost")))
        .await
        .expect("publish");
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 0);
}

#[tokio::test]
async fn state_is_shared_between_connections() {
    let _serial = serial().await;
    flush().await;
    let producer = connect(&url()).await;
    let consumer = connect(&url()).await;
    let exchange = unique("shared");
    let queue = unique("q");
    setup(&*producer, &exchange, ExchangeType::Direct, &queue, "rk").await;
    producer
        .publish(PublishRequest::new(
            Some(&exchange),
            "rk",
            message("shared"),
        ))
        .await
        .expect("publish");
    let mut stream = consumer
        .consume(&[CompactString::from(&queue)], 0)
        .await
        .expect("consume")
        .into_stream();
    let delivery = recv(&mut stream).await;
    assert_eq!(delivery.message.body, Bytes::from_static(b"shared"));
    consumer.ack(&delivery.delivery_tag).await.expect("ack");
}

#[tokio::test]
async fn duplicate_bindings_deliver_once() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("dupe");
    let queue = unique("q");
    setup(&*transport, &exchange, ExchangeType::Topic, &queue, "#").await;
    transport
        .bind(&QueueBinding::new(&queue, &exchange, "a.#"))
        .await
        .expect("second binding");
    transport
        .publish(PublishRequest::new(Some(&exchange), "a.b", message("once")))
        .await
        .expect("publish");
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 1);
}

#[tokio::test]
async fn exchange_declarations_must_be_equivalent() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("eq");
    transport
        .declare_exchange(&ExchangeDecl::new(&exchange, ExchangeType::Direct))
        .await
        .expect("first declaration");
    assert!(matches!(
        transport
            .declare_exchange(&ExchangeDecl::new(&exchange, ExchangeType::Topic))
            .await,
        Err(Error::ExchangeNotEquivalent(_))
    ));
    assert!(matches!(
        transport
            .bind(&QueueBinding::new(unique("q"), unique("missing"), "rk"))
            .await,
        Err(Error::UnknownExchange(_))
    ));
    assert!(matches!(
        transport.consume(&[], 0).await,
        Err(Error::InvalidArgument(_))
    ));
}

#[tokio::test]
async fn get_pops_one_message_and_honours_no_ack() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("get");
    let queue = unique("q");
    setup(&*transport, &exchange, ExchangeType::Direct, &queue, "rk").await;
    for body in ["a", "b"] {
        transport
            .publish(PublishRequest::new(Some(&exchange), "rk", message(body)))
            .await
            .expect("publish");
    }

    let first = transport
        .get(&queue, true)
        .await
        .expect("get")
        .expect("message");
    assert_eq!(first.message.body, Bytes::from_static(b"a"));
    assert_eq!(first.exchange, exchange);
    assert_eq!(first.routing_key, "rk");
    transport.ack(&first.delivery_tag).await.expect("ack no-op");
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 1);

    let second = transport
        .get(&queue, false)
        .await
        .expect("get")
        .expect("message");
    assert_eq!(second.message.body, Bytes::from_static(b"b"));
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 0);
    transport
        .reject(&second.delivery_tag, true)
        .await
        .expect("requeue");
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 1);

    let third = transport
        .get(&queue, false)
        .await
        .expect("get")
        .expect("message");
    assert_eq!(third.message.body, Bytes::from_static(b"b"));
    assert!(third.redelivered);
    transport.ack(&third.delivery_tag).await.expect("ack");

    assert!(transport.get(&queue, true).await.expect("get").is_none());
    assert!(
        transport
            .get(&unique("missing"), true)
            .await
            .expect("get")
            .is_none()
    );
}

#[tokio::test]
async fn consumer_control_changes_prefetch_and_queues() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let first = unique("q1");
    let second = unique("q2");
    for index in 0..4 {
        transport
            .publish(PublishRequest::new(
                None::<&str>,
                first.as_str(),
                message(&format!("q1-{index}")),
            ))
            .await
            .expect("publish");
    }
    for index in 0..2 {
        transport
            .publish(PublishRequest::new(
                None::<&str>,
                second.as_str(),
                message(&format!("q2-{index}")),
            ))
            .await
            .expect("publish");
    }

    let Consumer {
        deliveries,
        control,
    } = transport
        .consume(&[CompactString::from(&first)], 2)
        .await
        .expect("consume");
    let mut stream = deliveries;
    let first_delivery = recv(&mut stream).await;
    let second_delivery = recv(&mut stream).await;
    assert_eq!(first_delivery.message.body, Bytes::from_static(b"q1-0"));
    assert_eq!(second_delivery.message.body, Bytes::from_static(b"q1-1"));
    expect_empty(&mut stream, Duration::from_millis(300)).await;

    control.set_prefetch(4).await.expect("set_prefetch");
    let third = recv(&mut stream).await;
    let fourth = recv(&mut stream).await;
    assert_eq!(third.message.body, Bytes::from_static(b"q1-2"));
    assert_eq!(fourth.message.body, Bytes::from_static(b"q1-3"));
    transport
        .ack(&first_delivery.delivery_tag)
        .await
        .expect("ack");
    transport
        .ack(&second_delivery.delivery_tag)
        .await
        .expect("ack");

    control.add_queue(&second).await.expect("add_queue");
    let fifth = recv(&mut stream).await;
    assert_eq!(fifth.message.body, Bytes::from_static(b"q2-0"));
    assert_eq!(fifth.routing_key, second);

    control.set_prefetch(2).await.expect("set_prefetch");

    expect_empty(&mut stream, Duration::from_millis(300)).await;
    transport.ack(&third.delivery_tag).await.expect("ack");
    expect_empty(&mut stream, Duration::from_millis(300)).await;
    transport.ack(&fourth.delivery_tag).await.expect("ack");
    let sixth = recv(&mut stream).await;
    assert_eq!(sixth.message.body, Bytes::from_static(b"q2-1"));

    control.cancel_queue(&second).await.expect("cancel_queue");
    expect_empty(&mut stream, Duration::from_millis(300)).await;

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
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("cancel");
    let queue = unique("q");
    setup(&*transport, &exchange, ExchangeType::Direct, &queue, "rk").await;
    transport
        .publish(PublishRequest::new(
            Some(&exchange),
            "rk",
            message("inflight"),
        ))
        .await
        .expect("publish");
    let Consumer {
        deliveries,
        control,
    } = transport
        .consume(&[CompactString::from(&queue)], 0)
        .await
        .expect("consume");
    let mut stream = deliveries;
    let delivery = recv(&mut stream).await;
    control.cancel().await.expect("cancel");
    assert!(
        tokio::time::timeout(WAIT, next(&mut stream))
            .await
            .expect("no timeout")
            .is_none()
    );
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 0);
    transport
        .ack(&delivery.delivery_tag)
        .await
        .expect("ack after cancel");
}

#[tokio::test]
async fn unbind_matches_kombu_and_keeps_broker_bindings() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("unbind");
    let first = unique("qa");
    let second = unique("qb");
    setup(
        &*transport,
        &exchange,
        ExchangeType::Topic,
        &first,
        "task.#",
    )
    .await;
    transport
        .declare_queue(&QueueDecl::new(&second))
        .await
        .expect("queue");
    transport
        .bind(&QueueBinding::new(&second, &exchange, "task.#"))
        .await
        .expect("binding");
    transport
        .unbind(&QueueBinding::new(&second, &exchange, "task.#"))
        .await
        .expect("unbind");

    transport
        .publish(PublishRequest::new(
            Some(&exchange),
            "task.one",
            message("routed"),
        ))
        .await
        .expect("publish");
    // Redis queue_unbind leaves broker bindings intact, verified against kombu 5.7.
    assert_eq!(transport.queue_size(&first).await.expect("size"), 1);
    assert_eq!(transport.queue_size(&second).await.expect("size"), 1);
}

#[tokio::test]
async fn delete_exchange_removes_empty_queues_and_bindings() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("del");
    let empty = unique("empty");
    let full = unique("full");
    transport
        .declare_exchange(&ExchangeDecl::new(&exchange, ExchangeType::Direct))
        .await
        .expect("exchange");
    for queue in [&empty, &full] {
        transport
            .declare_queue(&QueueDecl::new(queue))
            .await
            .expect("queue");
        transport
            .bind(&QueueBinding::new(queue, &exchange, queue.as_str()))
            .await
            .expect("binding");
    }
    transport
        .publish(PublishRequest::new(
            Some(&exchange),
            full.as_str(),
            message("keep"),
        ))
        .await
        .expect("publish");

    transport
        .delete_exchange(&exchange)
        .await
        .expect("delete_exchange");

    assert!(matches!(
        transport
            .declare_exchange(&ExchangeDecl {
                passive: true,
                ..ExchangeDecl::new(&exchange, ExchangeType::Direct)
            })
            .await,
        Err(Error::NotFound(_))
    ));
    // kombu uses queue_delete(if_empty=True), preserving nonempty queues and bindings.
    assert!(matches!(
        transport
            .declare_queue(&QueueDecl {
                passive: true,
                ..QueueDecl::new(&empty)
            })
            .await,
        Err(Error::NotFound(_))
    ));
    let info = transport
        .declare_queue(&QueueDecl {
            passive: true,
            ..QueueDecl::new(&full)
        })
        .await
        .expect("nonempty queue");
    assert_eq!(info.message_count, 1);
    transport
        .publish(PublishRequest::new(
            Some(&exchange),
            full.as_str(),
            message("lost"),
        ))
        .await
        .expect("publish");
    assert_eq!(transport.queue_size(&full).await.expect("size"), 2);
}

#[tokio::test]
async fn passive_declarations_match_kombu() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("passive");
    let queue = unique("q");
    assert!(matches!(
        transport
            .declare_exchange(&ExchangeDecl {
                passive: true,
                ..ExchangeDecl::new(&exchange, ExchangeType::Direct)
            })
            .await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        transport
            .declare_queue(&QueueDecl {
                passive: true,
                ..QueueDecl::new(&queue)
            })
            .await,
        Err(Error::NotFound(_))
    ));

    transport
        .declare_exchange(&ExchangeDecl::new(&exchange, ExchangeType::Topic))
        .await
        .expect("exchange");
    transport
        .declare_queue(&QueueDecl::new(&queue))
        .await
        .expect("queue");
    transport
        .publish(PublishRequest::new(None::<&str>, &queue, message("x")))
        .await
        .expect("publish");

    transport
        .declare_exchange(&ExchangeDecl {
            passive: true,
            ..ExchangeDecl::new(&exchange, ExchangeType::Direct)
        })
        .await
        .expect("passive exchange");
    let info = transport
        .declare_queue(&QueueDecl {
            passive: true,
            ..QueueDecl::new(&queue)
        })
        .await
        .expect("passive queue");
    assert_eq!(info.message_count, 1);
}

#[tokio::test]
async fn closing_transport_emits_connection_lost() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("close");
    let queue = unique("q");
    setup(&*transport, &exchange, ExchangeType::Direct, &queue, "rk").await;
    let mut stream = transport
        .consume(&[CompactString::from(&queue)], 0)
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
    assert!(matches!(
        transport.queue_size(&queue).await,
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn closing_a_transport_keeps_other_consumers_alive() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let other = connect(&url()).await;
    let exchange = unique("close2");
    let queue = unique("q");
    setup(&*transport, &exchange, ExchangeType::Direct, &queue, "rk").await;
    let mut stream = transport
        .consume(&[CompactString::from(&queue)], 0)
        .await
        .expect("consume")
        .into_stream();
    other.close().await.expect("close");
    other
        .publish(PublishRequest::new(Some(&exchange), "rk", message("still")))
        .await
        .expect_err("closed transport rejects publications");
    transport
        .publish(PublishRequest::new(Some(&exchange), "rk", message("alive")))
        .await
        .expect("publish");
    let delivery = recv(&mut stream).await;
    assert_eq!(delivery.message.body, Bytes::from_static(b"alive"));
    transport.ack(&delivery.delivery_tag).await.expect("ack");
}

#[tokio::test]
async fn close_restores_unacked_messages() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let other = connect(&url()).await;
    let queue = unique("restore");
    transport
        .publish(PublishRequest::new(
            None::<&str>,
            &queue,
            message("pending"),
        ))
        .await
        .expect("publish");
    let mut stream = transport
        .consume(&[CompactString::from(&queue)], 0)
        .await
        .expect("consume")
        .into_stream();
    let delivery = recv(&mut stream).await;
    assert_eq!(delivery.message.body, Bytes::from_static(b"pending"));
    // QoS.restore_unacked_once requeues on close.
    transport.close().await.expect("close");
    assert_eq!(other.queue_size(&queue).await.expect("size"), 1);
    let mut stream = other
        .consume(&[CompactString::from(&queue)], 0)
        .await
        .expect("consume")
        .into_stream();
    let restored = recv(&mut stream).await;
    assert_eq!(restored.message.body, Bytes::from_static(b"pending"));
    assert!(restored.redelivered);
    other.ack(&restored.delivery_tag).await.expect("ack");
}

#[tokio::test]
async fn visibility_timeout_restores_unacked_messages() {
    let _serial = serial().await;
    flush().await;
    let url = url();
    let first = connect_with(&url, &[("visibility_timeout", HeaderValue::Float(1.0))]).await;
    let second = connect_with(&url, &[("visibility_timeout", HeaderValue::Float(1.0))]).await;
    let queue = unique("visible");
    first
        .publish(PublishRequest::new(None::<&str>, &queue, message("slow")))
        .await
        .expect("publish");
    let mut stream = first
        .consume(&[CompactString::from(&queue)], 0)
        .await
        .expect("consume")
        .into_stream();
    let delivery = recv(&mut stream).await;
    assert_eq!(delivery.message.body, Bytes::from_static(b"slow"));

    let mut other = second
        .consume(&[CompactString::from(&queue)], 0)
        .await
        .expect("consume")
        .into_stream();
    let restored = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let delivery = recv(&mut other).await;
            if delivery.redelivered {
                return delivery;
            }
        }
    })
    .await
    .expect("message not restored");
    assert_eq!(restored.message.body, Bytes::from_static(b"slow"));
    second.ack(&restored.delivery_tag).await.expect("ack");
}

#[tokio::test]
async fn global_keyprefix_applies_to_every_key() {
    let _serial = serial().await;
    flush().await;
    let url = url();
    let transport = connect_with(&url, &[("global_keyprefix", HeaderValue::from("prefix:"))]).await;
    let queue = unique("prefixed");
    transport
        .publish(PublishRequest::new(None::<&str>, &queue, message("x")))
        .await
        .expect("publish");
    assert!(keys(&format!("prefix:{queue}")).await.len() == 1);
    assert!(keys(&queue).await.is_empty());
    let mut stream = transport
        .consume(&[CompactString::from(&queue)], 0)
        .await
        .expect("consume")
        .into_stream();
    let delivery = recv(&mut stream).await;
    assert_eq!(delivery.message.body, Bytes::from_static(b"x"));
    transport.ack(&delivery.delivery_tag).await.expect("ack");
}

#[tokio::test]
async fn publish_batch_enqueues_all() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("batch");
    let queue = unique("q");
    setup(&*transport, &exchange, ExchangeType::Direct, &queue, "rk").await;
    transport
        .publish_batch(vec![
            PublishRequest::new(Some(&exchange), "rk", message("a")),
            PublishRequest::new(Some(&exchange), "rk", message("b")),
            PublishRequest::new(Some(&exchange), "other", message("dropped")),
        ])
        .await
        .expect("publish_batch");
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 2);
    let first = transport
        .get(&queue, true)
        .await
        .expect("get")
        .expect("msg");
    let second = transport
        .get(&queue, true)
        .await
        .expect("get")
        .expect("msg");
    assert_eq!(first.message.body, Bytes::from_static(b"a"));
    assert_eq!(second.message.body, Bytes::from_static(b"b"));
}

#[tokio::test]
async fn dead_letter_queue_receives_unroutable_messages() {
    let _serial = serial().await;
    flush().await;
    let dead = unique("dead");
    let url = url();
    let transport = connect_with(
        &url,
        &[("deadletter_queue", HeaderValue::from(dead.as_str()))],
    )
    .await;
    let exchange = unique("dl");
    transport
        .declare_exchange(&ExchangeDecl::new(&exchange, ExchangeType::Direct))
        .await
        .expect("exchange");
    transport
        .publish(PublishRequest::new(
            Some(&exchange),
            "nothing",
            message("lost"),
        ))
        .await
        .expect("publish");
    assert_eq!(transport.queue_size(&dead).await.expect("size"), 1);
}

#[tokio::test]
async fn queue_expires_sets_ttl_on_lists() {
    let _serial = serial().await;
    flush().await;
    let transport = connect(&url()).await;
    let exchange = unique("ttl");
    let queue = unique("q");
    transport
        .declare_exchange(&ExchangeDecl::new(&exchange, ExchangeType::Direct))
        .await
        .expect("exchange");
    let mut arguments = Headers::new();
    arguments.insert("x-expires", HeaderValue::Int(120_000));
    transport
        .declare_queue(&QueueDecl {
            arguments,
            ..QueueDecl::new(&queue)
        })
        .await
        .expect("queue");
    transport
        .bind(&QueueBinding::new(&queue, &exchange, "rk"))
        .await
        .expect("binding");
    transport
        .publish(PublishRequest::new(Some(&exchange), "rk", message("x")))
        .await
        .expect("publish");
    let client = ::redis::Client::open(url()).expect("client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("conn");
    let ttl: i64 = ::redis::cmd("PTTL")
        .arg(&queue)
        .query_async(&mut conn)
        .await
        .expect("PTTL");
    assert!(ttl > 0 && ttl <= 120_000, "ttl {ttl}");

    let deadline: i64 = ::redis::cmd("ZCARD")
        .arg("_kombu.binding_deadlines")
        .query_async(&mut conn)
        .await
        .expect("ZCARD");
    assert_eq!(deadline, 1);
}

#[tokio::test]
async fn queue_order_strategy_selects_the_queue_order() {
    async fn scenario(url: &str, strategy: HeaderValue, first: &str, second: &str) -> Vec<String> {
        let transport = connect_with(url, &[("queue_order_strategy", strategy)]).await;
        for queue in [first, second] {
            transport
                .publish(PublishRequest::new(None::<&str>, queue, message("x")))
                .await
                .expect("publish");
        }
        let queues = [CompactString::from(second), CompactString::from(first)];
        let mut stream = transport
            .consume(&queues, 0)
            .await
            .expect("consume")
            .into_stream();
        let mut seen = Vec::new();
        for _ in 0..2 {
            let delivery = recv(&mut stream).await;
            seen.push(delivery.routing_key.to_string());
            transport.ack(&delivery.delivery_tag).await.expect("ack");
        }
        seen
    }

    let _serial = serial().await;
    flush().await;
    let url = url();
    let first = unique("order-a");
    let second = unique("order-b");

    assert_eq!(
        scenario(&url, HeaderValue::from("round_robin"), &first, &second).await,
        vec![second.clone(), first.clone()]
    );

    assert_eq!(
        scenario(&url, HeaderValue::from("priority"), &first, &second).await,
        vec![second.clone(), first.clone()]
    );

    let mut sorted = vec![first.clone(), second.clone()];
    sorted.sort();
    assert_eq!(
        scenario(&url, HeaderValue::from("sorted"), &first, &second).await,
        sorted
    );
}

// Checks no_ack fanout binding TTL, unacked tracking bypass and auto-delete cleanup.
#[tokio::test]
async fn no_ack_fanout_skips_unacked_and_deletes_auto_delete_queue() {
    let _serial = serial().await;
    flush().await;
    let url = url();
    let exchange = unique("celeryev");
    let queue = unique("celeryev.node");

    let transport = connect(&url).await;
    let mut arguments = Headers::new();
    arguments.insert("x-expires", HeaderValue::Int(60_000));
    arguments.insert("x-message-ttl", HeaderValue::Int(2_000));
    transport
        .declare_exchange(&ExchangeDecl::new(exchange.as_str(), ExchangeType::Fanout))
        .await
        .expect("exchange");
    transport
        .declare_queue(&QueueDecl {
            name: Some(queue.as_str().into()),
            auto_delete: true,
            exclusive: true,
            arguments: arguments.clone(),
            ..QueueDecl::default()
        })
        .await
        .expect("queue");
    transport
        .bind(&QueueBinding::new(
            queue.as_str(),
            exchange.as_str(),
            "worker.#",
        ))
        .await
        .expect("binding");

    let consume_queue = CompactString::from(queue.as_str());
    let consumer = transport
        .consume_no_ack(std::slice::from_ref(&consume_queue), 0)
        .await
        .expect("consume no_ack");
    let mut stream = consumer.deliveries;

    let _ = tokio::time::timeout(Duration::from_millis(200), next(&mut stream)).await;
    transport
        .publish(PublishRequest::new(
            Some(exchange.as_str()),
            "worker.online",
            message("{}"),
        ))
        .await
        .expect("publish");
    let delivery = recv(&mut stream).await;
    assert_eq!(delivery.routing_key.as_str(), "worker.online");
    // no_ack skips unacked tracking even before the consumer's best-effort acknowledgment.
    let client = ::redis::Client::open(url.clone()).expect("client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("connection");
    let unacked: i64 = ::redis::cmd("HLEN")
        .arg("unacked")
        .query_async(&mut conn)
        .await
        .expect("HLEN");
    assert_eq!(unacked, 0);

    transport.close().await.expect("close");
    let members: Vec<String> = ::redis::cmd("SMEMBERS")
        .arg(format!("_kombu.binding.{exchange}"))
        .query_async(&mut conn)
        .await
        .expect("SMEMBERS");
    assert!(
        !members.iter().any(|member| member.ends_with(&queue)),
        "binding remains: {members:?}"
    );
}
