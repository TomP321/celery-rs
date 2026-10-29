//! RabbitMQ integration tests with unique entity names. CELERY_RS_TEST_AMQP selects the broker.

use std::time::Duration;

use bytes::Bytes;
use celery_transport::{
    ClientKind, ConnectConfig, Consumer, Delivery, DeliveryStream, DeliveryTag, Error,
    ExchangeDecl, ExchangeType, HeaderValue, Headers, Message, Properties, PublishRequest,
    QueueBinding, QueueDecl, QueueInfo, RetryPolicy, Transport, TransportOptions,
    connect_with_config,
};
use compact_str::CompactString;

const WAIT: Duration = Duration::from_secs(8);

fn broker_url() -> String {
    std::env::var("CELERY_RS_AMQP_URL")
        .or_else(|_| std::env::var("CELERY_RS_TEST_AMQP"))
        .unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5673//".to_string())
}

fn suffix() -> String {
    uuid::Uuid::new_v4().to_string().replace('-', "")
}

async fn transport_with(options: TransportOptions) -> Box<dyn Transport> {
    let retry = RetryPolicy {
        enabled: false,
        ..RetryPolicy::default()
    };
    connect_with_config(
        &broker_url(),
        &ConnectConfig {
            transport_options: options,
            retry_policy: retry,
        },
    )
    .await
    .unwrap_or_else(|error| panic!("amqp connection to {}: {error}", broker_url()))
}

async fn transport() -> Box<dyn Transport> {
    transport_with(TransportOptions::default()).await
}

async fn setup(
    transport: &dyn Transport,
    prefix: &str,
    kind: ExchangeType,
    routing_key: &str,
) -> (String, String) {
    let id = suffix();
    let exchange = format!("rs.{prefix}.ex.{id}");
    let queue = format!("rs.{prefix}.q.{id}");
    transport
        .declare_exchange(&ExchangeDecl::new(exchange.as_str(), kind))
        .await
        .expect("exchange");
    transport
        .declare_queue(&QueueDecl::new(queue.as_str()))
        .await
        .expect("queue");
    transport
        .bind(&QueueBinding::new(
            queue.as_str(),
            exchange.as_str(),
            routing_key,
        ))
        .await
        .expect("binding");
    (exchange, queue)
}

async fn next(stream: &mut DeliveryStream) -> Option<Result<Delivery, Error>> {
    std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
}

async fn recv(stream: &mut DeliveryStream) -> Delivery {
    tokio::time::timeout(WAIT, next(stream))
        .await
        .expect("timed out waiting for a message")
        .expect("stream ended")
        .expect("delivery error")
}

async fn wait_size(transport: &dyn Transport, queue: &str, expected: u64) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let size = transport.queue_size(queue).await.expect("size");
        if size == expected {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("queue {queue}: expected {expected} messages, got {size}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn declare_consumer(
    transport: &dyn Transport,
    queue: &str,
    prefetch: u16,
) -> (
    DeliveryStream,
    std::sync::Arc<dyn celery_transport::ConsumerControl>,
) {
    let consumer: Consumer = transport
        .consume(&[CompactString::from(queue)], prefetch)
        .await
        .expect("consume");
    (consumer.deliveries, consumer.control)
}

fn message(body: &str) -> Message {
    Message::new(Bytes::copy_from_slice(body.as_bytes()))
        .with_content_type("application/json")
        .with_content_encoding("utf-8")
}

fn message_with_properties(body: &str) -> (Message, Headers, Properties) {
    let mut headers = Headers::new();
    headers.insert("task", HeaderValue::from("add"));
    headers.insert("retries", HeaderValue::Int(2));
    headers.insert("flag", HeaderValue::Bool(true));
    headers.insert("ratio", HeaderValue::Float(0.5));
    headers.insert("nothing", HeaderValue::Null);
    headers.insert(
        "list",
        HeaderValue::List(vec![HeaderValue::Int(1), HeaderValue::from("two")]),
    );
    let mut inner = Headers::new();
    inner.insert("nested", HeaderValue::Bool(false));
    headers.insert("map", HeaderValue::Map(inner));

    let mut properties = Properties::new();
    properties.set_delivery_mode(2);
    properties.set_priority(3);
    properties.set_correlation_id("corr-1");
    properties.set_reply_to("reply-q");
    properties.insert("message_id", HeaderValue::from("m-1"));
    properties.insert("type", HeaderValue::from("task"));
    properties.insert("app_id", HeaderValue::from("app"));

    let message = message(body)
        .with_headers(headers.clone())
        .with_properties(properties.clone());
    (message, headers, properties)
}

#[tokio::test]
async fn publish_consume_ack_with_headers_and_properties() {
    let transport = transport().await;
    let (exchange, queue) = setup(transport.as_ref(), "pub", ExchangeType::Direct, "rk").await;
    let (msg, headers, properties) = message_with_properties("{\"x\": 1}");
    transport
        .publish(PublishRequest::new(
            Some(exchange.as_str()),
            "rk",
            msg.clone(),
        ))
        .await
        .expect("publish");
    wait_size(transport.as_ref(), &queue, 1).await;

    let (mut stream, _control) = declare_consumer(transport.as_ref(), &queue, 10).await;
    let delivery = recv(&mut stream).await;
    assert_eq!(delivery.message.body, msg.body);
    assert_eq!(delivery.message.content_type, msg.content_type);
    assert_eq!(delivery.message.content_encoding, msg.content_encoding);
    assert_eq!(delivery.message.headers, headers);

    for (key, value) in properties.iter() {
        assert_eq!(delivery.message.properties.get(key), Some(value), "{key}");
    }
    assert_eq!(delivery.message.properties.get("content_type"), None);
    assert_eq!(delivery.message.properties.get("application_headers"), None);
    assert_eq!(delivery.exchange, exchange);
    assert_eq!(delivery.routing_key, "rk");
    assert!(!delivery.redelivered);
    assert!(matches!(delivery.delivery_tag, DeliveryTag::Number(_)));

    transport.ack(&delivery.delivery_tag).await.expect("ack");
    wait_size(transport.as_ref(), &queue, 0).await;
    // Duplicate acknowledgments must not close the channel.
    transport
        .ack(&delivery.delivery_tag)
        .await
        .expect("duplicate ack");
    transport
        .declare_queue(&QueueDecl::new(queue.as_str()))
        .await
        .expect("channel still alive");
}

#[tokio::test]
async fn reject_without_requeue_discards() {
    let transport = transport().await;
    let (exchange, queue) = setup(transport.as_ref(), "rej", ExchangeType::Direct, "rk").await;
    transport
        .publish(PublishRequest::new(
            Some(exchange.as_str()),
            "rk",
            message("a"),
        ))
        .await
        .expect("publish");
    wait_size(transport.as_ref(), &queue, 1).await;
    let (mut stream, _control) = declare_consumer(transport.as_ref(), &queue, 0).await;
    let delivery = recv(&mut stream).await;
    transport
        .reject(&delivery.delivery_tag, false)
        .await
        .expect("reject");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), next(&mut stream))
            .await
            .is_err()
    );
    wait_size(transport.as_ref(), &queue, 0).await;
}

#[tokio::test]
async fn reject_with_requeue_marks_redelivered() {
    let transport = transport().await;
    let (exchange, queue) = setup(transport.as_ref(), "req", ExchangeType::Direct, "rk").await;
    transport
        .publish(PublishRequest::new(
            Some(exchange.as_str()),
            "rk",
            message("one"),
        ))
        .await
        .expect("publish");
    wait_size(transport.as_ref(), &queue, 1).await;
    let (mut stream, _control) = declare_consumer(transport.as_ref(), &queue, 0).await;
    let first = recv(&mut stream).await;
    assert_eq!(first.message.body, Bytes::from_static(b"one"));
    assert!(!first.redelivered);
    transport
        .reject(&first.delivery_tag, true)
        .await
        .expect("requeue");
    let second = recv(&mut stream).await;
    assert_eq!(second.message.body, Bytes::from_static(b"one"));
    assert!(second.redelivered);
    transport.ack(&second.delivery_tag).await.expect("ack");
    wait_size(transport.as_ref(), &queue, 0).await;

    // kombu rejects requeue requests for unknown tags.
    assert!(matches!(
        transport
            .reject(&DeliveryTag::Number(999_999_999), true)
            .await,
        Err(Error::UnknownDeliveryTag(_))
    ));

    transport
        .reject(&DeliveryTag::Number(999_999_999), false)
        .await
        .expect("reject unknown tag");
}

#[tokio::test]
async fn prefetch_can_be_changed_with_active_consumer() {
    let transport = transport().await;
    let (exchange, queue) = setup(transport.as_ref(), "pre", ExchangeType::Direct, "rk").await;
    for index in 0..5 {
        transport
            .publish(PublishRequest::new(
                Some(exchange.as_str()),
                "rk",
                message(&format!("m{index}")),
            ))
            .await
            .expect("publish");
    }
    wait_size(transport.as_ref(), &queue, 5).await;
    let (mut stream, control) = declare_consumer(transport.as_ref(), &queue, 2).await;
    let first = recv(&mut stream).await;
    let second = recv(&mut stream).await;
    assert_eq!(first.message.body, Bytes::from_static(b"m0"));
    assert_eq!(second.message.body, Bytes::from_static(b"m1"));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), next(&mut stream))
            .await
            .is_err()
    );

    // Match Celery's basic.qos call without recreating consumers.
    control.set_prefetch(4).await.expect("set_prefetch");
    transport.ack(&first.delivery_tag).await.expect("ack");
    let third = recv(&mut stream).await;
    assert_eq!(third.message.body, Bytes::from_static(b"m2"));

    control.set_prefetch(1).await.expect("set_prefetch");
    transport.ack(&second.delivery_tag).await.expect("ack");
    transport.ack(&third.delivery_tag).await.expect("ack");

    let fourth = recv(&mut stream).await;
    let fifth = recv(&mut stream).await;
    assert_eq!(fourth.message.body, Bytes::from_static(b"m3"));
    assert_eq!(fifth.message.body, Bytes::from_static(b"m4"));
    transport.ack(&fourth.delivery_tag).await.expect("ack");
    transport.ack(&fifth.delivery_tag).await.expect("ack");
    wait_size(transport.as_ref(), &queue, 0).await;

    // Quorum queues require per-consumer QoS.
    let id = suffix();
    let quorum = format!("rs.quorum.prefetch.{id}");
    let mut arguments = Headers::new();
    arguments.insert("x-queue-type", HeaderValue::from("quorum"));
    transport
        .declare_queue(&QueueDecl {
            name: Some(CompactString::from(quorum.as_str())),
            durable: true,
            arguments,
            ..QueueDecl::default()
        })
        .await
        .expect("quorum");
    for index in 0..3 {
        transport
            .publish(PublishRequest::new(
                None::<&str>,
                quorum.as_str(),
                message(&format!("q{index}")),
            ))
            .await
            .expect("publish");
    }
    wait_size(transport.as_ref(), &quorum, 3).await;
    let (mut stream, control) = declare_consumer(transport.as_ref(), &quorum, 1).await;
    let first = recv(&mut stream).await;
    control.set_prefetch(3).await.expect("qos on quorum");
    control.set_prefetch(1).await.expect("qos on quorum");
    transport.ack(&first.delivery_tag).await.expect("ack");
    transport
        .delete_queue(&quorum)
        .await
        .expect("delete quorum");
}

#[tokio::test]
async fn fanout_delivers_to_every_bound_queue() {
    let transport = transport().await;
    let id = suffix();
    let exchange = format!("rs.fan.ex.{id}");
    transport
        .declare_exchange(&ExchangeDecl::new(exchange.as_str(), ExchangeType::Fanout))
        .await
        .expect("exchange");
    let mut queues = Vec::new();
    for name in ["a", "b"] {
        let queue = format!("rs.fan.{name}.{id}");
        transport
            .declare_queue(&QueueDecl::new(queue.as_str()))
            .await
            .expect("queue");
        transport
            .bind(&QueueBinding::new(queue.as_str(), exchange.as_str(), ""))
            .await
            .expect("binding");
        queues.push(queue);
    }
    transport
        .publish(PublishRequest::new(
            Some(exchange.as_str()),
            "",
            message("bc"),
        ))
        .await
        .expect("publish");
    for queue in &queues {
        wait_size(transport.as_ref(), queue, 1).await;
        let (mut stream, _control) = declare_consumer(transport.as_ref(), queue, 0).await;
        let delivery = recv(&mut stream).await;
        assert_eq!(delivery.message.body, Bytes::from_static(b"bc"));
        assert_eq!(delivery.exchange, exchange);
        transport.ack(&delivery.delivery_tag).await.expect("ack");
    }
}

#[tokio::test]
async fn headers_exchange_routes_by_arguments() {
    let transport = transport().await;
    let id = suffix();
    let exchange = format!("rs.headers.ex.{id}");
    let queue = format!("rs.headers.q.{id}");
    transport
        .declare_exchange(&ExchangeDecl::new(exchange.as_str(), ExchangeType::Headers))
        .await
        .expect("exchange");
    transport
        .declare_queue(&QueueDecl::new(queue.as_str()))
        .await
        .expect("queue");
    let mut arguments = Headers::new();
    arguments.insert("x-match", HeaderValue::from("all"));
    arguments.insert("format", HeaderValue::from("pdf"));
    transport
        .bind(&QueueBinding {
            queue: CompactString::from(queue.as_str()),
            exchange: CompactString::from(exchange.as_str()),
            routing_key: CompactString::from(""),
            arguments,
        })
        .await
        .expect("binding");

    let mut matching = Headers::new();
    matching.insert("format", HeaderValue::from("pdf"));
    transport
        .publish(PublishRequest::new(
            Some(exchange.as_str()),
            "",
            message("pdf").with_headers(matching),
        ))
        .await
        .expect("publish");
    let mut other = Headers::new();
    other.insert("format", HeaderValue::from("txt"));
    transport
        .publish(PublishRequest::new(
            Some(exchange.as_str()),
            "",
            message("txt").with_headers(other),
        ))
        .await
        .expect("publish");
    wait_size(transport.as_ref(), &queue, 1).await;

    let (mut stream, _control) = declare_consumer(transport.as_ref(), &queue, 0).await;
    let delivery = recv(&mut stream).await;
    assert_eq!(delivery.message.body, Bytes::from_static(b"pdf"));
    transport.ack(&delivery.delivery_tag).await.expect("ack");
}

#[tokio::test]
async fn topic_routing_is_done_by_the_broker() {
    let transport = transport().await;
    let id = suffix();
    let exchange = format!("rs.topic.ex.{id}");
    transport
        .declare_exchange(&ExchangeDecl::new(exchange.as_str(), ExchangeType::Topic))
        .await
        .expect("exchange");
    let bindings = [
        ("all", "#"),
        ("created", "*.created"),
        ("account", "account.#"),
        ("exact", "account.1.created"),
    ];
    let mut queues = Vec::new();
    for (name, pattern) in bindings {
        let queue = format!("rs.topic.{name}.{id}");
        transport
            .declare_queue(&QueueDecl::new(queue.as_str()))
            .await
            .expect("queue");
        transport
            .bind(&QueueBinding::new(
                queue.as_str(),
                exchange.as_str(),
                pattern,
            ))
            .await
            .expect("binding");
        queues.push((name, queue));
    }

    for routing_key in [
        "account.1.created",
        "account.created",
        "account",
        "x.created",
    ] {
        transport
            .publish(PublishRequest::new(
                Some(exchange.as_str()),
                routing_key,
                message(routing_key),
            ))
            .await
            .expect("publish");
    }
    for (name, queue) in &queues {
        let expected = match *name {
            "all" => 4,

            "created" => 2,
            "account" => 3,
            "exact" => 1,
            other => panic!("unexpected queue {other}"),
        };
        wait_size(transport.as_ref(), queue, expected).await;
    }
}

#[tokio::test]
async fn anonymous_exchange_routes_to_queue_name() {
    let transport = transport().await;
    let queue = format!("rs.anon.{}", suffix());
    transport
        .declare_queue(&QueueDecl::new(queue.as_str()))
        .await
        .expect("queue");
    transport
        .publish(PublishRequest::new(
            None::<&str>,
            queue.as_str(),
            message("anon"),
        ))
        .await
        .expect("publish");
    let (mut stream, _control) = declare_consumer(transport.as_ref(), &queue, 0).await;
    let delivery = recv(&mut stream).await;
    assert_eq!(delivery.message.body, Bytes::from_static(b"anon"));
    assert_eq!(delivery.exchange, "");
    assert_eq!(delivery.routing_key, queue);
    transport.ack(&delivery.delivery_tag).await.expect("ack");
}

#[tokio::test]
async fn multiple_queues_are_all_consumed() {
    let transport = transport().await;
    let id = suffix();
    let queues = [format!("rs.multi.a.{id}"), format!("rs.multi.b.{id}")];
    for queue in &queues {
        transport
            .declare_queue(&QueueDecl::new(queue.as_str()))
            .await
            .expect("queue");
        for index in 0..2 {
            transport
                .publish(PublishRequest::new(
                    None::<&str>,
                    queue.as_str(),
                    message(&format!("{queue}-{index}")),
                ))
                .await
                .expect("publish");
        }
    }
    let names = [
        CompactString::from(queues[0].as_str()),
        CompactString::from(queues[1].as_str()),
    ];
    let mut stream = transport
        .consume(&names, 0)
        .await
        .expect("consume")
        .into_stream();
    let mut per_queue = std::collections::HashMap::new();
    for _ in 0..4 {
        let delivery = recv(&mut stream).await;
        *per_queue.entry(delivery.routing_key.clone()).or_insert(0) += 1;
        transport.ack(&delivery.delivery_tag).await.expect("ack");
    }
    assert_eq!(per_queue.get(queues[0].as_str()), Some(&2));
    assert_eq!(per_queue.get(queues[1].as_str()), Some(&2));
}

#[tokio::test]
async fn priority_queue_reorders_messages() {
    let transport = transport().await;
    let id = suffix();
    let exchange = format!("rs.prio.ex.{id}");
    let queue = format!("rs.prio.q.{id}");
    transport
        .declare_exchange(&ExchangeDecl::new(exchange.as_str(), ExchangeType::Direct))
        .await
        .expect("exchange");
    let mut arguments = Headers::new();
    arguments.insert("x-max-priority", HeaderValue::Int(10));
    transport
        .declare_queue(&QueueDecl {
            name: Some(CompactString::from(queue.as_str())),
            durable: false,
            auto_delete: false,
            exclusive: false,
            arguments,
            passive: false,
        })
        .await
        .expect("priority queue");
    transport
        .bind(&QueueBinding::new(queue.as_str(), exchange.as_str(), "rk"))
        .await
        .expect("binding");

    for (body, priority) in [("low", 1), ("high", 9), ("mid", 5), ("zero", 0)] {
        let mut properties = Properties::new();
        properties.set_priority(priority);
        let msg = Message::new(Bytes::copy_from_slice(body.as_bytes())).with_properties(properties);
        transport
            .publish(PublishRequest::new(Some(exchange.as_str()), "rk", msg))
            .await
            .expect("publish");
    }
    wait_size(transport.as_ref(), &queue, 4).await;
    let (mut stream, _control) = declare_consumer(transport.as_ref(), &queue, 0).await;
    let mut seen = Vec::new();
    for _ in 0..4 {
        let delivery = recv(&mut stream).await;
        seen.push((
            delivery.message.body.as_ref().to_vec(),
            delivery.message.properties.priority(),
        ));
        transport.ack(&delivery.delivery_tag).await.expect("ack");
    }
    assert_eq!(
        seen,
        vec![
            (b"high".to_vec(), Some(9)),
            (b"mid".to_vec(), Some(5)),
            (b"low".to_vec(), Some(1)),
            (b"zero".to_vec(), Some(0)),
        ]
    );
}

#[tokio::test]
async fn queue_arguments_expiry_ttl_and_quorum() {
    let transport = transport().await;
    let id = suffix();

    let ttl_queue = format!("rs.ttl.q.{id}");
    let mut arguments = Headers::new();
    arguments.insert("x-message-ttl", HeaderValue::Int(1500));
    transport
        .declare_queue(&QueueDecl {
            name: Some(CompactString::from(ttl_queue.as_str())),
            arguments,
            ..QueueDecl::default()
        })
        .await
        .expect("ttl queue");
    transport
        .publish(PublishRequest::new(
            None::<&str>,
            ttl_queue.as_str(),
            message("expiring"),
        ))
        .await
        .expect("publish");
    wait_size(transport.as_ref(), &ttl_queue, 1).await;
    wait_size(transport.as_ref(), &ttl_queue, 0).await;

    let expiry_queue = format!("rs.expires.q.{id}");
    let mut arguments = Headers::new();
    arguments.insert("x-expires", HeaderValue::Int(300));
    transport
        .declare_queue(&QueueDecl {
            name: Some(CompactString::from(expiry_queue.as_str())),
            arguments,
            ..QueueDecl::default()
        })
        .await
        .expect("expiring queue");
    let passive = QueueDecl {
        name: Some(CompactString::from(expiry_queue.as_str())),
        passive: true,
        ..QueueDecl::default()
    };
    transport
        .declare_queue(&passive)
        .await
        .expect("queue still present");
    // Declarations reset idle expiry; wait without querying and check once.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert!(matches!(
        transport.declare_queue(&passive).await,
        Err(Error::NotFound(_))
    ));

    // Quorum queues require durability.
    let quorum_queue = format!("rs.quorum.q.{id}");
    let mut arguments = Headers::new();
    arguments.insert("x-queue-type", HeaderValue::from("quorum"));
    let info: QueueInfo = transport
        .declare_queue(&QueueDecl {
            name: Some(CompactString::from(quorum_queue.as_str())),
            durable: true,
            arguments,
            ..QueueDecl::default()
        })
        .await
        .expect("quorum queue");
    assert_eq!(info.name, quorum_queue);
    transport
        .publish(PublishRequest::new(
            None::<&str>,
            quorum_queue.as_str(),
            message("quorum"),
        ))
        .await
        .expect("publish");
    let (mut stream, _control) = declare_consumer(transport.as_ref(), &quorum_queue, 0).await;
    let delivery = recv(&mut stream).await;
    assert_eq!(delivery.message.body, Bytes::from_static(b"quorum"));
    transport.ack(&delivery.delivery_tag).await.expect("ack");
    transport
        .delete_queue(&quorum_queue)
        .await
        .expect("delete quorum");
}

#[tokio::test]
async fn purge_size_and_delete() {
    let transport = transport().await;
    let (exchange, queue) = setup(transport.as_ref(), "purge", ExchangeType::Direct, "rk").await;
    for body in ["a", "b", "c"] {
        transport
            .publish(PublishRequest::new(
                Some(exchange.as_str()),
                "rk",
                message(body),
            ))
            .await
            .expect("publish");
    }
    wait_size(transport.as_ref(), &queue, 3).await;
    assert_eq!(transport.purge(&queue).await.expect("purge"), 3);
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 0);
    assert_eq!(transport.purge(&queue).await.expect("purge empty queue"), 0);

    transport
        .publish(PublishRequest::new(
            Some(exchange.as_str()),
            "rk",
            message("again"),
        ))
        .await
        .expect("publish");
    wait_size(transport.as_ref(), &queue, 1).await;
    transport.delete_queue(&queue).await.expect("delete");
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 0);

    transport
        .publish(PublishRequest::new(
            Some(exchange.as_str()),
            "rk",
            message("lost"),
        ))
        .await
        .expect("publish");
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 0);
    assert_eq!(
        transport.purge(&queue).await.expect("purge missing queue"),
        0
    );
}

#[tokio::test]
async fn connections_share_the_broker() {
    let producer = transport().await;
    let consumer = transport().await;
    let (exchange, queue) = setup(producer.as_ref(), "shared", ExchangeType::Direct, "rk").await;
    producer
        .publish(PublishRequest::new(
            Some(exchange.as_str()),
            "rk",
            message("shared"),
        ))
        .await
        .expect("publish");
    wait_size(consumer.as_ref(), &queue, 1).await;
    let (mut stream, _control) = declare_consumer(consumer.as_ref(), &queue, 0).await;
    let delivery = recv(&mut stream).await;
    assert_eq!(delivery.message.body, Bytes::from_static(b"shared"));
    consumer.ack(&delivery.delivery_tag).await.expect("ack");
    wait_size(producer.as_ref(), &queue, 0).await;
}

#[tokio::test]
async fn duplicate_bindings_deliver_once() {
    let transport = transport().await;
    let (exchange, queue) = setup(transport.as_ref(), "dup", ExchangeType::Topic, "#").await;
    transport
        .bind(&QueueBinding::new(queue.as_str(), exchange.as_str(), "a.#"))
        .await
        .expect("second binding");
    transport
        .publish(PublishRequest::new(
            Some(exchange.as_str()),
            "a.b",
            message("once"),
        ))
        .await
        .expect("publish");
    wait_size(transport.as_ref(), &queue, 1).await;
}

#[tokio::test]
async fn declarations_and_passive_checks() {
    let transport = transport().await;
    let id = suffix();
    let exchange = format!("rs.decl.ex.{id}");
    let queue = format!("rs.decl.q.{id}");

    assert!(matches!(
        transport
            .declare_exchange(&ExchangeDecl {
                name: CompactString::from(exchange.as_str()),
                kind: ExchangeType::Direct,
                passive: true,
                ..ExchangeDecl::default()
            })
            .await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        transport
            .declare_queue(&QueueDecl {
                name: Some(CompactString::from(queue.as_str())),
                passive: true,
                ..QueueDecl::default()
            })
            .await,
        Err(Error::NotFound(_))
    ));

    transport
        .declare_exchange(&ExchangeDecl::new(exchange.as_str(), ExchangeType::Direct))
        .await
        .expect("exchange");
    transport
        .declare_queue(&QueueDecl::new(queue.as_str()))
        .await
        .expect("queue");
    transport
        .declare_exchange(&ExchangeDecl {
            name: CompactString::from(exchange.as_str()),
            kind: ExchangeType::Direct,
            passive: true,
            ..ExchangeDecl::default()
        })
        .await
        .expect("passive exchange");
    // Passive declarations do not validate exchange type, matching AMQP and kombu.
    transport
        .declare_exchange(&ExchangeDecl {
            name: CompactString::from(exchange.as_str()),
            kind: ExchangeType::Topic,
            passive: true,
            ..ExchangeDecl::default()
        })
        .await
        .expect("type ignored in passive declaration");
    let info = transport
        .declare_queue(&QueueDecl {
            name: Some(CompactString::from(queue.as_str())),
            passive: true,
            ..QueueDecl::default()
        })
        .await
        .expect("passive queue");
    assert_eq!(info.message_count, 0);

    match transport
        .declare_exchange(&ExchangeDecl::new(exchange.as_str(), ExchangeType::Topic))
        .await
    {
        Err(Error::PreconditionFailed {
            reply_code,
            reply_text,
            method_sig,
            method_name,
        }) => {
            assert_eq!(reply_code, 406);
            assert_eq!(method_sig, (40, 10));
            assert_eq!(method_name, "Exchange.declare");
            assert!(reply_text.contains("PRECONDITION_FAILED"), "{reply_text}");
        }
        other => panic!("expected PreconditionFailed, got {other:?}"),
    }

    assert!(matches!(
        transport
            .bind(&QueueBinding::new(queue.as_str(), "missing.exchange", "rk"))
            .await,
        Err(Error::UnknownExchange(_))
    ));

    transport
        .unbind(&QueueBinding::new(
            queue.as_str(),
            exchange.as_str(),
            "nope",
        ))
        .await
        .expect("unbind no-op");
    transport
        .consume(&[], 0)
        .await
        .err()
        .expect("empty consume");
}

#[tokio::test]
async fn delete_exchange_keeps_queues() {
    let transport = transport().await;
    let id = suffix();
    let exchange = format!("rs.del.ex.{id}");
    let empty = format!("rs.del.empty.{id}");
    let full = format!("rs.del.full.{id}");
    transport
        .declare_exchange(&ExchangeDecl::new(exchange.as_str(), ExchangeType::Direct))
        .await
        .expect("exchange");
    for (queue, routing_key) in [(&empty, "empty"), (&full, "full")] {
        transport
            .declare_queue(&QueueDecl::new(queue.as_str()))
            .await
            .expect("queue");
        transport
            .bind(&QueueBinding::new(
                queue.as_str(),
                exchange.as_str(),
                routing_key,
            ))
            .await
            .expect("binding");
    }
    transport
        .publish(PublishRequest::new(
            Some(exchange.as_str()),
            "full",
            message("keep"),
        ))
        .await
        .expect("publish");
    wait_size(transport.as_ref(), &full, 1).await;

    transport
        .delete_exchange(&exchange)
        .await
        .expect("delete exchange");
    assert!(matches!(
        transport
            .declare_exchange(&ExchangeDecl {
                name: CompactString::from(exchange.as_str()),
                kind: ExchangeType::Direct,
                passive: true,
                ..ExchangeDecl::default()
            })
            .await,
        Err(Error::NotFound(_))
    ));
    // AMQP preserves queues, including empty ones, when deleting exchanges.
    let info = transport
        .declare_queue(&QueueDecl {
            name: Some(CompactString::from(empty.as_str())),
            passive: true,
            ..QueueDecl::default()
        })
        .await
        .expect("empty queue remains");
    assert_eq!(info.message_count, 0);
    let info = transport
        .declare_queue(&QueueDecl {
            name: Some(CompactString::from(full.as_str())),
            passive: true,
            ..QueueDecl::default()
        })
        .await
        .expect("nonempty queue remains");
    assert_eq!(info.message_count, 1);
    transport
        .publish(PublishRequest::new(
            Some(exchange.as_str()),
            "full",
            message("lost"),
        ))
        .await
        .expect("publish to deleted exchange");
    assert_eq!(transport.queue_size(&full).await.expect("size"), 1);
    // Broker exchange deletion is idempotent.
    transport
        .delete_exchange("missing.exchange.for.test")
        .await
        .expect("deleting a missing exchange is a no-op");
}

#[tokio::test]
async fn prefetch_global_changes_take_effect() {
    let mut options = TransportOptions::default();
    options.extra.insert("qos_global", HeaderValue::Bool(true));
    let transport = transport_with(options).await;
    let (exchange, queue) =
        setup(transport.as_ref(), "qosglobal", ExchangeType::Direct, "rk").await;
    for index in 0..6 {
        transport
            .publish(PublishRequest::new(
                Some(exchange.as_str()),
                "rk",
                message(&format!("m{index}")),
            ))
            .await
            .expect("publish");
    }
    wait_size(transport.as_ref(), &queue, 6).await;
    let (mut stream, control) = declare_consumer(transport.as_ref(), &queue, 2).await;
    let first = recv(&mut stream).await;
    let second = recv(&mut stream).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), next(&mut stream))
            .await
            .is_err()
    );

    // Global QoS applies immediately to the channel.
    control.set_prefetch(4).await.expect("set_prefetch");
    let third = recv(&mut stream).await;
    let fourth = recv(&mut stream).await;
    assert_eq!(third.message.body, Bytes::from_static(b"m2"));
    assert_eq!(fourth.message.body, Bytes::from_static(b"m3"));

    control.set_prefetch(1).await.expect("set_prefetch");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), next(&mut stream))
            .await
            .is_err()
    );
    transport.ack(&first.delivery_tag).await.expect("ack");
    transport.ack(&second.delivery_tag).await.expect("ack");
    transport.ack(&third.delivery_tag).await.expect("ack");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), next(&mut stream))
            .await
            .is_err()
    );
    transport.ack(&fourth.delivery_tag).await.expect("ack");
    let fifth = recv(&mut stream).await;
    assert_eq!(fifth.message.body, Bytes::from_static(b"m4"));
    transport.ack(&fifth.delivery_tag).await.expect("ack");
    let sixth = recv(&mut stream).await;
    assert_eq!(sixth.message.body, Bytes::from_static(b"m5"));
    transport.ack(&sixth.delivery_tag).await.expect("ack");
    wait_size(transport.as_ref(), &queue, 0).await;
}

#[tokio::test]
async fn get_honours_no_ack_and_requeue() {
    let transport = transport().await;
    let (exchange, queue) = setup(transport.as_ref(), "get", ExchangeType::Direct, "rk").await;
    for body in ["a", "b"] {
        transport
            .publish(PublishRequest::new(
                Some(exchange.as_str()),
                "rk",
                message(body),
            ))
            .await
            .expect("publish");
    }
    wait_size(transport.as_ref(), &queue, 2).await;

    let first = transport
        .get(&queue, true)
        .await
        .expect("get")
        .expect("message");
    assert_eq!(first.message.body, Bytes::from_static(b"a"));
    assert_eq!(first.exchange, exchange);
    assert_eq!(first.routing_key, "rk");
    transport.ack(&first.delivery_tag).await.expect("ack no-op");

    let second = transport
        .get(&queue, false)
        .await
        .expect("get")
        .expect("message");
    assert_eq!(second.message.body, Bytes::from_static(b"b"));
    transport
        .reject(&second.delivery_tag, true)
        .await
        .expect("requeue");
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
            .get("missing.queue", true)
            .await
            .expect("get")
            .is_none()
    );
}

#[tokio::test]
async fn consumer_control_add_cancel_queue_and_cancel() {
    let transport = transport().await;
    let id = suffix();
    let queue_one = format!("rs.ctrl.1.{id}");
    let queue_two = format!("rs.ctrl.2.{id}");
    for queue in [&queue_one, &queue_two] {
        transport
            .declare_queue(&QueueDecl::new(queue.as_str()))
            .await
            .expect("queue");
    }
    for index in 0..2 {
        transport
            .publish(PublishRequest::new(
                None::<&str>,
                queue_one.as_str(),
                message(&format!("one-{index}")),
            ))
            .await
            .expect("publish");
        transport
            .publish(PublishRequest::new(
                None::<&str>,
                queue_two.as_str(),
                message(&format!("two-{index}")),
            ))
            .await
            .expect("publish");
    }
    wait_size(transport.as_ref(), &queue_one, 2).await;
    wait_size(transport.as_ref(), &queue_two, 2).await;

    let consumer = transport
        .consume(&[CompactString::from(queue_one.as_str())], 2)
        .await
        .expect("consume");
    let mut stream = consumer.deliveries;
    let control = consumer.control;
    let first = recv(&mut stream).await;
    let second = recv(&mut stream).await;
    assert_eq!(first.routing_key, queue_one);
    assert_eq!(second.routing_key, queue_one);
    transport.ack(&first.delivery_tag).await.expect("ack");
    transport.ack(&second.delivery_tag).await.expect("ack");

    control.add_queue(&queue_two).await.expect("add_queue");
    let third = recv(&mut stream).await;
    assert_eq!(third.routing_key, queue_two);
    transport.ack(&third.delivery_tag).await.expect("ack");

    control
        .cancel_queue(&queue_two)
        .await
        .expect("cancel_queue");
    transport
        .publish(PublishRequest::new(
            None::<&str>,
            queue_two.as_str(),
            message("not-consumed"),
        ))
        .await
        .expect("publish");
    wait_size(transport.as_ref(), &queue_two, 2).await;

    // Cancellation preserves pending acknowledgments.
    transport
        .publish(PublishRequest::new(
            None::<&str>,
            queue_one.as_str(),
            message("inflight"),
        ))
        .await
        .expect("publish");
    let inflight = recv(&mut stream).await;
    control.cancel().await.expect("cancel");
    assert!(
        tokio::time::timeout(WAIT, next(&mut stream))
            .await
            .expect("no timeout")
            .is_none()
    );
    transport
        .ack(&inflight.delivery_tag)
        .await
        .expect("ack after cancel");
    assert!(matches!(control.set_prefetch(1).await, Err(Error::Closed)));
    assert!(matches!(
        control.add_queue(&queue_one).await,
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn publish_batch_enqueues_all() {
    let transport = transport().await;
    let (exchange, queue) = setup(transport.as_ref(), "batch", ExchangeType::Direct, "rk").await;
    transport
        .publish_batch(vec![
            PublishRequest::new(Some(exchange.as_str()), "rk", message("a")),
            PublishRequest::new(Some(exchange.as_str()), "rk", message("b")),
            PublishRequest::new(Some(exchange.as_str()), "other", message("dropped")),
        ])
        .await
        .expect("publish_batch");
    wait_size(transport.as_ref(), &queue, 2).await;
    let (mut stream, _control) = declare_consumer(transport.as_ref(), &queue, 0).await;
    let first = recv(&mut stream).await;
    let second = recv(&mut stream).await;
    assert_eq!(first.message.body, Bytes::from_static(b"a"));
    assert_eq!(second.message.body, Bytes::from_static(b"b"));
}

#[tokio::test]
async fn publisher_confirms_work() {
    let mut options = TransportOptions::default();
    options
        .extra
        .insert("confirm_publish", HeaderValue::Bool(true));
    let transport = transport_with(options).await;
    let (exchange, queue) = setup(transport.as_ref(), "confirm", ExchangeType::Direct, "rk").await;
    for index in 0..20 {
        transport
            .publish(PublishRequest::new(
                Some(exchange.as_str()),
                "rk",
                message(&format!("c{index}")),
            ))
            .await
            .expect("confirmed publish");
    }
    wait_size(transport.as_ref(), &queue, 20).await;
    assert_eq!(transport.purge(&queue).await.expect("purge"), 20);
}

#[tokio::test]
async fn close_emits_connection_lost_once_and_stream_ends() {
    let transport = transport().await;
    let (exchange, queue) = setup(transport.as_ref(), "lost", ExchangeType::Direct, "rk").await;
    transport
        .publish(PublishRequest::new(
            Some(exchange.as_str()),
            "rk",
            message("inflight"),
        ))
        .await
        .expect("publish");
    wait_size(transport.as_ref(), &queue, 1).await;
    let (mut stream, _control) = declare_consumer(transport.as_ref(), &queue, 0).await;
    let _delivery = recv(&mut stream).await;

    transport.close().await.expect("close");
    let first = tokio::time::timeout(WAIT, next(&mut stream))
        .await
        .expect("no timeout")
        .expect("no None before ConnectionLost");
    assert!(matches!(first, Err(Error::ConnectionLost(_))));
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
async fn connection_failure_is_reported_without_retry() {
    let retry = RetryPolicy {
        enabled: false,
        ..RetryPolicy::default()
    };
    let error = connect_with_config(
        "amqp://guest:guest@127.0.0.1:1//",
        &ConnectConfig {
            transport_options: TransportOptions::default(),
            retry_policy: retry,
        },
    )
    .await
    .err()
    .expect("connection refused");
    assert!(matches!(
        error,
        Error::Connect {
            client: ClientKind::Amqp,
            errno: Some(111),
            ..
        }
    ));
    assert_eq!(
        error.client_message().as_deref(),
        Some("[Errno 111] Connection refused")
    );
}

#[tokio::test]
async fn no_ack_consumer_acks_on_delivery_and_supports_queue_changes() {
    let transport = transport().await;
    let id = suffix();
    let queue_one = format!("rs.noack.1.{id}");
    let queue_two = format!("rs.noack.2.{id}");
    for queue in [&queue_one, &queue_two] {
        transport
            .declare_queue(&QueueDecl::new(queue.as_str()))
            .await
            .expect("queue");
    }
    for queue in [&queue_one, &queue_two] {
        transport
            .publish(PublishRequest::new(
                None::<&str>,
                queue.as_str(),
                message("noack"),
            ))
            .await
            .expect("publish");
    }
    wait_size(transport.as_ref(), &queue_one, 1).await;
    wait_size(transport.as_ref(), &queue_two, 1).await;

    let consumer = transport
        .consume_no_ack(&[CompactString::from(queue_one.as_str())], 0)
        .await
        .expect("consume_no_ack");
    let mut stream = consumer.deliveries;
    let control = consumer.control;
    let first = recv(&mut stream).await;
    assert_eq!(first.routing_key, queue_one);

    wait_size(transport.as_ref(), &queue_one, 0).await;

    transport.ack(&first.delivery_tag).await.expect("ack no-op");

    control.add_queue(&queue_two).await.expect("add_queue");
    let second = recv(&mut stream).await;
    assert_eq!(second.routing_key, queue_two);
    wait_size(transport.as_ref(), &queue_two, 0).await;

    control
        .cancel_queue(&queue_two)
        .await
        .expect("cancel_queue");
    control.cancel().await.expect("cancel");
    assert!(
        tokio::time::timeout(WAIT, next(&mut stream))
            .await
            .expect("no timeout")
            .is_none()
    );
}

#[tokio::test]
async fn control_replies_reuse_channel_without_changing_regular_consumers() {
    let transport = transport().await;
    let queue = CompactString::from(format!("rs.control.reuse.{}", suffix()));
    transport
        .declare_queue(&QueueDecl::new(queue.as_str()))
        .await
        .unwrap();
    let mut channel = None;
    for _ in 0..3 {
        let mut consumer = transport.consume_control_reply(&queue).await.unwrap();
        transport
            .publish(PublishRequest::new(
                None::<&str>,
                queue.as_str(),
                message("reply"),
            ))
            .await
            .unwrap();
        let delivery = recv(&mut consumer.deliveries).await;
        let DeliveryTag::Number(tag) = delivery.delivery_tag else {
            panic!("tag AMQP")
        };
        let current = tag >> 40;
        if let Some(previous) = channel {
            assert_eq!(previous, current);
        }
        channel = Some(current);
        consumer.control.cancel().await.unwrap();
    }
    let mut regular = transport
        .consume(std::slice::from_ref(&queue), 1)
        .await
        .unwrap();
    transport
        .publish(PublishRequest::new(
            None::<&str>,
            queue.as_str(),
            message("task"),
        ))
        .await
        .unwrap();
    let delivery = recv(&mut regular.deliveries).await;
    let DeliveryTag::Number(tag) = &delivery.delivery_tag else {
        panic!("tag AMQP")
    };
    assert_ne!(channel, Some(tag >> 40));
    transport.ack(&delivery.delivery_tag).await.unwrap();
    regular.control.cancel().await.unwrap();
    transport.delete_queue(&queue).await.unwrap();
}
