//! Test del consumer del worker contro Redis e RabbitMQ reali.
//!
//! Richiedono i broker di test (`tools/dev/with-brokers cargo test`):
//! Redis `redis://127.0.0.1:6390` e AMQP `amqp://guest:guest@127.0.0.1:5673//`,
//! sovrascrivibili con `CELERY_RS_TEST_REDIS`/`CELERY_RS_TEST_AMQP`.

use std::time::Duration;

use bytes::Bytes;
use celery_transport::message::Message;
use celery_transport::{PublishRequest, QueueDecl, Transport, connect};
use celery_worker::{Waker, WorkerConsumer};

fn redis_url() -> String {
    std::env::var("CELERY_RS_TEST_REDIS").unwrap_or_else(|_| "redis://127.0.0.1:6390/0".into())
}

fn amqp_url() -> String {
    std::env::var("CELERY_RS_TEST_AMQP")
        .unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5673//".into())
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
}

async fn open(url: &str) -> Box<dyn Transport> {
    connect(url)
        .await
        .unwrap_or_else(|error| panic!("broker non disponibile su {url}: {error}"))
}

async fn publish(transport: &dyn Transport, queue: &str, body: &str) {
    transport
        .publish(PublishRequest::new(
            None::<&str>,
            queue,
            Message::new(Bytes::copy_from_slice(body.as_bytes()))
                .with_content_type("application/json")
                .with_content_encoding("utf-8"),
        ))
        .await
        .expect("publish");
}

async fn wait_for(mut condition: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    condition()
}

async fn consume_and_ack(url: &str) {
    let queue = unique("rs-worker-broker");
    let producer = open(url).await;
    producer
        .declare_queue(&QueueDecl::new(queue.as_str()))
        .await
        .expect("declare");
    for index in 0..3 {
        publish(producer.as_ref(), &queue, &format!("m{index}")).await;
    }

    let consumer = WorkerConsumer::new(open(url).await, Waker::new(), 2);
    consumer
        .start(Some(vec![queue.as_str().into()]))
        .await
        .expect("start");
    assert!(
        wait_for(|| consumer.pending() == 2, Duration::from_secs(10)).await,
        "prefetch non rispettato"
    );
    let first = consumer.drain(2);
    assert_eq!(first.len(), 2);
    for delivery in &first {
        consumer.ack(&delivery.delivery_tag).await.expect("ack");
    }
    assert!(wait_for(|| !consumer.drain(1).is_empty(), Duration::from_secs(10)).await);
    consumer.close().await.expect("close");
    producer.delete_queue(&queue).await.expect("delete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_consumer_delivers_with_prefetch() {
    consume_and_ack(&redis_url()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_consumer_delivers_with_prefetch() {
    consume_and_ack(&amqp_url()).await;
}

async fn consume_without_prefetch_limit(url: &str) {
    let queue = unique("rs-worker-broker-batch");
    let producer = open(url).await;
    producer
        .declare_queue(&QueueDecl::new(queue.as_str()))
        .await
        .expect("declare");
    for index in 0..5 {
        publish(producer.as_ref(), &queue, &format!("b{index}")).await;
    }
    let consumer = WorkerConsumer::new(open(url).await, Waker::new(), 10);
    consumer
        .start(Some(vec![queue.as_str().into()]))
        .await
        .expect("start");
    assert!(wait_for(|| consumer.pending() == 5, Duration::from_secs(10)).await);
    let deliveries = consumer.drain(5);
    assert_eq!(deliveries.len(), 5);
    for delivery in &deliveries {
        consumer.ack(&delivery.delivery_tag).await.expect("ack");
    }
    consumer.close().await.expect("close");
    producer.delete_queue(&queue).await.expect("delete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_consumer_batches_deliveries() {
    consume_without_prefetch_limit(&redis_url()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_consumer_batches_deliveries() {
    consume_without_prefetch_limit(&amqp_url()).await;
}

async fn loss_is_reported_once(url: &str) {
    let queue = unique("rs-worker-broker-loss");
    let producer = open(url).await;
    producer
        .declare_queue(&QueueDecl::new(queue.as_str()))
        .await
        .expect("declare");
    let transport = open(url).await;
    let consumer = WorkerConsumer::new(transport, Waker::new(), 5);
    consumer
        .start(Some(vec![queue.as_str().into()]))
        .await
        .expect("start");
    tokio::time::sleep(Duration::from_millis(100)).await;
    consumer.transport().close().await.expect("close");
    assert!(wait_for(|| consumer.take_error().is_some(), Duration::from_secs(10)).await);
    assert!(consumer.take_error().is_none());
    consumer.close().await.expect("close");
    producer.delete_queue(&queue).await.expect("delete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_consumer_reports_connection_loss() {
    loss_is_reported_once(&redis_url()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_consumer_reports_connection_loss() {
    loss_is_reported_once(&amqp_url()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_redelivers_unacked_after_reconnect() {
    let queue = unique("rs-worker-broker-redeliver");
    let producer = open(&amqp_url()).await;
    producer
        .declare_queue(&QueueDecl::new(queue.as_str()))
        .await
        .expect("declare");
    publish(producer.as_ref(), &queue, "lost").await;

    let transport = open(&amqp_url()).await;
    let consumer = WorkerConsumer::new(transport, Waker::new(), 1);
    consumer
        .start(Some(vec![queue.as_str().into()]))
        .await
        .expect("start");
    assert!(wait_for(|| consumer.pending() == 1, Duration::from_secs(10)).await);
    // Non confermato: chiudendo la connessione il broker lo rimette in coda.
    consumer.close().await.expect("close");

    let again = WorkerConsumer::new(open(&amqp_url()).await, Waker::new(), 1);
    again
        .start(Some(vec![queue.as_str().into()]))
        .await
        .expect("start");
    assert!(wait_for(|| again.pending() == 1, Duration::from_secs(10)).await);
    let redelivered = again.drain(1);
    assert_eq!(&redelivered[0].message.body[..], b"lost");
    again
        .ack(&redelivered[0].delivery_tag)
        .await
        .expect("ack");
    again.close().await.expect("close");
    producer.delete_queue(&queue).await.expect("delete");
}


async fn ack_after_cancel(url: &str) {
    let queue = unique("rs-worker-broker-cancel");
    let transport = open(url).await;
    transport
        .declare_queue(&QueueDecl::new(queue.as_str()))
        .await
        .expect("declare");
    publish(transport.as_ref(), &queue, "after-cancel").await;

    let consumer = WorkerConsumer::new(open(url).await, Waker::new(), 1);
    consumer
        .start(Some(vec![queue.as_str().into()]))
        .await
        .expect("start");
    assert!(wait_for(|| consumer.pending() == 1, Duration::from_secs(10)).await);
    let delivery = consumer.drain(1);
    consumer.cancel().await.expect("cancel");
    // Come `basic_cancel` di kombu: il tag già consegnato resta confermabile.
    consumer
        .ack(&delivery[0].delivery_tag)
        .await
        .expect("ack dopo cancel");
    assert_eq!(transport.queue_size(&queue).await.expect("size"), 0);
    consumer.close().await.expect("close");
    transport.delete_queue(&queue).await.expect("delete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_allows_ack_after_cancel() {
    ack_after_cancel(&redis_url()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_allows_ack_after_cancel() {
    ack_after_cancel(&amqp_url()).await;
}
