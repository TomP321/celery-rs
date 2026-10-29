"""Compare AMQP transport behavior with kombu using unique entities and real RabbitMQ."""

from __future__ import annotations

import base64
import datetime
import decimal
import json
import os
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import pytest
from kombu import Connection as KombuConnection
from kombu import Consumer, Exchange, Producer, Queue
from kombu.exceptions import ChannelError

import _celery_rs as rs

AMQP_URL = os.environ.get(
    "CELERY_RS_TEST_AMQP", os.environ.get('CELERY_RS_AMQP_URL', 'amqp://guest:guest@127.0.0.1:5673//')
)
RABBIT_API = os.environ.get(
    "CELERY_RS_TEST_RABBIT_API", os.environ.get('CELERY_RS_RABBIT_API', 'http://127.0.0.1:15673/api')
)
RABBIT_AUTH = "Basic " + base64.b64encode(b"guest:guest").decode()

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT / "tools" / "compat") not in sys.path:
    sys.path.insert(0, str(ROOT / "tools" / "compat"))


def api(method: str, path: str, data=None):
    request = urllib.request.Request(
        f"{RABBIT_API}{path}",
        method=method,
        data=json.dumps(data).encode() if data is not None else None,
        headers={
            "Authorization": RABBIT_AUTH,
            "content-type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        raw = response.read()
        return json.loads(raw) if raw else None


@pytest.fixture(autouse=True)
def clean_broker():
    try:
        for queue in api("GET", "/queues/%2F"):
            if queue["name"].startswith("rs."):
                api(
                    "DELETE",
                    f"/queues/%2F/{urllib.parse.quote(queue['name'], safe='')}",
                )
        for exchange in api("GET", "/exchanges/%2F"):
            if exchange["name"].startswith("rs."):
                api(
                    "DELETE",
                    f"/exchanges/%2F/{urllib.parse.quote(exchange['name'], safe='')}",
                )
    except OSError:
        # AMQP remains reachable; management cleanup is best effort.
        pass
    yield
    try:
        for queue in api("GET", "/queues/%2F"):
            if queue["name"].startswith("rs."):
                api(
                    "DELETE",
                    f"/queues/%2F/{urllib.parse.quote(queue['name'], safe='')}",
                )
        for exchange in api("GET", "/exchanges/%2F"):
            if exchange["name"].startswith("rs."):
                api(
                    "DELETE",
                    f"/exchanges/%2F/{urllib.parse.quote(exchange['name'], safe='')}",
                )
    except OSError:
        pass


def unique(prefix: str) -> str:
    return f"rs.{prefix}.{uuid.uuid4().hex}"


def kombu_declare(channel, exchange, *queues):
    if exchange is not None:
        exchange(channel).declare()
    for queue in queues:
        queue(channel).declare()
    return channel


def rs_declare(connection, exchange, *queues):
    if exchange is not None:
        connection.declare_exchange(
            exchange.name,
            exchange.type,
            durable=exchange.durable,
            auto_delete=exchange.auto_delete,
            arguments=exchange.arguments or None,
        )
    for queue in queues:
        connection.declare_queue(
            queue.name,
            durable=queue.durable,
            auto_delete=queue.auto_delete,
            exclusive=queue.exclusive,
            arguments=queue.queue_arguments or None,
        )
        if exchange is not None:
            connection.bind(
                queue.name,
                exchange.name,
                queue.routing_key or "",
                arguments=queue.binding_arguments or None,
            )
    return connection


def normalize_delivery_info(info: dict) -> dict:
    """Exclude delivery tags and message counts because they depend on the channel."""
    return {
        key: value
        for key, value in dict(info).items()
        if key not in {"delivery_tag", "message_count"}
    }


def body_bytes(body) -> bytes:
    """Normalize kombu's decoded JSON body to bytes."""
    return body.encode("utf-8") if isinstance(body, str) else body


def kombu_message(message) -> tuple:
    if message is None:
        raise AssertionError("missing kombu message")
    return (
        body_bytes(message.body),
        dict(message.headers),
        dict(message.properties),
        normalize_delivery_info(message.delivery_info),
    )


def rs_message(delivery) -> tuple:
    body, headers, properties, delivery_info, _tag = delivery
    return (
        body,
        dict(headers),
        dict(properties),
        normalize_delivery_info(delivery_info),
    )


def collect_rs(connection, queues, count, *, timeout=2.0, prefetch=0, ack=True):
    collected = []
    deadline = time.monotonic() + timeout
    while len(collected) < count:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        batch = connection.drain(queues, timeout=remaining, prefetch=prefetch)
        if not batch:
            break
        for delivery in batch:
            collected.append(delivery)
            if ack:
                connection.ack(delivery[4])
    return collected


def collect_kombu(connection, queues, count, *, timeout=2.0, prefetch=0, ack=True):
    received = []

    def on_message(body, message):
        received.append(message)
        if ack:
            message.ack()

    consumer = Consumer(
        connection.default_channel,
        queues=queues,
        callbacks=[on_message],
        prefetch_count=prefetch,
    )
    consumer.consume()
    deadline = time.monotonic() + timeout
    while len(received) < count and time.monotonic() < deadline:
        try:
            connection.drain_events(timeout=0.02)
        except TimeoutError:
            pass
    return received


def take_rs(connection, queue_name, *, timeout=2.0):
    batch = connection.drain([queue_name], timeout=timeout, prefetch=1)
    assert len(batch) == 1, batch
    return batch[0]


def wait_size(connection, queue_name, expected, *, timeout=5.0):
    """Wait for the broker to route asynchronous publications."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        size = connection.size(queue_name)
        if size == expected:
            return
        time.sleep(0.02)
    raise AssertionError(
        f"queue {queue_name}: expected {expected} messages, got {connection.size(queue_name)}"
    )


def publish_both(kombu_producer, ours, body, exchange, routing_key, **kwargs):
    """Compare messages from kombu and Rust in queue order."""
    kombu_producer.publish(
        body, exchange=exchange, routing_key=routing_key, **kwargs
    )
    headers = kwargs.pop("headers", None)
    properties = {
        key: kwargs[key]
        for key in (
            "correlation_id",
            "reply_to",
            "message_id",
            "expiration",
            "timestamp",
        )
        if key in kwargs
    }
    # kombu converts expiration from seconds; Rust accepts the AMQP millisecond string.

    if isinstance(properties.get("expiration"), (int, float)):
        properties["expiration"] = str(int(properties["expiration"] * 1000))
    for key in ("type", "app_id"):
        if key in kwargs:
            properties[key] = kwargs[key]
    ours.publish(
        body,
        exchange=exchange.name,
        routing_key=routing_key,
        headers=headers,
        properties=properties or None,
        content_type=kwargs.get("content_type"),
        content_encoding=kwargs.get("content_encoding"),
        priority=kwargs.get("priority"),
        delivery_mode=kwargs.get("delivery_mode"),
    )


def test_publish_consume_ack_interop():
    exchange = Exchange(unique("direct"), type="direct", durable=False)
    queue = Queue(unique("q"), exchange=exchange, routing_key="rk", durable=False)

    kombu_conn = KombuConnection(AMQP_URL)
    channel = kombu_declare(kombu_conn.default_channel, exchange, queue)
    producer = Producer(channel)

    ours = rs.Connection(AMQP_URL)
    rs_declare(ours, exchange, queue)

    body = b'{"args": [1, 2]}'
    headers = {
        "task": "add",
        "retries": 2,
        "flag": True,
        "ratio": 0.5,
        "nothing": None,
        "list": [1, "two", {"x": 3}],
        "dict": {"a": [True, False]},
    }
    kwargs = {
        "headers": headers,
        "content_type": "application/json",
        "content_encoding": "utf-8",
        "priority": 4,
        "correlation_id": "abc",
        "reply_to": "reply",
        "message_id": "m1",
        "expiration": 60,
        "timestamp": 1_700_000_000,
        "type": "task",
        "app_id": "app",
    }
    publish_both(producer, ours, body, exchange, "rk", **kwargs)


    expected = kombu_message(queue(channel).get(no_ack=False))
    delivery = take_rs(ours, queue.name)
    received = rs_message(delivery)
    assert received == expected
    assert received[0] == body
    assert received[1] == headers
    assert received[2]["content_type"] == "application/json"
    assert received[2]["content_encoding"] == "utf-8"
    assert received[2]["priority"] == 4
    assert received[2]["delivery_mode"] == 2
    assert received[2]["correlation_id"] == "abc"
    assert received[2]["reply_to"] == "reply"
    assert received[2]["message_id"] == "m1"
    assert received[2]["expiration"] == "60000"
    assert received[2]["timestamp"] == 1_700_000_000
    assert received[2]["type"] == "task"
    assert received[2]["app_id"] == "app"
    assert received[3] == {
        "exchange": exchange.name,
        "routing_key": "rk",
        "redelivered": False,
    }
    assert isinstance(delivery[4], int)
    ours.ack(delivery[4])
    assert ours.size(queue.name) == 0
    ours.close()
    kombu_conn.close()


def test_header_types_decoded_like_py_amqp():
    exchange = Exchange(unique("types"), type="direct", durable=False)
    queue = Queue(unique("q"), exchange=exchange, routing_key="rk", durable=False)
    kombu_conn = KombuConnection(AMQP_URL)
    channel = kombu_declare(kombu_conn.default_channel, exchange, queue)
    producer = Producer(channel)
    ours = rs.Connection(AMQP_URL)
    rs_declare(ours, exchange, queue)

    headers = {
        "none": None,
        "true": True,
        "false": False,
        "int": -42,
        "big": 2**40,
        "float": 1.5,
        "text": "città €",
        "bytes": b"\x00\xff\x80",
        "list": [1, "two", None, {"three": 3}],
        "dict": {"a": [True, False], "b": b"raw"},
        "when": datetime.datetime(2020, 1, 2, 3, 4, 5),
        "price": decimal.Decimal("1.25"),
        "negative_price": decimal.Decimal("-0.05"),
    }


    ours.publish(b"body", exchange=exchange.name, routing_key="rk", headers=headers)
    wait_size(ours, queue.name, 1)
    received = queue(channel).get(no_ack=True)
    # Capture py-amqp normalization through a kombu-to-kombu reference publication.

    producer.publish(b"body", exchange=exchange, routing_key="rk", headers=headers)
    wait_size(ours, queue.name, 1)
    expected = queue(channel).get(no_ack=True)
    assert received.headers == expected.headers
    assert isinstance(received.headers["big"], int)
    assert received.headers["big"] == 2**40
    assert received.headers["bytes"] == b"\x00\xff\x80"
    assert received.headers["dict"]["b"] == "raw"
    assert isinstance(received.headers["when"], datetime.datetime)
    assert received.headers["price"] == decimal.Decimal("1.25")
    assert isinstance(received.headers["price"], decimal.Decimal)


    producer.publish(b"body", exchange=exchange, routing_key="rk", headers=headers)
    delivery = take_rs(ours, queue.name)
    assert delivery[0] == b"body"
    assert delivery[1] == expected.headers
    assert isinstance(delivery[1]["big"], int)
    assert isinstance(delivery[1]["when"], datetime.datetime)
    assert isinstance(delivery[1]["price"], decimal.Decimal)
    ours.ack(delivery[4])
    ours.close()
    kombu_conn.close()


def test_exchange_types_and_routing():
    """Compare direct, topic, fanout and headers routing using separate queues for each publisher."""
    suffix = uuid.uuid4().hex
    direct_ex = Exchange(f"rs.direct.{suffix}", type="direct", durable=False)
    topic_ex = Exchange(f"rs.topic.{suffix}", type="topic", durable=False)
    fanout_k_ex = Exchange(f"rs.fanout.k.{suffix}", type="fanout", durable=False)
    fanout_r_ex = Exchange(f"rs.fanout.r.{suffix}", type="fanout", durable=False)
    headers_ex = Exchange(f"rs.headers.{suffix}", type="headers", durable=False)
    exchanges = [direct_ex, topic_ex, fanout_k_ex, fanout_r_ex, headers_ex]

    direct_k = Queue(f"rs.direct.k.{suffix}", exchange=direct_ex, routing_key="k", durable=False)
    direct_r = Queue(f"rs.direct.r.{suffix}", exchange=direct_ex, routing_key="r", durable=False)

    topic_patterns = [("all", "#"), ("star", "*.created"), ("account", "account.#")]
    topic_k = [
        Queue(f"rs.topic.k.{name}.{suffix}", exchange=topic_ex, routing_key=f"k.{pattern}", durable=False)
        for name, pattern in topic_patterns
    ]
    topic_r = [
        Queue(f"rs.topic.r.{name}.{suffix}", exchange=topic_ex, routing_key=f"r.{pattern}", durable=False)
        for name, pattern in topic_patterns
    ]

    fanout_k = [
        Queue(f"rs.fanout.k.{name}.{suffix}", exchange=fanout_k_ex, durable=False)
        for name in ("a", "b")
    ]
    fanout_r = [
        Queue(f"rs.fanout.r.{name}.{suffix}", exchange=fanout_r_ex, durable=False)
        for name in ("a", "b")
    ]

    headers_k = Queue(
        f"rs.headers.k.{suffix}",
        exchange=headers_ex,
        routing_key="",
        durable=False,
        binding_arguments={"x-match": "all", "format": "k"},
    )
    headers_r = Queue(
        f"rs.headers.r.{suffix}",
        exchange=headers_ex,
        routing_key="",
        durable=False,
        binding_arguments={"x-match": "all", "format": "r"},
    )

    all_queues = [
        direct_k,
        direct_r,
        *topic_k,
        *topic_r,
        *fanout_k,
        *fanout_r,
        headers_k,
        headers_r,
    ]
    kombu_conn = KombuConnection(AMQP_URL)
    channel = kombu_conn.default_channel
    for exchange in exchanges:
        exchange(channel).declare()
    kombu_declare(channel, None, *all_queues)
    producer = Producer(channel)

    ours = rs.Connection(AMQP_URL)
    for exchange in exchanges:
        ours.declare_exchange(exchange.name, exchange.type, durable=False)
    for queue in all_queues:
        ours.declare_queue(queue.name, durable=False)
        if queue is headers_k or queue is headers_r:
            ours.bind(
                queue.name,
                queue.exchange.name,
                "",
                arguments=queue.binding_arguments,
            )
        else:
            ours.bind(queue.name, queue.exchange.name, queue.routing_key or "")


    ours.publish(b"rust", exchange=direct_ex.name, routing_key="r")
    producer.publish(b"kombu", exchange=direct_ex, routing_key="k")
    wait_size(ours, direct_r.name, 1)
    wait_size(ours, direct_k.name, 1)
    assert direct_r(channel).get(no_ack=True).body in (b"rust",)
    assert [delivery[0] for delivery in collect_rs(ours, [direct_k.name], 1)] == [b"kombu"]
    ours.publish(b"anonymous", routing_key=direct_r.name)
    wait_size(ours, direct_r.name, 1)
    assert direct_r(channel).get(no_ack=True).body == b"anonymous"


    keys = ["account.1.created", "account.created", "account", "x.created"]
    expected_by_pattern = {
        "all": 4,

        "star": 2,
        # The account segment alone also matches.
        "account": 3,
    }
    for key in keys:
        producer.publish(key.encode(), exchange=topic_ex, routing_key=f"k.{key}")
        ours.publish(key.encode(), exchange=topic_ex.name, routing_key=f"r.{key}")
    for (name, _), queue_k, queue_r in zip(topic_patterns, topic_k, topic_r):
        expected = expected_by_pattern[name]
        wait_size(ours, queue_k.name, expected)
        wait_size(ours, queue_r.name, expected)


    producer.publish(b"broadcast", exchange=fanout_k_ex, routing_key="")
    ours.publish(b"broadcast", exchange=fanout_r_ex.name, routing_key="")
    for queue in fanout_k:
        wait_size(ours, queue.name, 1)
        assert queue(channel).get(no_ack=True).body == b"broadcast"
    for queue in fanout_r:
        wait_size(ours, queue.name, 1)
        assert [delivery[0] for delivery in collect_rs(ours, [queue.name], 1)] == [b"broadcast"]


    producer.publish(
        b"kombu-header",
        exchange=headers_ex,
        routing_key="",
        headers={"format": "k"},
    )
    ours.publish(
        b"rust-header",
        exchange=headers_ex.name,
        routing_key="",
        headers={"format": "r"},
    )
    ours.publish(
        b"ignored",
        exchange=headers_ex.name,
        routing_key="",
        headers={"format": "other"},
    )
    wait_size(ours, headers_k.name, 1)
    wait_size(ours, headers_r.name, 1)
    assert headers_k(channel).get(no_ack=True).body == b"kombu-header"
    assert [delivery[0] for delivery in collect_rs(ours, [headers_r.name], 1)] == [b"rust-header"]

    ours.close()
    kombu_conn.close()


def test_priority_queue_with_mixed_priorities():
    suffix = uuid.uuid4().hex
    exchange = Exchange(f"rs.prio.{suffix}", type="direct", durable=False)
    queue_k = Queue(
        f"rs.prio.k.{suffix}",
        exchange=exchange,
        routing_key="k",
        durable=False,
        queue_arguments={"x-max-priority": 10},
    )
    queue_r = Queue(
        f"rs.prio.r.{suffix}",
        exchange=exchange,
        routing_key="r",
        durable=False,
        queue_arguments={"x-max-priority": 10},
    )
    kombu_conn = KombuConnection(AMQP_URL)
    channel = kombu_declare(kombu_conn.default_channel, exchange, queue_k, queue_r)
    ours = rs.Connection(AMQP_URL)
    rs_declare(ours, exchange, queue_k, queue_r)
    producer = Producer(channel)

    priorities = [("low", 1), ("high", 9), ("mid", 5), ("zero", 0)]
    for body, priority in priorities:
        producer.publish(
            body.encode(), exchange=exchange, routing_key="k", priority=priority
        )
        ours.publish(
            body.encode(),
            exchange=exchange.name,
            routing_key="r",
            priority=priority,
        )
    wait_size(ours, queue_k.name, len(priorities))
    wait_size(ours, queue_r.name, len(priorities))

    expected = [
        (queue_k(channel).get(no_ack=True).body, index)
        for index in range(len(priorities))
    ]
    received = [
        delivery[0]
        for delivery in collect_rs(ours, [queue_r.name], len(priorities))
    ]
    assert [body for body, _ in expected] == [b"high", b"mid", b"low", b"zero"]
    assert received == [body for body, _ in expected]
    ours.close()
    kombu_conn.close()


def test_queue_expiry_ttl_and_quorum():
    suffix = uuid.uuid4().hex
    ours = rs.Connection(AMQP_URL)

    # Declarations reset idle expiry; wait without querying and check once.

    expiry = f"rs.expires.{suffix}"
    ours.declare_queue(expiry, durable=False, arguments={"x-expires": 400})
    time.sleep(1.2)
    with pytest.raises(LookupError):
        ours.declare_queue(expiry, passive=True)


    ttl = f"rs.ttl.{suffix}"
    ours.declare_queue(ttl, durable=False, arguments={"x-message-ttl": 1500})
    ours.publish(b"expiring", routing_key=ttl)
    wait_size(ours, ttl, 1, timeout=1.0)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and ours.size(ttl) != 0:
        time.sleep(0.05)
    assert ours.size(ttl) == 0

    # Quorum queues require durability.
    quorum = f"rs.quorum.{suffix}"
    ours.declare_queue(
        quorum, durable=True, arguments={"x-queue-type": "quorum"}
    )
    ours.publish(b"quorum", routing_key=quorum)
    delivery = take_rs(ours, quorum)
    assert delivery[0] == b"quorum"
    ours.ack(delivery[4])
    ours.delete_queue(quorum)
    ours.close()


def test_broadcast_to_multiple_consumers():
    suffix = uuid.uuid4().hex
    exchange = Exchange(f"rs.broadcast.{suffix}", type="fanout", durable=False)
    queues = [
        Queue(f"rs.broadcast.{name}.{suffix}", exchange=exchange, durable=False)
        for name in ("a", "b")
    ]
    producer_conn = KombuConnection(AMQP_URL)
    channel = kombu_declare(producer_conn.default_channel, exchange, *queues)
    ours_producer = rs.Connection(AMQP_URL)
    rs_declare(ours_producer, exchange, *queues)
    producer = Producer(channel)

    consumer_a = rs.Connection(AMQP_URL)
    consumer_b = rs.Connection(AMQP_URL)
    producer.publish(b"broadcast", exchange=exchange, routing_key="")
    ours_producer.publish(
        b"broadcast", exchange=exchange.name, routing_key=""
    )
    wait_size(ours_producer, queues[0].name, 2)
    wait_size(ours_producer, queues[1].name, 2)
    from_a = collect_rs(consumer_a, [queues[0].name], 2)
    from_b = collect_rs(consumer_b, [queues[1].name], 2)
    assert [delivery[0] for delivery in from_a] == [b"broadcast", b"broadcast"]
    assert [delivery[0] for delivery in from_b] == [b"broadcast", b"broadcast"]
    consumer_a.close()
    consumer_b.close()
    ours_producer.close()
    producer_conn.close()


def test_ack_reject_and_prefetch():
    suffix = uuid.uuid4().hex
    exchange = Exchange(f"rs.ack.{suffix}", type="direct", durable=False)
    prefetch_q = Queue(
        f"rs.ack.prefetch.{suffix}", exchange=exchange, routing_key="prefetch", durable=False
    )
    reject_q = Queue(
        f"rs.ack.reject.{suffix}", exchange=exchange, routing_key="reject", durable=False
    )
    kombu_conn = KombuConnection(AMQP_URL)
    channel = kombu_declare(kombu_conn.default_channel, exchange, prefetch_q, reject_q)
    ours = rs.Connection(AMQP_URL)
    rs_declare(ours, exchange, prefetch_q, reject_q)
    producer = Producer(channel)


    for index in range(5):
        producer.publish(
            str(index).encode(), exchange=exchange, routing_key="prefetch"
        )
    wait_size(ours, prefetch_q.name, 5)
    first = collect_rs(ours, [prefetch_q.name], 2, timeout=2, prefetch=2, ack=False)
    assert [delivery[0] for delivery in first] == [b"0", b"1"]
    assert ours.drain([prefetch_q.name], timeout=0.2, prefetch=2) == []
    ours.ack(first[0][4])
    third = collect_rs(ours, [prefetch_q.name], 1, timeout=2, prefetch=2, ack=False)
    assert [delivery[0] for delivery in third] == [b"2"]
    for delivery in first[1:] + third:
        ours.ack(delivery[4])

    # Use basic_get to exclude consumer buffering from rejection checks.
    producer.publish(b"discard", exchange=exchange, routing_key="reject")
    wait_size(ours, reject_q.name, 1)
    delivery = ours.get(reject_q.name)
    assert delivery[0] == b"discard"
    ours.reject(delivery[4], requeue=False)
    assert ours.size(reject_q.name) == 0


    producer.publish(b"again", exchange=exchange, routing_key="reject")
    wait_size(ours, reject_q.name, 1)
    delivery = ours.get(reject_q.name)
    assert delivery[0] == b"again"
    ours.reject(delivery[4], requeue=True)
    wait_size(ours, reject_q.name, 1)
    redelivered = ours.get(reject_q.name)
    assert redelivered[0] == b"again"
    assert redelivered[3]["redelivered"] is True
    ours.ack(redelivered[4])
    assert ours.size(reject_q.name) == 0

    ours.close()
    kombu_conn.close()


def wait_kombu_size(channel, queue_name, expected, *, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        size = Queue(queue_name).queue_declare(passive=True, channel=channel)[1]
        if size == expected:
            return
        time.sleep(0.02)
    raise AssertionError(f"kombu queue {queue_name}: expected {expected}")


def test_prefetch_changes_match_kombu():
    """Compare prefetch changes while consumers are active."""

    def measure(global_flag):
        suffix = uuid.uuid4().hex
        queue_k = f"rs.qos.k.{suffix}"
        queue_r = f"rs.qos.r.{suffix}"
        producer_conn = KombuConnection(AMQP_URL)
        channel = producer_conn.default_channel
        producer = Producer(channel)
        for queue in (queue_k, queue_r):
            Queue(queue).queue_declare(channel=channel)
            for index in range(8):
                producer.publish(str(index).encode(), routing_key=queue)
        wait_kombu_size(channel, queue_k, 8)
        wait_kombu_size(channel, queue_r, 8)

        # Use Celery's qos_global setting for this broker.
        channel.basic_qos(0, 2, global_flag)
        received = []

        def on_message(body, message):
            received.append(message)

        consumer = Consumer(
            channel,
            queues=[Queue(queue_k)],
            callbacks=[on_message],
            prefetch_count=None,
        )
        consumer.consume()

        def drain_kombu(seconds=0.3):
            end = time.monotonic() + seconds
            while time.monotonic() < end:
                try:
                    producer_conn.drain_events(timeout=0.05)
                except TimeoutError:
                    pass

        drain_kombu()
        initial_k = len(received)
        consumer.qos(prefetch_count=4, apply_global=global_flag)
        drain_kombu()
        increased_k = len(received) - initial_k
        consumer.qos(prefetch_count=1, apply_global=global_flag)
        drain_kombu()
        decreased_k = len(received) - initial_k - increased_k

        # Change prefetch on an active consumer over the same interval as kombu.

        def drain_rust(connection, queue, prefetch, seconds=0.3):
            collected = []
            end = time.monotonic() + seconds
            while time.monotonic() < end:
                remaining = max(0.0, end - time.monotonic())
                collected += connection.drain(
                    [queue], timeout=min(0.05, remaining), prefetch=prefetch
                )
            return collected

        ours = rs.Connection(AMQP_URL, transport_options={"qos_global": global_flag})
        initial = drain_rust(ours, queue_r, 2)
        increased = drain_rust(ours, queue_r, 4)
        decreased = drain_rust(ours, queue_r, 1)
        delivered = list(initial) + list(increased) + list(decreased)

        assert [initial_k, increased_k, decreased_k] == [
            len(initial),
            len(increased),
            len(decreased),
        ], (
            global_flag,
            initial_k,
            increased_k,
            decreased_k,
            len(initial),
            len(increased),
            len(decreased),
        )
        assert len(initial) == 2
        assert all(delivery[0].isdigit() for delivery in delivered)

        for message in received:
            message.ack()
        for delivery in delivered:
            ours.ack(delivery[4])
        consumer.cancel()
        ours.purge(queue_r)
        channel.queue_purge(queue_k)
        ours.close()
        producer_conn.close()

    measure(False)
    measure(True)


def test_publisher_confirms():
    suffix = uuid.uuid4().hex
    queue_name = f"rs.confirm.{suffix}"
    ours = rs.Connection(
        AMQP_URL, transport_options={"confirm_publish": True}
    )
    ours.declare_queue(queue_name, durable=False)
    for index in range(50):
        ours.publish(str(index).encode(), routing_key=queue_name)
    assert ours.size(queue_name) == 50

    # A missing exchange closes the channel; the next publication needs a fresh one.

    with pytest.raises(RuntimeError):
        ours.publish(b"x", exchange=f"rs.missing.{suffix}", routing_key="rk")
    ours.publish(b"after-error", routing_key=queue_name)
    assert ours.size(queue_name) == 51
    assert ours.purge(queue_name) == 51
    ours.close()


def _close_rust_connection(connection_name, timeout=20.0):
    """Close the named connection through the management API."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        connections = []
        for connection in api("GET", "/connections"):
            client = connection.get("client_properties") or {}
            if client.get("connection_name") == connection_name:
                connections.append(connection)
        for connection in connections:
            api(
                "DELETE",
                f"/connections/{urllib.parse.quote(connection['name'], safe='')}",
            )
        if connections:
            return len(connections)
        time.sleep(0.1)
    raise AssertionError(
        f"connection {connection_name!r} not visible in management API"
    )


def test_reconnect_after_broker_closes_connection():
    suffix = uuid.uuid4().hex
    queue_name = f"rs.reconnect.{suffix}"
    connection_name = f"celery-rs-test-{suffix}"
    ours = rs.Connection(
        AMQP_URL, transport_options={"connection_name": connection_name}
    )
    ours.declare_queue(queue_name, durable=False)
    for index in range(4):
        ours.publish(str(index).encode(), routing_key=queue_name)


    first = collect_rs(ours, [queue_name], 2, timeout=2, prefetch=2)
    assert [delivery[0] for delivery in first] == [b"0", b"1"]

    closed = {}

    def closer():
        time.sleep(0.3)
        closed["count"] = _close_rust_connection(connection_name)

    deadline = time.monotonic() + 30
    thread = threading.Thread(target=closer)
    thread.start()
    # Drain prefetched deliveries without acknowledgment until disconnection is detected.

    while True:
        try:
            ours.drain([queue_name], timeout=5)
        except ConnectionError:
            break
        if time.monotonic() > deadline:
            raise AssertionError("connection loss was not detected")
    thread.join()
    assert closed.get("count", 0) >= 1
    try:
        ours.close()
    except ConnectionError:
        pass

    # Reconnect to verify pending deliveries return without duplicating acknowledged ones.

    reconnected = rs.Connection(AMQP_URL)
    remaining = collect_rs(reconnected, [queue_name], 2, timeout=5, prefetch=2)
    assert [delivery[0] for delivery in remaining] == [b"2", b"3"]
    assert reconnected.size(queue_name) == 0
    reconnected.delete_queue(queue_name)
    reconnected.close()


def test_passive_declare_and_delete_exchange():
    suffix = uuid.uuid4().hex
    exchange = Exchange(f"rs.delete.{suffix}", type="direct", durable=False)
    empty = Queue(f"rs.delete.empty.{suffix}", exchange=exchange, routing_key="empty", durable=False)
    full = Queue(f"rs.delete.full.{suffix}", exchange=exchange, routing_key="full", durable=False)
    kombu_conn = KombuConnection(AMQP_URL)
    channel = kombu_declare(kombu_conn.default_channel, exchange, empty, full)
    producer = Producer(channel)
    ours = rs.Connection(AMQP_URL)
    rs_declare(ours, exchange, empty, full)

    with pytest.raises(ChannelError):
        Exchange(f"rs.missing.{suffix}", durable=False).declare(passive=True, channel=channel)
    with pytest.raises(LookupError):
        ours.declare_exchange(f"rs.missing.{suffix}", passive=True)
    with pytest.raises(ChannelError):
        Queue(f"rs.missing.q.{suffix}", durable=False).queue_declare(passive=True, channel=channel)
    with pytest.raises(LookupError):
        ours.declare_queue(f"rs.missing.q.{suffix}", passive=True)

    producer.publish(b"keep", exchange=exchange, routing_key="full")
    ours.publish(b"keep", exchange=exchange.name, routing_key="full")

    ours.delete_exchange(exchange.name)

    with pytest.raises(ChannelError):
        Exchange(exchange.name, durable=False).declare(passive=True, channel=channel)
    with pytest.raises(LookupError):
        ours.declare_exchange(exchange.name, passive=True)
    # AMQP exchange deletion preserves all queues, including empty ones.

    assert empty.queue_declare(passive=True, channel=channel)[1] == 0
    assert ours.declare_queue(empty.name, passive=True) == empty.name
    assert full.queue_declare(passive=True, channel=channel)[1] == 2
    assert ours.size(full.name) == 2


    other_exchange = Exchange(f"rs.delete.other.{suffix}", type="direct", durable=False)
    other_queue = Queue(
        f"rs.delete.other.q.{suffix}",
        exchange=other_exchange,
        routing_key="other",
        durable=False,
    )
    kombu_declare(channel, other_exchange, other_queue)
    rs_declare(ours, other_exchange, other_queue)
    channel.exchange_delete(exchange=other_exchange.name)
    with pytest.raises(LookupError):
        ours.declare_exchange(other_exchange.name, passive=True)
    # AMQP deletes bindings but preserves the queue.
    assert ours.declare_queue(other_queue.name, passive=True) == other_queue.name
    ours.delete_queue(other_queue.name)

    ours.close()
    kombu_conn.close()


def test_get_no_ack_and_requeue():
    suffix = uuid.uuid4().hex
    exchange = Exchange(f"rs.get.{suffix}", type="direct", durable=False)
    queue = Queue(f"rs.get.q.{suffix}", exchange=exchange, routing_key="rk", durable=False)
    kombu_conn = KombuConnection(AMQP_URL)
    channel = kombu_declare(kombu_conn.default_channel, exchange, queue)
    producer = Producer(channel)
    ours = rs.Connection(AMQP_URL)
    rs_declare(ours, exchange, queue)

    body = b'{"args": [1, 2]}'
    headers = {"task": "get", "retries": 1}
    kwargs = {
        "headers": headers,
        "content_type": "application/json",
        "content_encoding": "utf-8",
        "priority": 2,
    }
    for _ in range(2):
        publish_both(producer, ours, body, exchange, "rk", **kwargs)
    wait_size(ours, queue.name, 4)

    expected = kombu_message(queue(channel).get(no_ack=True))
    received = ours.get(queue.name, no_ack=True)
    assert rs_message(received) == expected
    ours.ack(received[4])

    expected = kombu_message(queue(channel).get(no_ack=False))
    received = ours.get(queue.name, no_ack=False)
    assert rs_message(received) == expected
    ours.reject(received[4], requeue=True)
    redelivered = ours.get(queue.name, no_ack=False)
    assert redelivered[0] == received[0]
    assert redelivered[3]["redelivered"] is True
    ours.ack(redelivered[4])
    assert ours.get(queue.name) is None
    assert queue(channel).get() is None
    ours.close()
    kombu_conn.close()


def test_wire_publish_records_match_kombu():
    """Verify identical publications produce identical normalized records."""
    from celery_compat import wire

    wire.enable_rabbit_trace()
    suffix = "fixed"
    exchange_name = f"rs.wire.{suffix}"
    queue_name = f"rs.wire.q.{suffix}"
    headers = {
        "task": "wire.add",
        "retries": 2,
        "flag": True,
        "ratio": 0.5,
        "none": None,
        "list": [1, "two"],
        "map": {"a": False},
    }
    properties = {
        "content_type": "application/json",
        "content_encoding": "utf-8",
        "priority": 4,
        "correlation_id": "corr",
        "reply_to": "reply",
        "message_id": "mid",
        "type": "task",
        "app_id": "app",
    }
    normalization = wire.Normalizer()
    with wire.capture("amqp") as capture:
        exchange = Exchange(exchange_name, type="direct", durable=False)
        queue = Queue(queue_name, exchange=exchange, routing_key="rk", durable=False)
        kombu_conn = KombuConnection(AMQP_URL)
        channel = kombu_declare(kombu_conn.default_channel, exchange, queue)
        producer = Producer(channel)
        producer.publish(
            b'{"args": [1, 2]}',
            exchange=exchange,
            routing_key="rk",
            headers=headers,
            **properties,
        )
        ours = rs.Connection(AMQP_URL)
        rs_declare(ours, exchange, queue)
        ours.publish(
            b'{"args": [1, 2]}',
            exchange=exchange_name,
            routing_key="rk",
            headers=headers,
            properties={
                key: value
                for key, value in properties.items()
                if key not in {"content_type", "content_encoding"}
            },
            content_type=properties["content_type"],
            content_encoding=properties["content_encoding"],
            priority=properties["priority"],
        )
        ours.close()
        kombu_conn.close()

    records = capture.messages(normalization)
    assert len(records) == 2, records
    assert records[0] == records[1], (
        "record kombu:\n"
        f"{records[0]}\nrecord Rust:\n{records[1]}"
    )
