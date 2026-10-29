"""Compare memory transport messages with kombu, preserving delivery order and metadata."""

from __future__ import annotations

import datetime
import decimal
import time
import uuid

import pytest
from kombu import Connection as KombuConnection
from kombu import Consumer, Exchange, Producer, Queue
from kombu.exceptions import ChannelError

import _celery_rs as rs


def unique(prefix: str) -> str:
    return f"{prefix}.{uuid.uuid4().hex}"


def kombu_message(message):
    """Normalize a kombu message, excluding its generated delivery tag."""
    properties = dict(message.properties)
    properties.pop("delivery_tag", None)
    return (
        message.body,
        dict(message.headers),
        properties,
        dict(message.delivery_info),
    )


def rs_message(delivery) -> tuple:
    body, headers, properties, delivery_info, _delivery_tag = delivery
    properties = dict(properties)
    properties.pop("delivery_tag", None)
    return (body, dict(headers), properties, dict(delivery_info))


def declare_kombu(connection, exchange, queue):
    channel = connection.default_channel
    exchange(channel).declare()
    queue(channel).declare()
    return channel


def declare_rs(connection, exchange, queue):
    connection.declare_exchange(exchange.name, exchange.type)
    connection.declare_queue(queue.name)
    connection.bind(queue.name, exchange.name, queue.routing_key)


def collect_rs(connection, queues, count, *, timeout=1.0, prefetch=0, ack=True):
    """Collect up to count messages, allowing each drain to return a batch."""
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


def collect_kombu(connection, queues, count, *, timeout=1.0, prefetch=0, ack=True):
    """Collect up to count messages from a kombu consumer."""
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


def take_rs(connection, queue_name, *, timeout=1.0):
    """Fetch one message with prefetch set to one."""
    batch = connection.drain([queue_name], timeout=timeout, prefetch=1)
    assert len(batch) == 1, batch
    return batch[0]


def test_publish_consume_ack():
    exchange_name = unique("direct")
    queue_name = unique("q")
    exchange = Exchange(exchange_name, type="direct", durable=False, auto_delete=False)
    queue = Queue(queue_name, exchange=exchange, routing_key="rk")

    kombu_conn = KombuConnection("memory://")
    channel = declare_kombu(kombu_conn, exchange, queue)
    producer = Producer(channel)

    ours = rs.Connection("memory://")
    declare_rs(ours, exchange, queue)

    headers = {"task": "add", "retries": 2, "flag": True}
    properties = {"correlation_id": "abc", "reply_to": "reply", "message_id": "m1"}
    producer.publish(
        b'{"args": [1, 2]}',
        exchange=exchange,
        routing_key="rk",
        headers=headers,
        content_type="application/json",
        content_encoding="utf-8",
        priority=4,
        **properties,
    )
    ours.publish(
        b'{"args": [1, 2]}',
        exchange=exchange_name,
        routing_key="rk",
        headers=headers,
        properties=properties,
        content_type="application/json",
        content_encoding="utf-8",
        priority=4,
    )

    expected = kombu_message(queue(channel).get(no_ack=False))
    delivery = collect_rs(ours, [queue_name], 1)[0]
    assert rs_message(delivery) == expected
    assert delivery[1] == headers
    assert delivery[2]["priority"] == 4
    assert delivery[2]["delivery_mode"] == 2
    assert delivery[2]["body_encoding"] == "base64"
    assert isinstance(delivery[4], str) and len(delivery[4]) == 36
    assert ours.size(queue_name) == 0


def test_header_types_roundtrip():
    exchange_name = unique("headers")
    queue_name = unique("q")
    exchange = Exchange(exchange_name, type="direct")
    queue = Queue(queue_name, exchange=exchange, routing_key="rk")

    kombu_conn = KombuConnection("memory://")
    channel = declare_kombu(kombu_conn, exchange, queue)
    producer = Producer(channel)

    ours = rs.Connection("memory://")
    declare_rs(ours, exchange, queue)

    headers = {
        "none": None,
        "true": True,
        "false": False,
        "int": -42,
        "big": 2**70,
        "float": 1.5,
        "text": "città €",
        "bytes": b"\x00\xff\x80",
        "list": [1, "two", None, {"three": 3}],
        "dict": {"a": [True, False], "b": b"raw"},
    }
    producer.publish(b"body", exchange=exchange, routing_key="rk", headers=headers)
    ours.publish(b"body", exchange=exchange_name, routing_key="rk", headers=headers)

    expected = kombu_message(queue(channel).get(no_ack=False))
    delivery = collect_rs(ours, [queue_name], 1)[0]
    assert rs_message(delivery) == expected
    assert delivery[1]["big"] == 2**70
    assert isinstance(delivery[1]["big"], int)
    assert delivery[1]["bytes"] == b"\x00\xff\x80"


def test_datetime_and_decimal_headers():
    exchange_name = unique("temporal")
    queue_name = unique("q")
    exchange = Exchange(exchange_name, type="direct")
    queue = Queue(queue_name, exchange=exchange, routing_key="rk")

    kombu_conn = KombuConnection("memory://")
    channel = declare_kombu(kombu_conn, exchange, queue)
    producer = Producer(channel)

    ours = rs.Connection("memory://")
    declare_rs(ours, exchange, queue)

    # Match py-amqp's naive UTC timestamps and Decimal tuple normalization.

    headers = {
        "when": datetime.datetime(2020, 1, 2, 3, 4, 5),
        "price": decimal.Decimal("1.25"),
        "negative": decimal.Decimal("-0.05"),
    }
    producer.publish(b"body", exchange=exchange, routing_key="rk", headers=headers)
    ours.publish(b"body", exchange=exchange_name, routing_key="rk", headers=headers)

    expected = kombu_message(queue(channel).get(no_ack=False))
    received = rs_message(collect_rs(ours, [queue_name], 1)[0])
    assert received == expected
    assert received[1]["when"] == datetime.datetime(2020, 1, 2, 3, 4, 5)
    assert isinstance(received[1]["when"], datetime.datetime)
    assert received[1]["price"] == decimal.Decimal("1.25")
    assert isinstance(received[1]["price"], decimal.Decimal)


def test_unsupported_or_unrepresentable_headers_are_rejected():
    ours = rs.Connection("memory://")
    queue_name = unique("bad-headers")
    ours.declare_queue(queue_name)

    with pytest.raises(TypeError):
        ours.publish(b"x", routing_key=queue_name, headers={"object": object()})
    with pytest.raises(ValueError):
        ours.publish(
            b"x", routing_key=queue_name, headers={"big": decimal.Decimal("1E+100")}
        )

    assert ours.size(queue_name) == 0


def test_topic_routing_matches_kombu():
    exchange_name = unique("topic")
    exchange = Exchange(exchange_name, type="topic")
    bindings = [
        ("all", "#"),
        ("created", "*.created"),
        ("account", "account.#"),
        ("exact", "account.1.created"),
    ]
    queues = [
        Queue(unique(f"q-{suffix}"), exchange=exchange, routing_key=pattern)
        for suffix, pattern in bindings
    ]

    kombu_conn = KombuConnection("memory://")
    channel = declare_kombu(kombu_conn, exchange, queues[0])
    for queue in queues[1:]:
        queue(channel).declare()
    producer = Producer(channel)

    ours = rs.Connection("memory://")
    ours.declare_exchange(exchange_name, "topic")
    for queue in queues:
        ours.declare_queue(queue.name)
        ours.bind(queue.name, exchange_name, queue.routing_key)

    published = ["account.1.created", "account.created", "account", "x.created"]
    for routing_key in published:
        producer.publish(routing_key.encode(), exchange=exchange, routing_key=routing_key)
        ours.publish(routing_key.encode(), exchange=exchange_name, routing_key=routing_key)

    for queue in queues:
        expected = []
        while True:
            message = queue(channel).get(no_ack=True)
            if message is None:
                break
            expected.append(kombu_message(message))
        received = [
            rs_message(delivery)
            for delivery in collect_rs(ours, [queue.name], len(expected))
        ]
        assert received == expected, f"queue {queue.routing_key!r}"
        assert received, f"queue {queue.routing_key!r} received nothing"


def test_fanout_and_anonymous_exchange():
    exchange_name = unique("fanout")
    exchange = Exchange(exchange_name, type="fanout")
    queues = [Queue(unique("fa"), exchange=exchange), Queue(unique("fb"), exchange=exchange)]

    kombu_conn = KombuConnection("memory://")
    channel = declare_kombu(kombu_conn, exchange, queues[0])
    queues[1](channel).declare()
    producer = Producer(channel)

    ours = rs.Connection("memory://")
    ours.declare_exchange(exchange_name, "fanout")
    for queue in queues:
        ours.declare_queue(queue.name)
        ours.bind(queue.name, exchange_name, "")

    producer.publish(b"broadcast", exchange=exchange, routing_key="")
    ours.publish(b"broadcast", exchange=exchange_name, routing_key="")

    for queue in queues:
        expected = kombu_message(queue(channel).get(no_ack=True))
        received = rs_message(collect_rs(ours, [queue.name], 1)[0])
        assert received == expected
        assert received[3] == {"exchange": exchange_name, "routing_key": ""}

    # The default exchange routes directly to the queue name.
    producer.publish(b"direct-to-queue", routing_key=queues[0].name)
    ours.publish(b"direct-to-queue", routing_key=queues[0].name)
    expected = kombu_message(queues[0](channel).get(no_ack=True))
    received = rs_message(collect_rs(ours, [queues[0].name], 1)[0])
    assert received == expected
    assert received[3] == {"exchange": "", "routing_key": queues[0].name}


def test_ack_reject_and_requeue_order():
    exchange_name = unique("ack")
    queue_name = unique("q")
    exchange = Exchange(exchange_name, type="direct")
    queue = Queue(queue_name, exchange=exchange, routing_key="rk")

    kombu_conn = KombuConnection("memory://")
    channel = declare_kombu(kombu_conn, exchange, queue)
    producer = Producer(channel)

    ours = rs.Connection("memory://")
    declare_rs(ours, exchange, queue)

    for body in (b"one", b"two", b"three"):
        producer.publish(body, exchange=exchange, routing_key="rk")
        ours.publish(body, exchange=exchange_name, routing_key="rk")


    first = queue(channel).get(no_ack=False)
    first.reject()
    delivery = take_rs(ours, queue_name)
    ours.reject(delivery[4], requeue=False)

    # Memory requeue appends at the tail.
    second = queue(channel).get(no_ack=False)
    second.reject(requeue=True)
    delivery = take_rs(ours, queue_name)
    ours.reject(delivery[4], requeue=True)

    expected = [kombu_message(queue(channel).get(no_ack=False)) for _ in range(2)]
    received = []
    for _ in range(2):
        delivery = take_rs(ours, queue_name)
        received.append(rs_message(delivery))
        ours.ack(delivery[4])
    assert received == expected
    assert [item[0] for item in received] == [b"three", b"two"]


    assert ours.size(queue_name) == 0


def test_prefetch_matches_kombu():
    exchange_name = unique("prefetch")
    queue_name = unique("q")
    exchange = Exchange(exchange_name, type="direct")
    queue = Queue(queue_name, exchange=exchange, routing_key="rk")

    kombu_conn = KombuConnection("memory://")
    channel = declare_kombu(kombu_conn, exchange, queue)
    producer = Producer(channel)

    ours = rs.Connection("memory://")
    declare_rs(ours, exchange, queue)

    for index in range(5):
        producer.publish(str(index).encode(), exchange=exchange, routing_key="rk")
        ours.publish(str(index).encode(), exchange=exchange_name, routing_key="rk")

    received = []

    def on_message(body, message):
        received.append(message)

    consumer = Consumer(
        channel, queues=[queue], callbacks=[on_message], prefetch_count=2
    )
    consumer.consume()
    deadline = time.monotonic() + 0.5
    while len(received) < 2 and time.monotonic() < deadline:
        kombu_conn.drain_events(timeout=0.05)
    with pytest.raises(TimeoutError):
        kombu_conn.drain_events(timeout=0.05)

    first_batch = collect_rs(ours, [queue_name], 5, timeout=0.2, prefetch=2, ack=False)
    assert [item[0] for item in first_batch] == [b"0", b"1"]

    assert ours.drain([queue_name], timeout=0.2, prefetch=2) == []

    channel.basic_ack(received[0].delivery_tag)
    ours.ack(first_batch[0][4])
    kombu_conn.drain_events(timeout=0.2)
    third = collect_rs(ours, [queue_name], 1, timeout=0.2, prefetch=2, ack=False)
    assert [item[0] for item in third] == [b"2"]
    assert [message.body for message in received] == [b"0", b"1", b"2"]


def test_multiple_queues_round_robin():
    queue_names = [unique("qa"), unique("qb")]
    queues = [Queue(name) for name in queue_names]

    kombu_conn = KombuConnection("memory://")
    channel = kombu_conn.default_channel
    for queue in queues:
        queue(channel).declare()
    producer = Producer(channel)

    ours = rs.Connection("memory://")
    for index in range(3):
        for queue_name, prefix in zip(queue_names, (b"a", b"b")):
            body = prefix + str(index).encode()
            producer.publish(body, routing_key=queue_name)
            ours.publish(body, routing_key=queue_name)

    expected = [
        message.body
        for message in collect_kombu(kombu_conn, queues, 6, timeout=0.5)
    ]
    received = [
        delivery[0]
        for delivery in collect_rs(ours, queue_names, 6, timeout=0.5)
    ]
    assert received == expected


def test_priority_is_preserved_and_fifo():
    queue_name = unique("q")
    queue = Queue(queue_name)

    kombu_conn = KombuConnection("memory://")
    channel = kombu_conn.default_channel
    queue(channel).declare()
    producer = Producer(channel)

    ours = rs.Connection("memory://")
    ours.declare_queue(queue_name)

    priorities = [0, 9, 4, 1]
    for index, priority in enumerate(priorities):
        body = f"m{index}".encode()
        producer.publish(body, routing_key=queue_name, priority=priority)
        ours.publish(body, routing_key=queue_name, priority=priority)

    expected = [kombu_message(queue(channel).get(no_ack=True)) for _ in priorities]
    received = [rs_message(delivery) for delivery in collect_rs(ours, [queue_name], 4)]
    assert received == expected
    # Memory preserves FIFO order regardless of priority.
    assert [item[0] for item in received] == [b"m0", b"m1", b"m2", b"m3"]
    assert [item[2]["priority"] for item in received] == priorities


def test_purge_size_delete_queue():
    exchange_name = unique("purge")
    queue_name = unique("q")
    exchange = Exchange(exchange_name, type="direct")
    queue = Queue(queue_name, exchange=exchange, routing_key="rk")

    kombu_conn = KombuConnection("memory://")
    channel = declare_kombu(kombu_conn, exchange, queue)
    producer = Producer(channel)

    ours = rs.Connection("memory://")
    declare_rs(ours, exchange, queue)

    for index in range(3):
        producer.publish(str(index).encode(), exchange=exchange, routing_key="rk")
        ours.publish(str(index).encode(), exchange=exchange_name, routing_key="rk")

    assert ours.size(queue_name) == queue(channel).queue_declare()[1] == 3
    assert ours.purge(queue_name) == queue(channel).purge() == 3
    assert ours.size(queue_name) == 0

    producer.publish(b"again", exchange=exchange, routing_key="rk")
    ours.publish(b"again", exchange=exchange_name, routing_key="rk")
    ours.delete_queue(queue_name)
    assert ours.size(queue_name) == 0

    ours.publish(b"lost", exchange=exchange_name, routing_key="rk")
    assert ours.size(queue_name) == 0


def test_connections_share_the_in_process_broker():
    queue_name = unique("shared")
    producer_conn = rs.Connection("memory://")
    consumer_conn = rs.Connection("memory://")
    assert producer_conn.declare_queue(queue_name) == queue_name
    producer_conn.publish(b"hello", routing_key=queue_name)

    deliveries = collect_rs(consumer_conn, [queue_name], 1)
    assert [item[0] for item in deliveries] == [b"hello"]
    assert consumer_conn.size(queue_name) == 0


def test_large_binary_body_roundtrip():
    queue_name = unique("big")
    body = bytes(range(256)) * 4096

    ours = rs.Connection("memory://")
    ours.declare_queue(queue_name)
    ours.publish(bytearray(body), routing_key=queue_name)
    delivery = collect_rs(ours, [queue_name], 1)[0]
    assert delivery[0] == body


def test_declare_queue_generates_name():
    ours = rs.Connection("memory://")
    name = ours.declare_queue()
    assert name.startswith("amq.gen-")
    ours.publish(b"x", routing_key=name)
    assert collect_rs(ours, [name], 1)[0][0] == b"x"


def test_drain_timeout_returns_empty_list():
    ours = rs.Connection("memory://")
    queue_name = unique("empty")
    ours.declare_queue(queue_name)
    assert ours.drain([queue_name], timeout=0.05) == []


def test_drain_zero_timeout_is_non_blocking():
    ours = rs.Connection("memory://")
    queue_name = unique("t0")
    ours.declare_queue(queue_name)
    started = time.monotonic()
    assert ours.drain([queue_name], timeout=0) == []
    assert time.monotonic() - started < 0.5
    ours.publish(b"now", routing_key=queue_name)
    assert [delivery[0] for delivery in ours.drain([queue_name], timeout=0)] == [b"now"]


def test_url_and_transport_options_errors():

    with pytest.raises(ValueError):
        rs.Connection("not-a-url")
    with pytest.raises(ValueError):
        rs.Connection("un supported://host/")

    connection = rs.Connection(
        "memory:///options",
        transport_options={
            "visibility_timeout": 10,
            "priority_steps": [0, 3, 6, 9],
            "fanout_prefix": "myprefix",
            "queue_order_strategy": "sorted",
            "custom_option": {"a": 1},
        },
    )
    connection.close()

    with pytest.raises(ValueError):
        rs.Connection(
            "memory:///bad-options", transport_options={"priority_steps": "0,3,6,9"}
        )


def test_closed_connection_rejects_operations():
    ours = rs.Connection("memory://")
    queue_name = unique("closed")
    ours.declare_queue(queue_name)
    ours.close()
    with pytest.raises(ConnectionError):
        ours.size(queue_name)


def test_get_matches_kombu():
    exchange_name = unique("get")
    queue_name = unique("q")
    exchange = Exchange(exchange_name, type="direct")
    queue = Queue(queue_name, exchange=exchange, routing_key="rk")

    kombu_conn = KombuConnection("memory://")
    channel = declare_kombu(kombu_conn, exchange, queue)
    producer = Producer(channel)

    ours = rs.Connection("memory://")
    declare_rs(ours, exchange, queue)

    for body in (b"first", b"second"):
        producer.publish(body, exchange=exchange, routing_key="rk")
        ours.publish(body, exchange=exchange_name, routing_key="rk")


    expected = kombu_message(queue(channel).get(no_ack=True))
    received = ours.get(queue_name, no_ack=True)
    assert received is not None
    assert rs_message(received) == expected
    ours.ack(received[4])


    expected = kombu_message(queue(channel).get(no_ack=False))
    received = ours.get(queue_name, no_ack=False)
    assert rs_message(received) == expected
    ours.ack(received[4])

    assert ours.get(queue_name) is None
    assert queue(channel).get() is None


def test_get_requeue_matches_kombu():
    queue_name = unique("get-requeue")
    queue = Queue(queue_name)

    kombu_conn = KombuConnection("memory://")
    channel = kombu_conn.default_channel
    queue(channel).declare()
    producer = Producer(channel)

    ours = rs.Connection("memory://")
    ours.declare_queue(queue_name)

    producer.publish(b"payload", routing_key=queue_name)
    ours.publish(b"payload", routing_key=queue_name)

    message = queue(channel).get(no_ack=False)
    message.reject(requeue=True)
    received = ours.get(queue_name)
    ours.reject(received[4], requeue=True)

    expected = kombu_message(queue(channel).get(no_ack=False))
    received = ours.get(queue_name)
    assert rs_message(received) == expected


def test_unbind_matches_kombu():
    exchange_name = unique("unbind")
    exchange = Exchange(exchange_name, type="topic")
    queues = [
        Queue(unique("q-keep"), exchange=exchange, routing_key="task.#"),
        Queue(unique("q-drop"), exchange=exchange, routing_key="task.#"),
    ]

    kombu_conn = KombuConnection("memory://")
    channel = declare_kombu(kombu_conn, exchange, queues[0])
    queues[1](channel).declare()
    producer = Producer(channel)

    ours = rs.Connection("memory://")
    ours.declare_exchange(exchange_name, "topic")
    for queue in queues:
        ours.declare_queue(queue.name)
        ours.bind(queue.name, exchange_name, queue.routing_key)

    queues[1].unbind_from(exchange, routing_key="task.#", channel=channel)
    ours.unbind(queues[1].name, exchange_name, "task.#")

    producer.publish(b"routed", exchange=exchange, routing_key="task.one")
    ours.publish(b"routed", exchange=exchange_name, routing_key="task.one")

    assert ours.size(queues[0].name) == queues[0](channel).queue_declare()[1] == 1
    assert ours.size(queues[1].name) == queues[1](channel).queue_declare()[1] == 0


def test_delete_exchange_matches_kombu():
    exchange_name = unique("delete")
    exchange = Exchange(exchange_name, type="direct")
    empty = Queue(unique("q-empty"), exchange=exchange, routing_key="empty")
    full = Queue(unique("q-full"), exchange=exchange, routing_key="full")

    kombu_conn = KombuConnection("memory://")
    channel = declare_kombu(kombu_conn, exchange, empty)
    full(channel).declare()
    producer = Producer(channel)

    ours = rs.Connection("memory://")
    ours.declare_exchange(exchange_name, "direct")
    for queue in (empty, full):
        ours.declare_queue(queue.name)
        ours.bind(queue.name, exchange_name, queue.routing_key)

    producer.publish(b"keep", exchange=exchange, routing_key="full")
    ours.publish(b"keep", exchange=exchange_name, routing_key="full")

    channel.exchange_delete(exchange=exchange_name)
    ours.delete_exchange(exchange_name)


    with pytest.raises(ChannelError):
        Exchange(exchange_name).declare(passive=True, channel=channel)
    with pytest.raises(LookupError):
        ours.declare_exchange(exchange_name, passive=True)


    with pytest.raises(ChannelError):
        empty.queue_declare(passive=True, channel=channel)
    with pytest.raises(LookupError):
        ours.declare_queue(empty.name, passive=True)


    info = full.queue_declare(passive=True, channel=channel)
    assert info[1] == 1
    assert ours.declare_queue(full.name, passive=True) == full.name
    assert ours.size(full.name) == 1


    producer.publish(b"lost", exchange=exchange, routing_key="full")
    ours.publish(b"lost", exchange=exchange_name, routing_key="full")
    assert ours.size(full.name) == 1


def test_passive_declare_matches_kombu():
    exchange_name = unique("passive")
    queue_name = unique("q-passive")

    kombu_conn = KombuConnection("memory://")
    channel = kombu_conn.default_channel
    ours = rs.Connection("memory://")

    with pytest.raises(ChannelError):
        Exchange(exchange_name).declare(passive=True, channel=channel)
    with pytest.raises(LookupError):
        ours.declare_exchange(exchange_name, passive=True)
    with pytest.raises(ChannelError):
        Queue(queue_name).queue_declare(passive=True, channel=channel)
    with pytest.raises(LookupError):
        ours.declare_queue(queue_name, passive=True)

    Exchange(exchange_name).declare(channel=channel)
    Queue(queue_name).declare(channel=channel)
    ours.declare_exchange(exchange_name)
    ours.declare_queue(queue_name)
    ours.publish(b"one", routing_key=queue_name)

    Exchange(exchange_name).declare(passive=True, channel=channel)
    ours.declare_exchange(exchange_name, passive=True)
    assert Queue(queue_name).queue_declare(passive=True, channel=channel)[1] == 0
    assert ours.declare_queue(queue_name, passive=True) == queue_name
    assert ours.size(queue_name) == 1


def test_prefetch_can_change_between_drains():
    exchange_name = unique("prefetch-change")
    queue_name = unique("q")
    exchange = Exchange(exchange_name, type="direct")
    queue = Queue(queue_name, exchange=exchange, routing_key="rk")

    kombu_conn = KombuConnection("memory://")
    channel = declare_kombu(kombu_conn, exchange, queue)
    producer = Producer(channel)

    ours = rs.Connection("memory://")
    declare_rs(ours, exchange, queue)

    for index in range(6):
        producer.publish(str(index).encode(), exchange=exchange, routing_key="rk")
        ours.publish(str(index).encode(), exchange=exchange_name, routing_key="rk")

    received = []

    def on_message(body, message):
        received.append(message)

    consumer = Consumer(
        channel, queues=[queue], callbacks=[on_message], prefetch_count=2
    )
    consumer.consume()
    deadline = time.monotonic() + 0.5
    while len(received) < 2 and time.monotonic() < deadline:
        kombu_conn.drain_events(timeout=0.05)
    with pytest.raises(TimeoutError):
        kombu_conn.drain_events(timeout=0.05)

    ours_first = ours.drain([queue_name], timeout=0.2, prefetch=2)
    assert [item[0] for item in ours_first] == [b"0", b"1"]
    assert ours.drain([queue_name], timeout=0.2, prefetch=2) == []

    # Update prefetch on the existing consumer to preserve pending deliveries.
    consumer.qos(prefetch_count=4)
    deadline = time.monotonic() + 0.5
    while len(received) < 4 and time.monotonic() < deadline:
        kombu_conn.drain_events(timeout=0.05)
    with pytest.raises(TimeoutError):
        kombu_conn.drain_events(timeout=0.05)

    ours_second = ours.drain([queue_name], timeout=0.2, prefetch=4)
    assert [item[0] for item in ours_second] == [b"2", b"3"]
    assert ours.drain([queue_name], timeout=0.2, prefetch=4) == []


    ours.set_prefetch(2)
    assert ours.drain([queue_name], timeout=0.2, prefetch=2) == []

    for message in received:
        message.ack()
    for delivery in ours_first + ours_second:
        ours.ack(delivery[4])

    consumer.qos(prefetch_count=2)
    deadline = time.monotonic() + 0.5
    while len(received) < 6 and time.monotonic() < deadline:
        kombu_conn.drain_events(timeout=0.05)
    ours_third = ours.drain([queue_name], timeout=0.2, prefetch=2)
    assert [item[0] for item in ours_third] == [b"4", b"5"]
    assert [message.body for message in received] == [b"0", b"1", b"2", b"3", b"4", b"5"]


def test_drain_reconciles_queue_set_without_losing_consumer():
    queue_one = unique("reconcile-a")
    queue_two = unique("reconcile-b")

    ours = rs.Connection("memory://")
    for queue_name in (queue_one, queue_two):
        ours.declare_queue(queue_name)
        for index in range(2):
            ours.publish(f"{queue_name}-{index}".encode(), routing_key=queue_name)

    first = ours.drain([queue_one], timeout=0.2)
    assert [item[0] for item in first] == [
        f"{queue_one}-0".encode(),
        f"{queue_one}-1".encode(),
    ]

    assert ours.size(queue_two) == 2

    second = ours.drain([queue_one, queue_two], timeout=0.2)
    assert [item[0] for item in second] == [
        f"{queue_two}-0".encode(),
        f"{queue_two}-1".encode(),
    ]
    for delivery in first + second:
        ours.ack(delivery[4])


    assert ours.drain([queue_one], timeout=0.05) == []
    ours.publish(b"later", routing_key=queue_two)
    assert ours.drain([queue_one], timeout=0.05) == []
    assert ours.size(queue_two) == 1
