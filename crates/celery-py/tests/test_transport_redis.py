"""Interoperabilità del trasporto Redis: kombu 5.7 vs ``_celery_rs.Connection``.

I due client parlano con lo stesso server (``CELERY_RS_TEST_REDIS``, default
``redis://127.0.0.1:6390``). Ogni test usa nomi univoci e azzera il database:
lo stato sul broker (liste, hash ``unacked``, zset, insiemi di binding) è
confrontato con quello prodotto da kombu a parità di operazioni.
"""

from __future__ import annotations

import datetime
import decimal
import os
import re
import sys
import time
import uuid
from pathlib import Path

import pytest
import redis as redis_lib
from kombu import Connection as KombuConnection
from kombu import Consumer, Exchange, Producer, Queue

import _celery_rs as rs

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools" / "compat"))
from celery_compat.wire import Normalizer, RedisCapture  # noqa: E402

REDIS_URL = os.environ.get("CELERY_RS_TEST_REDIS", "redis://127.0.0.1:6390")
UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
TS_RE = re.compile(r"(?<![\d.])1[5-9]\d{8}\.\d+")


def unique(prefix: str) -> str:
    return f"{prefix}.{uuid.uuid4().hex}"


def with_db(url: str, db: int) -> str:
    base = url.rstrip("/")
    if re.search(r"/\d+$", base):
        base = base.rsplit("/", 1)[0]
    return f"{base}/{db}"


def redis_for(url: str) -> redis_lib.Redis:
    return redis_lib.Redis.from_url(url)


def flush(url: str = REDIS_URL) -> None:
    redis_for(url).flushdb()


def kombu_message(message, *, decode=False):
    """Tupla normalizzata di un messaggio kombu (delivery_tag escluso)."""
    properties = dict(message.properties)
    properties.pop("delivery_tag", None)
    body = message.body if not decode else message.decode()
    return (
        body,
        dict(message.headers),
        properties,
        dict(message.delivery_info),
    )


def rs_message(delivery):
    body, headers, properties, delivery_info, _delivery_tag = delivery
    properties = dict(properties)
    properties.pop("delivery_tag", None)
    return (body, dict(headers), properties, dict(delivery_info))


def declare_kombu(channel, exchange, queues):
    exchange(channel).declare()
    for queue in queues:
        queue(channel).declare()
    return channel


def declare_rs(connection, exchange, queues):
    connection.declare_exchange(exchange.name, exchange.type)
    for queue in queues:
        connection.declare_queue(queue.name)
        connection.bind(queue.name, exchange.name, queue.routing_key)


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


def collect_rs_until_empty(connection, queue_name, *, timeout=1.0):
    """Drena una coda fino a timeout (il cambio code attende il BRPOP in corso)."""
    bodies = []
    deadline = time.monotonic() + timeout
    empty = 0
    while time.monotonic() < deadline and empty < 2:
        batch = connection.drain([queue_name], timeout=0.2)
        if not batch:
            empty += 1
            continue
        empty = 0
        for delivery in batch:
            bodies.append(delivery[0])
            connection.ack(delivery[4])
    return bodies


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
            connection.drain_events(timeout=0.05)
        except TimeoutError:
            pass
    return received


@pytest.fixture(autouse=True)
def clean_redis():
    flush()
    yield
    flush()


# ---------------------------------------------------------------- messaggi


def test_publish_and_consume_match_kombu():
    """Rust -> kombu e kombu -> Rust: stesso body/header/properties/delivery_info."""
    exchange_name = unique("ex")
    queue_name = unique("q")
    exchange = Exchange(exchange_name, type="direct", durable=False, auto_delete=False)
    queue = Queue(queue_name, exchange=exchange, routing_key="rk")

    kombu_conn = KombuConnection(REDIS_URL)
    producer = Producer(declare_kombu(kombu_conn.default_channel, exchange, [queue]))

    ours = rs.Connection(REDIS_URL)
    declare_rs(ours, exchange, [queue])

    headers = {"task": "add", "retries": 2, "flag": True}
    properties = {"correlation_id": "abc", "reply_to": "reply", "message_id": "m1"}

    # kombu pubblica, Rust consuma.
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
    delivery = ours.get(queue_name)
    assert delivery is not None
    assert delivery[0] == b'{"args": [1, 2]}'
    assert delivery[1] == headers
    assert delivery[2]["correlation_id"] == "abc"
    assert delivery[2]["delivery_mode"] == 2
    assert delivery[2]["priority"] == 4
    assert delivery[2]["body_encoding"] == "base64"
    assert delivery[3] == {"exchange": exchange_name, "routing_key": "rk"}
    ours.ack(delivery[4])

    # Rust pubblica, kombu consuma.
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
    expected = kombu_message(queue(kombu_conn.default_channel).get(no_ack=False))
    assert expected[0] == b'{"args": [1, 2]}'
    assert expected[1] == headers
    assert expected[2]["priority"] == 4
    assert expected[3] == {"exchange": exchange_name, "routing_key": "rk"}


def test_publish_envelope_is_identical_to_kombu():
    """Lo stesso publish produce la stessa lista Redis (a meno dell'uuid)."""
    exchange_name = unique("same")
    queue_name = unique("q")
    exchange = Exchange(exchange_name, type="direct")
    queue = Queue(queue_name, exchange=exchange, routing_key="rk")

    kombu_conn = KombuConnection(REDIS_URL)
    channel = declare_kombu(kombu_conn.default_channel, exchange, [queue])
    producer = Producer(channel)
    ours = rs.Connection(REDIS_URL)
    declare_rs(ours, exchange, [queue])

    def publish_kombu():
        producer.publish(
            b'{"x": 1}',
            exchange=exchange,
            routing_key="rk",
            headers={"task": "add", "n": -1},
            correlation_id="cid",
            content_type="application/json",
            content_encoding="utf-8",
            priority=7,
            delivery_mode=1,
        )

    def publish_rs():
        ours.publish(
            b'{"x": 1}',
            exchange=exchange_name,
            routing_key="rk",
            headers={"task": "add", "n": -1},
            properties={"correlation_id": "cid"},
            content_type="application/json",
            content_encoding="utf-8",
            priority=7,
            delivery_mode=1,
        )

    publish_kombu()
    publish_rs()

    raw = redis_for(REDIS_URL).lrange(f"{queue_name}\x06\x166", 0, -1)
    assert len(raw) == 2
    first, second = (UUID_RE.sub("<id>", item.decode()) for item in raw)
    assert first == second

    # Stesso `get` dalla coda: kombu e Rust decodificano lo stesso envelope.
    expected = kombu_message(queue(channel).get(no_ack=False))
    delivery = ours.get(queue_name)
    assert rs_message(delivery) == expected


def test_header_types_roundtrip():
    exchange_name = unique("headers")
    queue_name = unique("q")
    exchange = Exchange(exchange_name, type="direct")
    queue = Queue(queue_name, exchange=exchange, routing_key="rk")
    kombu_conn = KombuConnection(REDIS_URL)
    channel = declare_kombu(kombu_conn.default_channel, exchange, [queue])
    producer = Producer(channel)
    ours = rs.Connection(REDIS_URL)
    declare_rs(ours, exchange, [queue])

    headers = {
        "none": None,
        "true": True,
        "false": False,
        "int": -42,
        "big": 2**70,
        "float": 1.5,
        "text": "città €",
        "bytes": b"\x00\xff\x80",
        "utf8": b"abc",
        "list": [1, "two", None, {"three": 3}],
        "dict": {"a": [True, False], "b": b"raw"},
    }
    # Rust -> kombu.
    ours.publish(b"body", exchange=exchange_name, routing_key="rk", headers=headers)
    expected = kombu_message(queue(channel).get(no_ack=False))
    assert expected[1]["big"] == 2**70
    assert expected[1]["bytes"] == b"\x00\xff\x80"
    assert expected[1]["utf8"] == b"abc"

    # kombu -> Rust.
    producer.publish(b"body", exchange=exchange, routing_key="rk", headers=headers)
    delivery = ours.get(queue_name)
    assert rs_message(delivery) == expected
    assert delivery[1]["bytes"] == b"\x00\xff\x80"
    assert isinstance(delivery[1]["big"], int)


def test_datetime_and_decimal_headers():
    exchange_name = unique("temporal")
    queue_name = unique("q")
    exchange = Exchange(exchange_name, type="direct")
    queue = Queue(queue_name, exchange=exchange, routing_key="rk")
    kombu_conn = KombuConnection(REDIS_URL)
    channel = declare_kombu(kombu_conn.default_channel, exchange, [queue])
    producer = Producer(channel)
    ours = rs.Connection(REDIS_URL)
    declare_rs(ours, exchange, [queue])

    headers = {
        "when": datetime.datetime(2020, 1, 2, 3, 4, 5),
        "price": decimal.Decimal("1.25"),
        "negative": decimal.Decimal("-0.05"),
    }
    producer.publish(b"body", exchange=exchange, routing_key="rk", headers=headers)
    ours.publish(b"body", exchange=exchange_name, routing_key="rk", headers=headers)

    expected = kombu_message(queue(channel).get(no_ack=False))
    received = rs_message(ours.get(queue_name))
    assert received == expected
    assert received[1]["when"] == datetime.datetime(2020, 1, 2, 3, 4, 5)
    assert received[1]["price"] == decimal.Decimal("1.25")
    assert isinstance(received[1]["price"], decimal.Decimal)


# ---------------------------------------------------------------- routing


def test_topic_routing_matches_kombu():
    """Stesse binding su due exchange: le stesse code ricevono gli stessi messaggi."""
    bindings = [
        ("all", "#"),
        ("created", "*.created"),
        ("account", "account.#"),
        ("exact", "account.1.created"),
    ]
    published = ["account.1.created", "account.created", "account", "x.created"]

    def run(prefix, connection_factory, declare, publish, read):
        exchange_name = unique(f"topic-{prefix}")
        exchange = Exchange(exchange_name, type="topic")
        queues = [
            Queue(unique(f"q-{prefix}-{suffix}"), exchange=exchange, routing_key=pattern)
            for suffix, pattern in bindings
        ]
        state = declare(exchange, queues)
        for routing_key in published:
            publish(state, exchange_name, routing_key)
        return {
            suffix: [body for body in read(state, queue)]
            for (suffix, _), queue in zip(bindings, queues)
        }

    def declare_kombu_side(exchange, queues):
        conn = KombuConnection(REDIS_URL)
        channel = declare_kombu(conn.default_channel, exchange, queues)
        return conn, channel

    def declare_rs_side(exchange, queues):
        conn = rs.Connection(REDIS_URL)
        declare_rs(conn, exchange, queues)
        return conn

    def publish_kombu(state, exchange_name, routing_key):
        conn, channel = state
        Producer(channel).publish(
            routing_key.encode(), exchange=exchange_name, routing_key=routing_key
        )

    def publish_rs(state, exchange_name, routing_key):
        state.publish(
            routing_key.encode(), exchange=exchange_name, routing_key=routing_key
        )

    def read_kombu(state, queue):
        conn, channel = state
        bodies = []
        while True:
            message = queue(channel).get(no_ack=True)
            if message is None:
                return bodies
            bodies.append(message.body)

    def read_rs(state, queue):
        return collect_rs_until_empty(state, queue.name, timeout=1.5)

    expected = run("k", None, declare_kombu_side, publish_kombu, read_kombu)
    received = run("r", None, declare_rs_side, publish_rs, read_rs)
    assert received == expected
    assert any(expected.values())
    assert received["exact"] == [b"account.1.created"]
    assert received["created"] == [b"account.created", b"x.created"]


def test_direct_multiple_queues():
    """Routing direct su più code, con lo stesso esito di kombu."""
    keys = ("a", "b")

    def scenario(prefix, kombu_side):
        exchange_name = unique(f"multi-{prefix}")
        exchange = Exchange(exchange_name, type="direct")
        queues = [
            Queue(unique(f"q-{prefix}-{key}"), exchange=exchange, routing_key=key)
            for key in keys
        ]
        if kombu_side:
            conn = KombuConnection(REDIS_URL)
            channel = declare_kombu(conn.default_channel, exchange, queues)
            producer = Producer(channel)
            for index in range(3):
                for key in keys:
                    producer.publish(b"x", exchange=exchange, routing_key=key)
            return {
                key: [
                    queue(channel).get(no_ack=True).body
                    for _ in range(3)
                ]
                for key, queue in zip(keys, queues)
            }
        conn = rs.Connection(REDIS_URL)
        declare_rs(conn, exchange, queues)
        for index in range(3):
            for key in keys:
                conn.publish(b"x", exchange=exchange_name, routing_key=key)
        return {
            key: collect_rs_until_empty(conn, queue.name, timeout=1.5)
            for key, queue in zip(keys, queues)
        }

    assert scenario("k", True) == scenario("r", False) == {"a": [b"x"] * 3, "b": [b"x"] * 3}


def test_fanout_broadcast_to_multiple_consumers():
    """Fanout: broadcast a più consumer e interoperabilità con kombu."""
    # A) Un publish Rust raggiunge due consumer Rust su code diverse.
    exchange_name = unique("fan")
    exchange = Exchange(exchange_name, type="fanout")
    queues = [
        Queue(unique("fa"), exchange=exchange, routing_key=""),
        Queue(unique("fb"), exchange=exchange, routing_key=""),
    ]
    ours = rs.Connection(REDIS_URL)
    declare_rs(ours, exchange, queues)
    # Ogni consumatore dichiara il proprio binding fanout: come in kombu il
    # tipo di exchange è stato locale alla connessione.
    first = rs.Connection(REDIS_URL)
    second = rs.Connection(REDIS_URL)
    for connection, queue in ((first, queues[0]), (second, queues[1])):
        connection.declare_exchange(exchange_name, "fanout")
        connection.declare_queue(queue.name)
        connection.bind(queue.name, exchange_name, queue.routing_key)
        assert connection.drain([queue.name], timeout=0.1) == []

    ours.publish(b"broadcast", exchange=exchange_name, routing_key="")
    first_batch = collect_rs(first, [queues[0].name], 1)
    second_batch = collect_rs(second, [queues[1].name], 1)
    assert [item[0] for item in first_batch] == [b"broadcast"]
    assert [item[0] for item in second_batch] == [b"broadcast"]
    assert first_batch[0][3] == {"exchange": exchange_name, "routing_key": ""}
    assert second_batch[0][3] == {"exchange": exchange_name, "routing_key": ""}

    # B) Un publish kombu raggiunge il consumer Rust.
    exchange_b = Exchange(unique("fank"), type="fanout")
    queue_b = Queue(unique("kq"), exchange=exchange_b, routing_key="")
    kombu_conn = KombuConnection(REDIS_URL)
    channel_b = declare_kombu(kombu_conn.default_channel, exchange_b, [queue_b])
    consumer_rs = rs.Connection(REDIS_URL)
    consumer_rs.declare_exchange(exchange_b.name, "fanout")
    consumer_rs.declare_queue(queue_b.name)
    consumer_rs.bind(queue_b.name, exchange_b.name, queue_b.routing_key or "")
    assert consumer_rs.drain([queue_b.name], timeout=0.1) == []
    Producer(channel_b).publish(b"from-kombu", exchange=exchange_b, routing_key="")
    batch = collect_rs(consumer_rs, [queue_b.name], 1)
    assert [item[0] for item in batch] == [b"from-kombu"]

    # C) Un publish Rust raggiunge il consumer kombu.
    exchange_c = Exchange(unique("fanr"), type="fanout")
    queue_c = Queue(unique("rq"), exchange=exchange_c, routing_key="")
    kombu_conn_c = KombuConnection(REDIS_URL)
    channel_c = declare_kombu(kombu_conn_c.default_channel, exchange_c, [queue_c])
    received = []
    consumer_c = Consumer(
        channel_c, queues=[queue_c], callbacks=[lambda b, m: received.append(m)]
    )
    consumer_c.consume()
    # Registra la PSUBSCRIBE prima del publish (pub/sub non ha backlog).
    try:
        kombu_conn_c.drain_events(timeout=0.3)
    except TimeoutError:
        pass
    producer_rs = rs.Connection(REDIS_URL)
    declare_rs(producer_rs, exchange_c, [queue_c])
    producer_rs.publish(b"from-rust", exchange=exchange_c.name, routing_key="")
    deadline = time.monotonic() + 3.0
    while not received and time.monotonic() < deadline:
        try:
            kombu_conn_c.drain_events(timeout=0.2)
        except TimeoutError:
            pass
    assert [message.body for message in received] == [b"from-rust"]
    assert dict(received[0].delivery_info)["exchange"] == exchange_c.name


def test_priority_matches_kombu():
    """Priorità: stesse liste per step e stesso ordine di consumo di kombu."""
    priorities = [0, 9, 4, 1]

    def scenario(prefix, kombu_side):
        queue_name = unique(f"pq-{prefix}")
        queue = Queue(queue_name)
        if kombu_side:
            conn = KombuConnection(REDIS_URL)
            channel = conn.default_channel
            queue(channel).declare()
            for index, priority in enumerate(priorities):
                Producer(channel).publish(
                    f"m{index}".encode(), routing_key=queue_name, priority=priority
                )
            client = redis_for(REDIS_URL)
            raw_keys = sorted(k.decode() for k in client.keys(f"{queue_name}*"))
            suffixes = sorted(key.split(queue_name, 1)[1] for key in raw_keys)
            consumed = [queue(channel).get(no_ack=True) for _ in priorities]
            return suffixes, [(m.body, m.properties["priority"]) for m in consumed]

        conn = rs.Connection(REDIS_URL)
        conn.declare_queue(queue_name)
        for index, priority in enumerate(priorities):
            conn.publish(f"m{index}".encode(), routing_key=queue_name, priority=priority)
        client = redis_for(REDIS_URL)
        raw_keys = sorted(k.decode() for k in client.keys(f"{queue_name}*"))
        suffixes = sorted(
            key.split(queue_name, 1)[1] for key in raw_keys
        )
        deliveries = collect_rs(conn, [queue_name], len(priorities))
        return suffixes, [(item[0], item[2]["priority"]) for item in deliveries]

    expected_keys, expected_order = scenario("k", True)
    received_keys, received_order = scenario("r", False)
    assert expected_keys == received_keys
    assert len(expected_keys) == 3
    assert received_order == expected_order
    assert [priority for _, priority in received_order] == [0, 1, 4, 9]


# ---------------------------------------------------------------- ack/qos


def test_ack_reject_and_requeue_match_kombu():
    """ack/reject/requeue: stesso esito (body, header, ordine, redelivered)."""
    bodies = [b"one", b"two", b"three"]

    def scenario(prefix, kombu_side):
        queue_name = unique(f"ack-{prefix}")
        queue = Queue(queue_name)
        if kombu_side:
            conn = KombuConnection(REDIS_URL)
            channel = conn.default_channel
            queue(channel).declare()
            for body in bodies:
                Producer(channel).publish(body, routing_key=queue_name)
            first = queue(channel).get(no_ack=False)
            first.reject()
            second = queue(channel).get(no_ack=False)
            second.reject(requeue=True)
            out = []
            for _ in range(2):
                message = queue(channel).get(no_ack=False)
                out.append(
                    (
                        message.body,
                        dict(message.headers),
                        dict(message.delivery_info),
                    )
                )
                message.ack()
            return out

        conn = rs.Connection(REDIS_URL)
        conn.declare_queue(queue_name)
        for body in bodies:
            conn.publish(body, routing_key=queue_name)
        first = conn.get(queue_name, no_ack=False)
        conn.reject(first[4], requeue=False)
        second = conn.get(queue_name, no_ack=False)
        conn.reject(second[4], requeue=True)
        out = []
        for _ in range(2):
            delivery = conn.get(queue_name, no_ack=False)
            out.append((delivery[0], dict(delivery[1]), dict(delivery[3])))
            conn.ack(delivery[4])
        return out

    def normalize(items, queue_prefix):
        out = []
        for body, headers, delivery_info in items:
            info = dict(delivery_info)
            if info.get("routing_key", "").startswith(queue_prefix):
                info["routing_key"] = "<queue>"
            out.append((body, headers, info))
        return out

    expected = normalize(scenario("k", True), "ack-k")
    received = normalize(scenario("r", False), "ack-r")
    assert received == expected
    assert [body for body, _, _ in received] == [b"three", b"two"]
    assert received[1][1].get("redelivered") is True
    assert received[1][2].get("redelivered") is True

    # Requeue di un tag sconosciuto: il contratto del trasporto richiede
    # `UnknownDeliveryTag` (il QoS Redis di kombu è invece un no-op: HGET non
    # trova nulla e `ack` marca il tag, senza sollevare).
    conn = rs.Connection(REDIS_URL)
    with pytest.raises(KeyError):
        conn.reject("missing-tag", requeue=True)


def test_prefetch_matches_kombu():
    """Prefetch 2: due messaggi in volo, il terzo dopo un ack (kombu e Rust)."""
    def publish(prefix, producer_publish, queue_name):
        for index in range(5):
            producer_publish(index, queue_name)

    # kombu
    queue_k = Queue(unique("prefetch-k"))
    kombu_conn = KombuConnection(REDIS_URL)
    channel = kombu_conn.default_channel
    queue_k(channel).declare()
    producer_k = Producer(channel)
    publish("k", lambda i, q: producer_k.publish(str(i).encode(), routing_key=q), queue_k.name)
    received_k = []

    def on_message(body, message):
        received_k.append(message)

    consumer_k = Consumer(
        channel, queues=[queue_k], callbacks=[on_message], prefetch_count=2
    )
    consumer_k.consume()
    deadline = time.monotonic() + 1.0
    while len(received_k) < 2 and time.monotonic() < deadline:
        try:
            kombu_conn.drain_events(timeout=0.05)
        except TimeoutError:
            pass
    with pytest.raises(TimeoutError):
        kombu_conn.drain_events(timeout=0.05)

    # Rust
    queue_r = unique("prefetch-r")
    ours = rs.Connection(REDIS_URL)
    ours.declare_queue(queue_r)
    for index in range(5):
        ours.publish(str(index).encode(), routing_key=queue_r)
    first_batch = collect_rs(ours, [queue_r], 5, timeout=0.5, prefetch=2, ack=False)
    assert [item[0] for item in first_batch] == [b"0", b"1"]
    assert ours.drain([queue_r], timeout=0.3, prefetch=2) == []

    channel.basic_ack(received_k[0].delivery_tag)
    ours.ack(first_batch[0][4])
    kombu_conn.drain_events(timeout=0.3)
    third = collect_rs(ours, [queue_r], 1, timeout=0.5, prefetch=2, ack=False)
    assert [item[0] for item in third] == [b"2"]
    assert [message.body for message in received_k] == [b"0", b"1", b"2"]


# ---------------------------------------------------------------- stato Redis


def _normalized_state(client, norm):
    state = {}
    for key in sorted(client.keys("*")):
        name = norm(key.decode("latin1"))
        kind = client.type(key).decode()
        if kind == "list":
            value = ("list", [norm(item.decode("latin1")) for item in client.lrange(key, 0, -1)])
        elif kind == "set":
            value = ("set", sorted(norm(m.decode("latin1")) for m in client.smembers(key)))
        elif kind == "hash":
            value = (
                "hash",
                {
                    norm(k.decode("latin1")): norm(v.decode("latin1"))
                    for k, v in client.hgetall(key).items()
                },
            )
        elif kind == "zset":
            value = (
                "zset",
                sorted(
                    (norm(m.decode("latin1")), norm(str(score)))
                    for m, score in client.zrange(key, 0, -1, withscores=True)
                ),
            )
        else:  # pragma: no cover - tipi non usati dal trasporto
            value = (kind, "<ignorato>")
        state[name] = value
    return state


def _publish_scenario(publish, queue_name, exchange_name):
    for index, priority in enumerate([0, 4, 9]):
        publish(index, priority)


def test_state_after_publish_matches_kombu():
    """Dopo un publish lo stato delle chiavi è identico (a meno degli uuid)."""
    exchange_name = unique("stateex")
    queue_name = unique("stateq")

    # kombu sul db 0, Rust sul db 1: stesso scenario, stato confrontabile.
    flush(with_db(REDIS_URL, 0))
    flush(with_db(REDIS_URL, 1))

    kombu_conn = KombuConnection(with_db(REDIS_URL, 0))
    exchange = Exchange(exchange_name, type="direct")
    queue = Queue(queue_name, exchange=exchange, routing_key="rk")
    channel = declare_kombu(kombu_conn.default_channel, exchange, [queue])
    producer = Producer(channel)

    ours = rs.Connection(with_db(REDIS_URL, 1))
    declare_rs(ours, exchange, [queue])

    def publish_kombu(index, priority):
        producer.publish(
            f"body-{index}".encode(),
            exchange=exchange,
            routing_key="rk",
            priority=priority,
            headers={"n": index},
            content_type="application/json",
            content_encoding="utf-8",
        )

    def publish_rs(index, priority):
        ours.publish(
            f"body-{index}".encode(),
            exchange=exchange_name,
            routing_key="rk",
            priority=priority,
            headers={"n": index},
            content_type="application/json",
            content_encoding="utf-8",
        )

    _publish_scenario(publish_kombu, queue_name, exchange_name)
    _publish_scenario(publish_rs, queue_name, exchange_name)

    # Un Normalizer per lato: gli uuid sono diversi, i segnaposto devono
    # essere assegnati nello stesso ordine (chiavi ordinate).
    expected = _normalized_state(redis_for(with_db(REDIS_URL, 0)), Normalizer())
    received = _normalized_state(redis_for(with_db(REDIS_URL, 1)), Normalizer())
    assert received == expected


def test_state_after_unacked_consume_matches_kombu():
    exchange_name = unique("unackedex")
    queue_name = unique("unackedq")
    flush(with_db(REDIS_URL, 0))
    flush(with_db(REDIS_URL, 1))

    kombu_conn = KombuConnection(with_db(REDIS_URL, 0))
    exchange = Exchange(exchange_name, type="direct")
    queue = Queue(queue_name, exchange=exchange, routing_key="rk")
    channel = declare_kombu(kombu_conn.default_channel, exchange, [queue])
    Producer(channel).publish(
        b"payload",
        exchange=exchange,
        routing_key="rk",
        priority=3,
        headers={"task": "x"},
        content_type="application/json",
        content_encoding="utf-8",
    )
    message = queue(channel).get(no_ack=False)

    ours = rs.Connection(with_db(REDIS_URL, 1))
    declare_rs(ours, exchange, [queue])
    ours.publish(
        b"payload",
        exchange=exchange_name,
        routing_key="rk",
        priority=3,
        headers={"task": "x"},
        content_type="application/json",
        content_encoding="utf-8",
    )
    delivery = ours.get(queue_name)

    expected = _normalized_state(redis_for(with_db(REDIS_URL, 0)), Normalizer())
    received = _normalized_state(redis_for(with_db(REDIS_URL, 1)), Normalizer())
    assert "unacked" in expected
    assert received == expected

    # Il valore di `unacked` è già confrontato sopra, a meno di uuid e score.
    assert redis_for(with_db(REDIS_URL, 1)).hget("unacked", delivery[4]) is not None

    ours.ack(delivery[4])
    channel.basic_ack(message.delivery_tag)
    assert redis_for(with_db(REDIS_URL, 1)).hgetall("unacked") == {}
    assert redis_for(with_db(REDIS_URL, 0)).hgetall("unacked") == {}


def test_visibility_timeout_restores_between_kombu_and_rust():
    """Un messaggio non confermato torna visibile anche all'altro client."""
    options = {"visibility_timeout": 1}

    # Rust consuma senza ack; un consumer kombu lo ripristina.
    queue_one = unique("visible")
    kombu_conn_one = KombuConnection(REDIS_URL, transport_options=options)
    channel_one = kombu_conn_one.default_channel
    queue_obj = Queue(queue_one)
    queue_obj(channel_one).declare()
    ours = rs.Connection(REDIS_URL, transport_options=options)
    ours.declare_queue(queue_one)
    ours.publish(b"from-rust", routing_key=queue_one)
    delivery = ours.get(queue_one)
    assert delivery is not None and delivery[0] == b"from-rust"

    restored = []
    consumer_one = Consumer(channel_one, queues=[queue_obj], callbacks=[lambda b, m: restored.append(m)])
    consumer_one.consume()
    deadline = time.monotonic() + 8
    while not restored and time.monotonic() < deadline:
        try:
            kombu_conn_one.drain_events(timeout=0.2)
        except TimeoutError:
            pass
    assert restored, "kombu non ha ripristinato il messaggio non confermato"
    assert restored[0].body == b"from-rust"
    assert dict(restored[0].headers).get("redelivered") is True

    # Kombu consuma senza ack; un consumer Rust lo ripristina.
    queue_two = unique("visible2")
    kombu_conn = KombuConnection(REDIS_URL, transport_options=options)
    channel_two = kombu_conn.default_channel
    queue_two_obj = Queue(queue_two)
    queue_two_obj(channel_two).declare()
    Producer(channel_two).publish(b"from-kombu", routing_key=queue_two)
    message = queue_two_obj(channel_two).get(no_ack=False)
    assert message.body == b"from-kombu"

    ours_two = rs.Connection(REDIS_URL, transport_options=options)
    ours_two.declare_queue(queue_two)
    found = None
    deadline = time.monotonic() + 8
    while found is None and time.monotonic() < deadline:
        batch = ours_two.drain([queue_two], timeout=1.0)
        if batch:
            found = batch[0]
        else:
            # Il drain crea il consumer: il suo loop ripristina i visibili.
            time.sleep(0.2)
    assert found is not None, "Rust non ha ripristinato il messaggio non confermato"
    assert found[0] == b"from-kombu"
    assert found[1].get("redelivered") is True
    ours_two.ack(found[4])


# ---------------------------------------------------------------- wire


def _wire_declare(scenario):
    """Comandi di declare+bind / publish confrontati con MONITOR."""
    norm = Normalizer()
    with RedisCapture() as capture:
        scenario()
    return capture.messages(norm)


def _kombu_declare_scenario(kind, exchange_name, queue_name, routing_key, publish=True,
                            priority=0):
    conn = KombuConnection(REDIS_URL)
    channel = conn.default_channel
    exchange = Exchange(exchange_name, type=kind)
    queue = Queue(queue_name, exchange=exchange, routing_key=routing_key)
    exchange(channel).declare()
    queue(channel).declare()
    if publish:
        Producer(channel).publish(
            b'{"a": 1}',
            exchange=exchange,
            routing_key=routing_key or exchange_name,
            priority=priority,
            headers={"task": "add"},
            content_type="application/json",
            content_encoding="utf-8",
        )
    conn.close()


def _rs_declare_scenario(kind, exchange_name, queue_name, routing_key, publish=True,
                         priority=0):
    conn = rs.Connection(REDIS_URL)
    conn.declare_exchange(exchange_name, kind)
    conn.declare_queue(queue_name)
    conn.bind(queue_name, exchange_name, routing_key)
    if publish:
        conn.publish(
            b'{"a": 1}',
            exchange=exchange_name,
            routing_key=routing_key or exchange_name,
            priority=priority,
            headers={"task": "add"},
            content_type="application/json",
            content_encoding="utf-8",
        )
    conn.close()


def _compare_wire(kind, routing_key, *, publish=True, priority=0, fanout=False):
    exchange_name = unique(f"wire-{kind}")
    queue_name = unique("wireq")
    if fanout:
        routing_key = ""

    expected = _wire_declare(
        lambda: _kombu_declare_scenario(
            kind, exchange_name, queue_name, routing_key, publish, priority
        )
    )
    received = _wire_declare(
        lambda: _rs_declare_scenario(
            kind, exchange_name, queue_name, routing_key, publish, priority
        )
    )
    # Stessi nomi di exchange/coda, quindi le due sequenze devono coincidere
    # comando per comando (a meno di uuid e timestamp, normalizzati).
    assert received, "nessun comando catturato"
    assert received == expected


def test_wire_declare_and_bind_direct():
    _compare_wire("direct", "rk", publish=False)


def test_wire_publish_direct():
    _compare_wire("direct", "rk")


def test_wire_publish_with_priority():
    _compare_wire("direct", "rk", priority=7)


def test_wire_topic_declare_and_bind():
    _compare_wire("topic", "task.#", publish=False)


def test_wire_fanout_publish():
    _compare_wire("fanout", "", fanout=True)


# ---------------------------------------------------------------- errori


def test_url_and_transport_options():
    flush()
    connection = rs.Connection(REDIS_URL, transport_options={"visibility_timeout": 10})
    queue_name = unique("opt")
    connection.declare_queue(queue_name)
    connection.publish(b"x", routing_key=queue_name)
    assert connection.size(queue_name) == 1
    connection.close()

    with pytest.raises(ValueError):
        rs.Connection("redis://127.0.0.1:6390/not-a-db")
    with pytest.raises(ValueError):
        rs.Connection(REDIS_URL, transport_options={"priority_steps": "0,3,6,9"})
