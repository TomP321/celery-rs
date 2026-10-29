"""Compare Rust and kombu Redis publish and consume rates.

Use --messages under tools/dev/with-brokers. CELERY_RS_TEST_REDIS defaults to
redis://127.0.0.1:6390. A short JSON body (51 bytes) keeps conditions equal;
local server performance affects results, so these are not pass thresholds."""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid

import redis as redis_lib
from kombu import Connection as KombuConnection
from kombu import Consumer, Producer, Queue

import _celery_rs as rs

URL = os.environ.get('CELERY_RS_REDIS_URL', os.environ.get("CELERY_RS_TEST_REDIS", 'redis://127.0.0.1:6390'))
BODY = json.dumps({"task": "bench.add", "args": [1, 2], "kwargs": {}}).encode()


def rate(count: int, elapsed: float) -> float:
    return count / elapsed if elapsed else 0.0


def bench_kombu_publish(count: int, queue_name: str) -> float:
    conn = KombuConnection(URL)
    channel = conn.default_channel
    queue = Queue(queue_name)
    queue(channel).declare()
    producer = Producer(channel)
    started = time.perf_counter()
    for _ in range(count):
        producer.publish(
            BODY,
            routing_key=queue_name,
            content_type="application/json",
            content_encoding="utf-8",
        )
    return rate(count, time.perf_counter() - started)


def bench_rs_publish(count: int, queue_name: str) -> float:
    conn = rs.Connection(URL)
    conn.declare_queue(queue_name)
    started = time.perf_counter()
    for _ in range(count):
        conn.publish(
            BODY,
            routing_key=queue_name,
            content_type="application/json",
            content_encoding="utf-8",
        )
    conn.close()
    return rate(count, time.perf_counter() - started)


def bench_kombu_consume(count: int, queue_name: str) -> float:
    conn = KombuConnection(URL)
    channel = conn.default_channel
    queue = Queue(queue_name)
    queue(channel).declare()
    received = 0

    def on_message(body, message):
        nonlocal received
        received += 1
        message.ack()

    consumer = Consumer(
        channel, queues=[queue], callbacks=[on_message], prefetch_count=0
    )
    consumer.consume()
    started = time.perf_counter()
    while received < count:
        try:
            conn.drain_events(timeout=1.0)
        except TimeoutError:
            break
    return rate(received, time.perf_counter() - started)


def bench_rs_consume(count: int, queue_name: str) -> float:
    conn = rs.Connection(URL)
    conn.declare_queue(queue_name)
    received = 0
    started = time.perf_counter()
    while received < count:
        batch = conn.drain([queue_name], timeout=1.0, prefetch=0)
        if not batch:
            break
        for delivery in batch:
            received += 1
            conn.ack(delivery[4])
    return rate(received, time.perf_counter() - started)


def fill(queue_name: str, count: int) -> None:
    client = redis_lib.Redis.from_url(URL)
    client.delete(queue_name)
    conn = rs.Connection(URL)
    conn.declare_queue(queue_name)
    for _ in range(count):
        conn.publish(BODY, routing_key=queue_name)
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--messages", type=int, default=5000)
    args = parser.parse_args()
    count = args.messages
    redis_lib.Redis.from_url(URL).flushdb()

    queue_name = f"bench.{uuid.uuid4().hex}"
    kombu_publish = bench_kombu_publish(count, queue_name)
    redis_lib.Redis.from_url(URL).flushdb()
    rs_publish = bench_rs_publish(count, queue_name)

    consume_queue = f"bench.consume.{uuid.uuid4().hex}"
    fill(consume_queue, count)
    kombu_consume = bench_kombu_consume(count, consume_queue)
    fill(consume_queue, count)
    rs_consume = bench_rs_consume(count, consume_queue)

    print(f"Redis {URL}, {count} messages of {len(BODY)} bytes")
    print(f"publish/s  kombu {kombu_publish:10.0f}   rust {rs_publish:10.0f}"
          f"   ({rs_publish / kombu_publish:.2f}x)")
    print(f"consume/s  kombu {kombu_consume:10.0f}   rust {rs_consume:10.0f}"
          f"   ({rs_consume / kombu_consume:.2f}x)")


if __name__ == "__main__":
    main()
