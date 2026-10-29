"""Compare Rust and kombu AMQP publish and acknowledged consume rates.

Use --n and --url under tools/dev/with-brokers. Dedicated queues are cleared
before each measurement; publisher confirms are disabled."""

from __future__ import annotations

import argparse
import os
import time
import uuid
from pathlib import Path
import sys

from kombu import Connection as KombuConnection
from kombu import Consumer, Producer, Queue

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "tools" / "compat") not in sys.path:
    sys.path.insert(0, str(ROOT / "tools" / "compat"))

import _celery_rs as rs


def timed(fn):
    started = time.perf_counter()
    fn()
    return time.perf_counter() - started


def wait_size(connection, queue, expected, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if connection.size(queue) == expected:
            return
        time.sleep(0.005)
    raise AssertionError(f"{queue}: expected {expected}, found {connection.size(queue)}")


def bench_publish_rs(url, queue, count):
    connection = rs.Connection(url)
    connection.declare_queue(queue)
    body = b'{"args": [1, 2]}'

    def run():
        for _ in range(count):
            connection.publish(body, routing_key=queue)

    elapsed = timed(run)
    wait_size(connection, queue, count)
    connection.purge(queue)
    connection.close()
    return count / elapsed


def bench_consume_rs(url, queue, count):
    connection = rs.Connection(url)
    connection.declare_queue(queue)
    body = b'{"args": [1, 2]}'
    for _ in range(count):
        connection.publish(body, routing_key=queue)

    collected = 0

    def run():
        nonlocal collected
        while collected < count:
            batch = connection.drain([queue], timeout=2.0, prefetch=500)
            if not batch:
                break
            for delivery in batch:
                connection.ack(delivery[4])
            collected += len(batch)

    elapsed = timed(run)
    assert collected == count, (collected, count)
    connection.close()
    return count / elapsed


def bench_publish_kombu(url, queue, count):
    connection = KombuConnection(url)
    channel = connection.default_channel
    declared = Queue(queue)
    declared(channel).declare()
    producer = Producer(channel)
    body = b'{"args": [1, 2]}'

    def run():
        for _ in range(count):
            producer.publish(body, routing_key=queue)

    elapsed = timed(run)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if declared.queue_declare(passive=True, channel=channel)[1] == count:
            break
        time.sleep(0.005)
    else:
        raise AssertionError("kombu publishes not queued")
    channel.queue_purge(queue)
    connection.close()
    return count / elapsed


def bench_consume_kombu(url, queue, count):
    connection = KombuConnection(url)
    channel = connection.default_channel
    declared = Queue(queue)
    declared(channel).declare()
    producer = Producer(channel)
    body = b'{"args": [1, 2]}'
    for _ in range(count):
        producer.publish(body, routing_key=queue)
    received = 0

    def on_message(body, message):
        nonlocal received
        received += 1
        message.ack()

    consumer = Consumer(
        channel, queues=[declared], callbacks=[on_message], prefetch_count=500
    )
    consumer.consume()

    def run():
        while received < count:
            connection.drain_events(timeout=2.0)

    elapsed = timed(run)
    assert received == count
    consumer.cancel()
    connection.close()
    return count / elapsed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--url",
        default=os.environ.get(
            "CELERY_RS_TEST_AMQP", os.environ.get('CELERY_RS_AMQP_URL', 'amqp://guest:guest@127.0.0.1:5673//')
        ),
    )
    parser.add_argument("--n", type=int, default=5000)
    args = parser.parse_args()

    suffix = uuid.uuid4().hex
    queues = {
        "rs-pub": f"bench.rs.pub.{suffix}",
        "rs-con": f"bench.rs.con.{suffix}",
        "k-pub": f"bench.kombu.pub.{suffix}",
        "k-con": f"bench.kombu.con.{suffix}",
    }


    bench_publish_rs(args.url, f"bench.rs.warm.{suffix}", 10)
    bench_publish_kombu(args.url, f"bench.kombu.warm.{suffix}", 10)

    results = {
        "rust publish/s": bench_publish_rs(args.url, queues["rs-pub"], args.n),
        "kombu publish/s": bench_publish_kombu(args.url, queues["k-pub"], args.n),
        "rust consume/s": bench_consume_rs(args.url, queues["rs-con"], args.n),
        "kombu consume/s": bench_consume_kombu(args.url, queues["k-con"], args.n),
    }
    for label, value in results.items():
        print(f"{label:20s} {value:10.1f}")
    return results


if __name__ == "__main__":
    main()
