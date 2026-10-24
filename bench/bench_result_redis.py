"""Benchmark del result backend Redis: riferimento vs client Rust.

    .venv-ref/bin/python bench/bench_result_redis.py --flavor ref
    .venv-rs/bin/python  bench/bench_result_redis.py --flavor rs

Misura, con un worker del **riferimento** (``.venv-ref``) sullo stesso Redis:

- latenza ``apply_async().get()``: p50/p99 su ``--n`` task;
- throughput ``get_many`` e ``join_native`` su ``--bulk`` risultati;
- RSS del processo client (corrente e picco).

I risultati sono scritti in ``bench/results/backend-redis-<flavor>.json``.
Redis e database sono dedicati (``BENCH_BROKER_PORT``/db 6 e 7) e vengono
azzerati prima della misura: eseguire sotto ``tools/dev/with-brokers``.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))

REDIS_PORT = os.environ.get('BENCH_REDIS_PORT', '6390')
BROKER = os.environ.get('BENCH_BROKER', f'redis://127.0.0.1:{REDIS_PORT}/6')
BACKEND = os.environ.get('BENCH_BACKEND', f'redis://127.0.0.1:{REDIS_PORT}/7')
QUEUE = os.environ.get('BENCH_QUEUE', f'bench-result-{uuid.uuid4().hex[:8]}')

# `benchapp` legge queste variabili all'import: il client deve usare gli stessi
# URL del worker, altrimenti pubblica su un altro database.
os.environ['BENCH_BROKER'] = BROKER
os.environ['BENCH_BACKEND'] = BACKEND


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round(fraction * len(ordered))) - 1))
    return ordered[index]


def rss_kb():
    data = {}
    with open('/proc/self/status') as handle:
        for line in handle:
            if line.startswith(('VmRSS:', 'VmHWM:')):
                name, value, _ = line.split()
                data[name.rstrip(':')] = int(value)
    return {'rss_kb': data.get('VmRSS'), 'peak_rss_kb': data.get('VmHWM')}


class Worker:
    def __init__(self, python):
        self.python = str(python)
        self.process = None
        self.lines = []

    def start(self):
        env = dict(os.environ)
        env.update(BENCH_BROKER=BROKER, BENCH_BACKEND=BACKEND)
        self.process = subprocess.Popen(
            [self.python, '-m', 'celery', '-A', 'benchapp', 'worker',
             '-P', 'solo', '-Q', QUEUE, '--loglevel=INFO',
             '--without-heartbeat', '--without-gossip', '--without-mingle'],
            cwd=HERE, env=env, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True,
        )
        thread = threading.Thread(target=self._read, daemon=True)
        thread.start()
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if any('ready.' in line for line in self.lines):
                return
            if self.process.poll() is not None:
                break
            time.sleep(0.1)
        self.stop()
        raise RuntimeError(
            f'worker non pronto:\n{"".join(self.lines[-30:])}')

    def _read(self):
        assert self.process is not None and self.process.stdout is not None
        for line in self.process.stdout:
            self.lines.append(line)

    def stop(self):
        if self.process is None:
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=20)
        except subprocess.TimeoutExpired:  # pragma: no cover
            self.process.kill()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--flavor', choices=('ref', 'rs'), required=True)
    parser.add_argument('--worker-python',
                        default=str(ROOT / '.venv-ref' / 'bin' / 'python'))
    parser.add_argument('--n', type=int, default=200,
                        help='numero di task per la latenza end-to-end')
    parser.add_argument('--bulk', type=int, default=1000,
                        help='risultati per get_many/join')
    parser.add_argument('--no-worker', action='store_true')
    parser.add_argument('--out', default=None)
    args = parser.parse_args()

    import redis
    from celery import Celery

    assert BROKER.startswith('redis://') and BACKEND.startswith('redis://')
    for url in (BROKER, BACKEND):
        redis.Redis.from_url(url).flushdb()

    app = Celery('bench-result', broker=BROKER, backend=BACKEND)
    app.conf.update(
        task_default_queue=QUEUE,
        result_expires=600,
        broker_connection_retry_on_startup=True,
        worker_hijack_root_logger=False,
    )
    from benchapp import add  # noqa: E402 - usa l'app del modulo (stesso task)

    worker = Worker(args.worker_python)
    if not args.no_worker:
        worker.start()
    try:
        # Riscaldamento: la prima connessione paga l'handshake.
        add.apply_async((0, 0), queue=QUEUE).get(timeout=60)

        latencies = []
        for index in range(args.n):
            started = time.perf_counter()
            value = add.apply_async((index, index), queue=QUEUE).get(timeout=60)
            latencies.append((time.perf_counter() - started) * 1000.0)
            assert value == index * 2
        latencies_ms = {
            'p50': percentile(latencies, 0.50),
            'p90': percentile(latencies, 0.90),
            'p99': percentile(latencies, 0.99),
            'mean': statistics.fmean(latencies),
            'min': min(latencies),
            'max': max(latencies),
            'count': len(latencies),
        }

        backend = app.backend
        ids = [str(uuid.uuid4()) for _ in range(args.bulk)]
        for index, task_id in enumerate(ids):
            backend.store_result(task_id, index, 'SUCCESS')

        started = time.perf_counter()
        fetched = list(backend.get_many(ids))
        get_many_seconds = time.perf_counter() - started
        assert len(fetched) == args.bulk

        results = [app.AsyncResult(task_id) for task_id in ids]
        started = time.perf_counter()
        values = app.GroupResult(str(uuid.uuid4()), results).join_native(
            timeout=120)
        join_seconds = time.perf_counter() - started
        assert len(values) == args.bulk

        report = {
            'flavor': args.flavor,
            'python': sys.executable,
            'celery': getattr(__import__('celery'), '__version__', None),
            'broker': BROKER,
            'backend': BACKEND,
            'worker_python': None if args.no_worker else str(args.worker_python),
            'latency_ms': latencies_ms,
            'get_many': {
                'count': args.bulk,
                'seconds': get_many_seconds,
                'per_second': args.bulk / get_many_seconds,
            },
            'join_native': {
                'count': args.bulk,
                'seconds': join_seconds,
                'per_second': args.bulk / join_seconds,
            },
            **rss_kb(),
        }
    finally:
        if not args.no_worker:
            worker.stop()

    output = Path(args.out) if args.out else (
        HERE / 'results' / f'backend-redis-{args.flavor}.json')
    output.write_text(json.dumps(report, indent=1, sort_keys=True) + '\n')
    print(json.dumps(report, indent=1, sort_keys=True))


if __name__ == '__main__':
    main()
