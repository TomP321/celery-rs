"""Benchmark del result backend ``rpc://`` (o Redis) lato client.

Confronta il client del riferimento e quello di celery-rs usando lo stesso
worker del riferimento:

    python3 bench/bench_result.py --backend rpc
    python3 bench/bench_result.py --backend rpc --venvs .venv-ref .venv-rs

Misure per client:
  * latenza ``apply_async().get()`` p50/p99 (ms);
  * throughput di ``join_native`` su N risultati (risultati/s);
  * RSS massimo del client (``ru_maxrss``, KB).

Il worker gira sempre con ``.venv-ref``: in questa fase il worker celery-rs
non esiste ancora, quindi la variabile misurata è il client.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import signal
import statistics
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
REF_VENV = ROOT / '.venv-ref'
REF_PYTHON = REF_VENV / 'bin' / 'python'

AMQP = 'amqp://guest:guest@127.0.0.1:5673//'
REDIS = 'redis://127.0.0.1:6390/0'
REDIS_BACKEND = 'redis://127.0.0.1:6390/1'


def unique(prefix):
    return f'{prefix}.{uuid.uuid4().hex}'


def venv_python(venv):
    return str((Path(venv).resolve() / 'bin' / 'python'))


def backend_settings(backend):
    if backend == 'rpc':
        return AMQP, 'rpc://'
    if backend == 'redis':
        return REDIS, REDIS_BACKEND
    raise SystemExit(f'backend sconosciuto: {backend}')


def worker_env(broker, result_backend, queue):
    env = {k: v for k, v in os.environ.items() if not k.startswith('PYTHON')}
    env.update({
        'BENCH_BROKER': broker,
        'BENCH_RESULT_BACKEND': result_backend,
        'BENCH_QUEUE': queue,
        'PYTHONPATH': str(HERE),
        'PYTHONHASHSEED': '0',
    })
    return env


def wait_ready(env, nodename, timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = subprocess.run(
            [str(REF_PYTHON), '-m', 'celery', '-A', 'bench_result_app',
             'inspect', 'ping', '-d', nodename, '-t', '1'],
            cwd=HERE, env=env, capture_output=True, text=True)
        if 'pong' in result.stdout:
            return
        time.sleep(0.5)
    raise SystemExit('worker non pronto')


CLIENT = r'''
import json
import resource
import sys
import time

from celery import group
from bench_result_app import add, noop

latency_n = int(sys.argv[1])
join_n = int(sys.argv[2])

add.delay(1, 1).get(timeout=60)  # warm-up connessioni e consumer

latencies = []
for index in range(latency_n):
    started = time.perf_counter()
    value = add.delay(index, 1).get(timeout=60)
    latencies.append(time.perf_counter() - started)
    assert value == index + 1, (value, index)

rss_before_join = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
started = time.perf_counter()
canvas = group(noop.s() for _ in range(join_n))
result = canvas.apply_async()
values = result.join_native(timeout=300)
elapsed = time.perf_counter() - started
assert len(values) == join_n, (len(values), join_n)
rss_after_join = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

print(json.dumps({
    'latency_s': latencies,
    'join_s': elapsed,
    'join_n': join_n,
    'rss_before_join_kb': rss_before_join,
    'rss_max_kb': rss_after_join,
}))
'''


def run_client(venv, env, latency_n, join_n):
    out = subprocess.run(
        [venv_python(venv), '-c', CLIENT, str(latency_n), str(join_n)],
        cwd=HERE, env=env, capture_output=True, text=True)
    if out.returncode != 0:
        raise SystemExit(
            f'client {venv} fallito:\n{out.stdout[-4000:]}\n{out.stderr[-8000:]}')
    return json.loads(out.stdout.strip().splitlines()[-1])


def delete_rabbit_queue(queue):
    """Rimuove la coda del worker dal broker condiviso (best effort)."""
    authenticator = 'Basic ' + base64.b64encode(b'guest:guest').decode()
    path = '/api/queues/%2F/' + urllib.parse.quote(queue, safe='')
    request = urllib.request.Request(
        'http://127.0.0.1:15673' + path, method='DELETE',
        headers={'Authorization': authenticator})
    try:
        with urllib.request.urlopen(request, timeout=5):
            pass
    except Exception:
        pass


def redis_clean():
    try:
        import redis
    except ImportError:  # pragma: no cover - solo con backend redis
        return
    client = redis.Redis.from_url('redis://127.0.0.1:6390/1')
    client.flushdb()
    redis.Redis.from_url('redis://127.0.0.1:6390/0').flushdb()


def benchmark(venv, args):
    broker, result_backend = backend_settings(args.backend)
    queue = unique('bench.result')
    env = worker_env(broker, result_backend, queue)
    nodename = unique('bench-result-worker') + '@localhost'
    worker = subprocess.Popen(
        [str(REF_PYTHON), '-m', 'celery', '-A', 'bench_result_app',
         'worker', '-P', 'prefork', '-c', str(args.concurrency),
         '-Q', queue, '-n', nodename, '-l', 'warning',
         '--without-gossip', '--without-mingle'],
        cwd=HERE, env=env, stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE, start_new_session=True)
    try:
        wait_ready(env, nodename)
        client = run_client(venv, env, args.latency_n, args.join_n)
    finally:
        try:
            os.killpg(worker.pid, signal.SIGTERM)
            worker.wait(30)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                os.killpg(worker.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if args.backend == 'rpc':
            delete_rabbit_queue(queue)

    latencies = sorted(value * 1000 for value in client['latency_s'])
    label = Path(venv).resolve().name.lstrip('.').removeprefix('venv-')
    return {
        'label': label,
        'venv': str(Path(venv).resolve()),
        'backend': args.backend,
        'latency_n': args.latency_n,
        'latency_ms': {
            'p50': statistics.median(latencies),
            'p99': latencies[int(len(latencies) * 0.99) - 1],
            'max': latencies[-1],
        },
        'join_n': client['join_n'],
        'join_s': client['join_s'],
        'join_results_per_s': client['join_n'] / client['join_s'],
        'rss_before_join_kb': client['rss_before_join_kb'],
        'rss_max_kb': client['rss_max_kb'],
        'python': sys.version.split()[0],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--backend', choices=('rpc', 'redis'), default='rpc')
    parser.add_argument('--venvs', nargs='+',
                        default=[str(ROOT / '.venv-ref'), str(ROOT / '.venv-rs')])
    parser.add_argument('--latency-n', type=int, default=200)
    parser.add_argument('--join-n', type=int, default=1000)
    parser.add_argument('--concurrency', type=int, default=4)
    args = parser.parse_args()

    if args.backend == 'redis':
        redis_clean()
    results = [benchmark(venv, args) for venv in args.venvs]
    payload = {'backend': args.backend, 'results': results}
    out = HERE / 'results' / f'result-{args.backend}.json'
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(payload, indent=1))
    print(json.dumps(payload, indent=1))

    if len(results) == 2:
        reference, ours = results
        print(f"latenza p50: {reference['latency_ms']['p50']:.3f} ms -> "
              f"{ours['latency_ms']['p50']:.3f} ms")
        print(f"latenza p99: {reference['latency_ms']['p99']:.3f} ms -> "
              f"{ours['latency_ms']['p99']:.3f} ms")
        print(f"join_native: {reference['join_results_per_s']:.0f} -> "
              f"{ours['join_results_per_s']:.0f} risultati/s")
        print(f"RSS client: {reference['rss_max_kb']} -> "
              f"{ours['rss_max_kb']} KB")


if __name__ == '__main__':
    main()
