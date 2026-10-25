"""Misura ``app.control.inspect(timeout=2, limit=1).ping()`` in un processo.

Usata da ``bench/bench_cli.py``: stampa una riga JSON con la latenza mediana
(p50) per chiamata su ``--n`` iterazioni e il RSS finale/picco del processo.
"""
import argparse
import json
import os
import resource
import time

from celery import Celery


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--n', type=int, default=1000)
    parser.add_argument('--timeout', type=float, default=2.0)
    args = parser.parse_args()

    app = Celery('bench_cli_probe', broker=os.environ['BENCH_CLI_BROKER'])
    app.conf.update(broker_connection_retry_on_startup=True)
    inspect = app.control.inspect

    # Warmup: connessione, coda di risposta, script Lua.
    assert inspect(timeout=args.timeout, limit=1).ping()

    times = []
    for _ in range(args.n):
        started = time.perf_counter()
        reply = inspect(timeout=args.timeout, limit=1).ping()
        times.append((time.perf_counter() - started) * 1000.0)
        if not reply:
            raise SystemExit('nessuna risposta dal worker')
    times.sort()
    usage = resource.getrusage(resource.RUSAGE_SELF)
    print(json.dumps({
        'n': args.n,
        'p50_ms': times[len(times) // 2],
        'p99_ms': times[min(len(times) - 1, int(len(times) * 0.99))],
        'rss_kb': usage.ru_maxrss,
    }))


if __name__ == '__main__':
    main()
