#!/usr/bin/env python3
"""Sonda c1, carico misto e riavvio del broker, durata massima 5 minuti.

Eseguire sotto tools/dev/with-brokers con .venv-rs/bin/python.
--trace richiede una build memory-probe per includere l'heap Rust.
--without-cleanup riproduce il comportamento precedente alla 09c.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

from soak import ROOT, descendants, restart_broker


def run(args):
    if os.environ.get('CELERY_RS_BROKER_LOCKED') != '1':
        raise RuntimeError('eseguire sotto tools/dev/with-brokers')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    redis = os.environ['CELERY_RS_REDIS_URL']
    env = dict(os.environ, SOAK_QUEUE='memory-' + uuid.uuid4().hex,
               SOAK_BACKEND=redis + '/13',
               SOAK_BROKER=redis + '/12' if args.broker == 'redis'
               else os.environ['CELERY_RS_AMQP_URL'])
    env.pop('CELERY_RS_MEMORY_PROBE', None)
    env.pop('CELERY_RS_MEMORY_PROBE_RECLAIM', None)
    if args.trace:
        env['CELERY_RS_MEMORY_PROBE'] = str(output)
        if args.reclaim:
            env['CELERY_RS_MEMORY_PROBE_RECLAIM'] = '1'
    command = ['-m', 'celery', '-A', 'soak_app:app', 'worker',
               '-P', 'prefork', '-c', '1', '-l', 'ERROR']
    if args.without_cleanup:
        command = ['-c', "import runpy; from celery.concurrency import _rs_child; "
                   "_rs_child._clear_result_traceback = lambda result: None; "
                   "runpy.run_module('celery', run_name='__main__')", *command[2:]]
    commands = [command, [str(ROOT / 'bench/soak.py'), '--load']]
    processes = []
    (output / 'options.json').write_text(json.dumps(dict(
        broker=args.broker, seconds=args.seconds, trace=args.trace,
        reclaim=args.reclaim, cleanup=not args.without_cleanup)))
    try:
        for label, command in zip(('worker', 'load'), commands):
            with (output / f'{label}.log').open('w') as log:
                processes.append(subprocess.Popen(
                    [sys.executable, *command], cwd=ROOT / 'bench', env=env,
                    stdout=log, stderr=subprocess.STDOUT, start_new_session=True))
        start = time.monotonic()
        restarted = False
        with (output / 'processes.jsonl').open('w') as stream:
            while True:
                elapsed = time.monotonic() - start
                if any(process.poll() is not None for process in processes):
                    raise RuntimeError('worker o produttore terminato prematuramente')
                stream.write(json.dumps(dict(seconds=elapsed,
                    processes=descendants(processes[0].pid))) + '\n')
                stream.flush()
                if elapsed >= args.seconds:
                    break
                if not restarted and elapsed >= args.seconds / 2:
                    restart_broker(args.broker)
                    restarted = True
                time.sleep(min(10, max(0, args.seconds - (time.monotonic() - start))))
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
        for process in reversed(processes):
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--broker', choices=('redis', 'amqp'), required=True)
    parser.add_argument('--seconds', type=int, default=120)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--trace', action='store_true')
    parser.add_argument('--reclaim', action='store_true')
    parser.add_argument('--without-cleanup', action='store_true')
    args = parser.parse_args()
    if not 1 <= args.seconds <= 300 or (args.reclaim and not args.trace):
        parser.error('durata 1–300 secondi; --reclaim richiede --trace')
    run(args)
