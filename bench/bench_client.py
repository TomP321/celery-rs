"""Memoria del processo client: riferimento vs celery-rs.

    tools/dev/with-brokers .venv-ref/bin/python bench/bench_client.py

Per ogni venv e broker (Redis, RabbitMQ) avvia un interprete pulito e misura la PSS
(/proc/self/smaps_rollup) e i moduli caricati dopo ciascun passo tipico di un client:
``import celery``, definizione dell'app e di un task, ``delay()``, ``AsyncResult.get()``
(un worker del riferimento esegue il task). Salva bench/results/client.json.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
VENVS = ('ref', 'rs')
BROKERS = {
    'redis': 'redis://127.0.0.1:6390/7',
    'amqp': 'amqp://guest:guest@127.0.0.1:5673//',
}
BACKEND = 'redis://127.0.0.1:6390/8'
WATCH = ('kombu.connection', 'kombu.serialization', 'kombu.transport', 'redis', 'amqp', 'billiard',
         'click', 'yaml', 'msgpack', 'greenlet', 'cffi', 'ssl', 'multiprocessing', 'importlib.metadata',
         'dateutil', 'decimal', 'email')

APP = '''
from celery import Celery
app = Celery('benchclient', broker={broker!r}, backend={backend!r})
app.conf.broker_connection_retry_on_startup = True

@app.task
def add(x, y):
    return x + y
'''

PROBE = r'''
import json, sys
def pss():
    d = {}
    for line in open('/proc/self/smaps_rollup'):
        k, _, v = line.partition(':')
        if k in ('Pss', 'Pss_Anon'):
            d[k] = int(v.split()[0])
    return d
WATCH = %r
def step(name):
    loaded = [w for w in WATCH if any(m == w or m.startswith(w + '.') for m in sys.modules)]
    out.append({'step': name, **pss(), 'modules': len(sys.modules), 'watch': loaded})
out = []
step('start')
import celery
step('import celery')
from client_app import add
step('app + task')
r = add.delay(2, 3)
step('delay')
assert r.get(timeout=30) == 5
step('get')
print(json.dumps(out))
'''


def run(venv, broker, workdir):
    env = {**{k: v for k, v in os.environ.items() if not k.startswith('PYTHON')},
           'PYTHONPATH': str(workdir), 'PYTHONHASHSEED': '0'}
    py = str(ROOT / f'.venv-{venv}' / 'bin' / 'python')
    out = subprocess.run([py, '-c', PROBE % (WATCH,)], cwd=workdir, env=env,
                         capture_output=True, text=True, timeout=120)
    if out.returncode:
        raise SystemExit(out.stderr[-3000:])
    return json.loads(out.stdout.strip().splitlines()[-1])


def main():
    results = {}
    for bname, broker in BROKERS.items():
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, 'client_app.py').write_text(APP.format(broker=broker, backend=BACKEND))
            env = {**{k: v for k, v in os.environ.items() if not k.startswith('PYTHON')},
                   'PYTHONPATH': tmp}
            worker = subprocess.Popen(
                [str(ROOT / '.venv-ref' / 'bin' / 'python'), '-m', 'celery', '-A', 'client_app', 'worker',
                 '-P', 'solo', '-l', 'warning', '--without-gossip', '--without-mingle'],
                cwd=tmp, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True)
            try:
                time.sleep(4)
                for venv in VENVS:
                    results[f'{venv}-{bname}'] = run(venv, broker, tmp)
            finally:
                os.killpg(worker.pid, signal.SIGTERM)
                worker.wait(30)

    (HERE / 'results').mkdir(exist_ok=True)
    (HERE / 'results' / 'client.json').write_text(json.dumps(results, indent=1))
    for bname in BROKERS:
        print(f'== {bname}')
        ref, rs = results[f'ref-{bname}'], results[f'rs-{bname}']
        for a, b in zip(ref, rs):
            print(f'  {a["step"]:13} PSS ref {a["Pss"] / 1024:5.1f} MB  rs {b["Pss"] / 1024:5.1f} MB'
                  f'   moduli ref {a["modules"]:4} rs {b["modules"]:4}')
        print(f'  rs carica ancora: {", ".join(rs[-1]["watch"]) or "-"}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
