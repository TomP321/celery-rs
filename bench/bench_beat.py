"""Benchmark di ``celery beat``: riferimento vs celery-rs.

    tools/dev/with-brokers .venv-ref/bin/python bench/bench_beat.py --venv .venv-ref --label ref
    tools/dev/with-brokers .venv-rs/bin/python  bench/bench_beat.py --venv .venv-rs  --label rs
    .venv-rs/bin/python bench/bench_beat.py --compare ref rs

Misure per broker (Redis e RabbitMQ):
  * PSS del processo beat a riposo con 3 voci e con 1000 voci (500 a intervallo
    1 s + 500 crontab misti), letta da /proc/<pid>/smaps_rollup;
  * CPU per ``Scheduler.tick`` con 1000 voci non dovute (µs, ``process_time``);
  * invii/s sostenuti con 1000 voci dovute (``Scheduler.tick`` in un ciclo);
  * ``crontab.remaining_estimate``/``is_due`` (ns/chiamata), schedule semplice
    e con tutti i campi.

I numeri sono salvati in ``bench/results/beat-<label>.json``.
"""
from __future__ import annotations

import argparse
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
RESULTS = HERE / 'results'

BROKERS = {
    'redis': os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/5',
    'amqp': os.environ.get('CELERY_RS_AMQP_URL', 'amqp://guest:guest@127.0.0.1:5673//'),
}

MICROBENCH = r'''
import json, timeit
from datetime import datetime, timezone
from celery import Celery
from celery.schedules import crontab

app = Celery('bench-cron')
app.conf.enable_utc = True
simple = crontab('*', '*', '*', app=app)
mixed = crontab('*/5', '0-6', 'mon-fri', '1-7', '2,4,6,8,10,12', app=app)
last = datetime.now(timezone.utc)
n = 20000

def measure(fn):
    return timeit.timeit(fn, number=n) / n * 1e9

out = {
    'remaining_estimate_simple_ns': measure(lambda: simple.remaining_estimate(last)),
    'is_due_simple_ns': measure(lambda: simple.is_due(last)),
    'remaining_estimate_mixed_ns': measure(lambda: mixed.remaining_estimate(last)),
    'is_due_mixed_ns': measure(lambda: mixed.is_due(last)),
}
print(json.dumps(out))
'''

TICK = r'''
import json, os, time
from datetime import timedelta
from celery import Celery
from celery.beat import Scheduler
from celery.schedules import crontab

N = 1000
app = Celery('bench-tick', broker=os.environ['BENCH_BROKER'])
app.conf.update(worker_hijack_root_logger=False, beat_max_loop_interval=0.1)
now = app.now()
entries = {}
for i in range(N // 2):
    entries[f'interval-{i}'] = Scheduler.Entry(
        name=f'interval-{i}', task='bench.noop', schedule=3600.0,
        last_run_at=now, args=(i,), app=app)
for i in range(N // 2):
    spec = crontab(minute=str(i % 60), hour=str(i % 24),
                   day_of_week=str(i % 7), day_of_month='*',
                   month_of_year='*', app=app)
    entries[f'cron-{i}'] = Scheduler.Entry(
        name=f'cron-{i}', task='bench.noop', schedule=spec,
        last_run_at=now, args=(i,), app=app)
scheduler = Scheduler(app=app, schedule=entries, lazy=True)
scheduler.tick()  # popola l'heap
loops = 2000
cpu0 = time.process_time()
wall0 = time.perf_counter()
for _ in range(loops):
    scheduler.tick()
cpu = time.process_time() - cpu0
wall = time.perf_counter() - wall0
print(json.dumps({'tick_us': cpu / loops * 1e6,
                  'tick_wall_us': wall / loops * 1e6,
                  'entries': N}))
'''

SENDS = r'''
import json, os, time
from datetime import timedelta
from celery import Celery
from celery.beat import Scheduler
from celery.schedules import crontab

N = 1000
app = Celery('bench-sends', broker=os.environ['BENCH_BROKER'])
app.conf.update(worker_hijack_root_logger=False, beat_max_loop_interval=0.1,
                task_ignore_result=True)
now = app.now()
entries = {}
for i in range(N // 2):
    entries[f'interval-{i}'] = Scheduler.Entry(
        name=f'interval-{i}', task='bench.noop',
        schedule=float(os.environ.get('BENCH_INTERVAL', '0.05')),
        last_run_at=now - timedelta(seconds=10), args=(i,), app=app)
for i in range(N // 2):
    spec = crontab(minute='*', app=app)
    entries[f'cron-{i}'] = Scheduler.Entry(
        name=f'cron-{i}', task='bench.noop', schedule=spec,
        last_run_at=now - timedelta(seconds=90), args=(i,), app=app)
scheduler = Scheduler(app=app, schedule=entries, lazy=True)
sends = [0]
original = scheduler.apply_entry

def counting_apply_entry(entry, producer=None):
    sends[0] += 1
    return original(entry, producer=producer)

scheduler.apply_entry = counting_apply_entry
duration = float(os.environ.get('BENCH_SECONDS', '3'))
cpu0 = time.process_time()
start = time.perf_counter()
while time.perf_counter() - start < duration:
    scheduler.tick()
elapsed = time.perf_counter() - start
cpu = time.process_time() - cpu0
scheduler.close()
print(json.dumps({'sends': sends[0], 'seconds': elapsed,
                  'sends_s': sends[0] / elapsed,
                  'cpu_sends_s': sends[0] / cpu if cpu else None}))
'''

BEAT_APP = r'''
import os
from datetime import timedelta
from celery import Celery
from celery.schedules import crontab

count = int(os.environ.get('BENCH_ENTRIES', '3'))
app = Celery('bench-beat-app', broker=os.environ['BENCH_BROKER'])
app.conf.update(
    worker_hijack_root_logger=False,
    beat_max_loop_interval=1.0,
    broker_connection_max_retries=1,
)
schedule = {}
if count <= 3:
    for i in range(count):
        schedule[f'idle-{i}'] = {
            'task': 'bench.noop', 'schedule': 3600.0, 'args': (i,),
        }
else:
    for i in range(count // 2):
        schedule[f'interval-{i}'] = {
            'task': 'bench.noop', 'schedule': 1.0, 'args': (i,),
        }
    for i in range(count // 2):
        schedule[f'cron-{i}'] = {
            'task': 'bench.noop',
            'schedule': crontab(
                minute=str(i % 60), hour=str(i % 24),
                day_of_week=str(i % 7), day_of_month='*',
                month_of_year='*'),
            'args': (i,),
        }
app.conf.beat_schedule = schedule
'''


def venv_python(venv):
    return os.path.join(os.path.abspath(venv), 'bin', 'python')


def child(venv, code, env, timeout=180):
    env = dict(env)
    env.setdefault('PYTHONPATH', str(HERE))
    result = subprocess.run(
        [venv_python(venv), '-c', code], cwd=HERE, env=env,
        capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f'comando fallito ({result.returncode}):\n{result.stdout[-2000:]}\n'
            f'{result.stderr[-4000:]}')
    return json.loads(result.stdout.strip().splitlines()[-1])


def pss_kb(pid):
    try:
        with open(f'/proc/{pid}/smaps_rollup') as handle:
            for line in handle:
                if line.startswith('Pss:'):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


def beat_pss(venv, broker_name, entries, directory):
    """PSS del processo beat dopo l'avvio, con `entries` voci in schedule."""
    app_path = Path(directory) / 'bench_beat_app.py'
    app_path.write_text(BEAT_APP)
    workdir = Path(directory) / f'pss-{broker_name}-{entries}'
    workdir.mkdir(parents=True, exist_ok=True)
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith('PYTHON') and not key.startswith('CELERY_')
    }
    env.update(BENCH_BROKER=BROKERS[broker_name],
               BENCH_ENTRIES=str(entries),
               PYTHONPATH=str(Path(directory)))
    log_path = workdir / 'beat.log'
    with open(log_path, 'w') as log:
        process = subprocess.Popen(
            [venv_python(venv), '-m', 'celery', '-A', 'bench_beat_app',
             'beat', '-s', str(workdir / 'schedule'), '-l', 'info'],
            cwd=workdir, env=env, stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True)
    try:
        deadline = time.monotonic() + 60
        while 'beat: Starting...' not in log_path.read_text(errors='replace'):
            if time.monotonic() > deadline or process.poll() is not None:
                raise RuntimeError(
                    'beat non partito:\n' +
                    log_path.read_text(errors='replace')[-3000:])
            time.sleep(0.05)
        time.sleep(2.0)
        return pss_kb(process.pid)
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
        try:
            process.wait(20)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()


def measure(venv, brokers, workers=False):
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('PYTHON') and not key.startswith('CELERY_')}
    out = {'micro': child(venv, MICROBENCH, env), 'brokers': {}}
    with tempfile.TemporaryDirectory(prefix='bench-beat-') as directory:
        for name in brokers:
            broker_env = dict(env, BENCH_BROKER=BROKERS[name])
            result = {
                'pss_3_kb': beat_pss(venv, name, 3, directory),
                'pss_1000_kb': beat_pss(venv, name, 1000, directory),
                'tick': child(venv, TICK, broker_env),
                'sends': child(venv, SENDS, broker_env),
            }
            out['brokers'][name] = result
    return out


def print_report(label, data):
    print(f'\n== {label} ==')
    micro = data['micro']
    print(f"remaining_estimate  semplice {micro['remaining_estimate_simple_ns']:8.0f} ns"
          f"   misto {micro['remaining_estimate_mixed_ns']:8.0f} ns")
    print(f"is_due              semplice {micro['is_due_simple_ns']:8.0f} ns"
          f"   misto {micro['is_due_mixed_ns']:8.0f} ns")
    for name, result in data['brokers'].items():
        tick = result['tick']
        sends = result['sends']
        print(f'{name:7} PSS 3 voci {result["pss_3_kb"] / 1024:6.1f} MB'
              f'   1000 voci {result["pss_1000_kb"] / 1024:6.1f} MB'
              f'   tick {tick["tick_us"]:7.1f} µs'
              f'   invii {sends["sends_s"]:8.0f}/s'
              f'   CPU invii {sends["cpu_sends_s"] or 0:8.0f}/s')


def compare(labels):
    data = {}
    for label in labels:
        path = RESULTS / f'beat-{label}.json'
        if not path.exists():
            raise SystemExit(f'manca {path}: esegui prima il bench con --label {label}')
        data[label] = json.loads(path.read_text())
    print('\n== ref vs rs ==')
    first, second = labels
    def pair(a, b, fmt):
        return f'{fmt.format(a)} vs {fmt.format(b)}'
    micro_a, micro_b = data[first]['micro'], data[second]['micro']
    for key in micro_a:
        print(f'{key:32} {pair(micro_a[key], micro_b[key], "{:.0f}")} ns')
    for broker in data[first]['brokers']:
        a = data[first]['brokers'][broker]
        b = data[second]['brokers'][broker]
        print(f'{broker}: PSS 3 voci {pair(a["pss_3_kb"]/1024, b["pss_3_kb"]/1024, "{:.1f}")} MB')
        print(f'{broker}: PSS 1000 voci {pair(a["pss_1000_kb"]/1024, b["pss_1000_kb"]/1024, "{:.1f}")} MB')
        print(f'{broker}: tick {pair(a["tick"]["tick_us"], b["tick"]["tick_us"], "{:.1f}")} µs')
        print(f'{broker}: invii/s {pair(a["sends"]["sends_s"], b["sends"]["sends_s"], "{:.0f}")}')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--venv', default=None,
                        help='virtualenv da misurare (es. .venv-ref)')
    parser.add_argument('--label', default=None,
                        help='nome del risultato (es. ref o rs)')
    parser.add_argument('--broker', nargs='+', default=['redis', 'amqp'],
                        choices=list(BROKERS))
    parser.add_argument('--compare', nargs=2, metavar=('A', 'B'))
    args = parser.parse_args(argv)
    if args.compare:
        compare(args.compare)
        return 0
    if not (args.venv and args.label):
        parser.error('servono --venv e --label (oppure --compare A B)')
    data = measure(args.venv, args.broker)
    data['label'] = args.label
    RESULTS.mkdir(parents=True, exist_ok=True)
    path = RESULTS / f'beat-{args.label}.json'
    path.write_text(json.dumps(data, indent=1))
    print_report(args.label, data)
    print(f'\nsalvato in {path}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
