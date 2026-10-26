"""Benchmark memoria/throughput/latenza di un worker Celery (riferimento o celery-rs).

    python3 bench/bench.py --venv .venv-ref [--concurrency 4] [--n 20000] [--label ref]

Avvia il worker del venv indicato, misura e salva bench/results/<label>.json.
La memoria è PSS (/proc/<pid>/smaps_rollup) sommata sull'intero albero di processi del worker:
conta correttamente le pagine condivise copy-on-write tra master e figli.
"""
import argparse
import json
import os
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
REDIS = ('127.0.0.1', 6390)


def tree(pid):
    pids, todo = [], [pid]
    while todo:
        p = todo.pop()
        pids.append(p)
        try:
            for t in os.listdir(f'/proc/{p}/task'):
                with open(f'/proc/{p}/task/{t}/children') as fh:
                    todo += [int(c) for c in fh.read().split()]
        except OSError:
            pass
    return pids


def pss_kb(pid):
    total, per = 0, []
    for p in tree(pid):
        try:
            with open(f'/proc/{p}/smaps_rollup') as fh:
                for line in fh:
                    if line.startswith('Pss:'):
                        kb = int(line.split()[1])
                        total += kb
                        per.append(kb)
                        break
        except OSError:
            pass
    return total, per


def venv_python(venv):
    return os.path.join(os.path.abspath(venv), 'bin', 'python')


def client(venv, code, env):
    """Esegue codice client nel venv indicato e ritorna il JSON stampato su stdout."""
    out = subprocess.run([venv_python(venv), '-c', code], env=env, cwd=HERE,
                         capture_output=True, text=True, check=True)
    return json.loads(out.stdout.strip().splitlines()[-1])


PUBLISH = '''
import json, resource, time
from benchapp import noop, counter, COUNTER
counter().delete(COUNTER)
noop.delay()  # warm-up della connessione
counter().delete(COUNTER)
rss0 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
wall0 = time.time()
t0 = time.perf_counter()
for _ in range({n}):
    noop.delay()
dt = time.perf_counter() - t0
print(json.dumps({{"publish_s": dt, "wall_start": wall0, "maxrss_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                  "maxrss_before_kb": rss0}}))
'''

LATENCY = '''
import json, time
from benchapp import add
add.delay(1, 1).get(timeout=30)
lat = []
for i in range({n}):
    t0 = time.perf_counter()
    assert add.delay(i, 1).get(timeout=30) == i + 1
    lat.append(time.perf_counter() - t0)
print(json.dumps(lat))
'''


def wait_ready(proc, timeout=60):
    # "ready." è stampato dal worker (anche con loglevel warning il banner va su stdout)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise SystemExit(f'worker terminato con codice {proc.returncode}')
        try:
            out = subprocess.run([venv_python(ARGS.venv), '-m', 'celery', '-A', 'benchapp', 'inspect',
                                  'ping', '-t', '1'], cwd=HERE, env=ENV, capture_output=True, text=True)
            if 'pong' in out.stdout:
                return
        except OSError:
            pass
        time.sleep(0.5)
    raise SystemExit('worker non pronto')


def main():
    global ARGS, ENV
    ap = argparse.ArgumentParser()
    ap.add_argument('--venv', required=True)
    ap.add_argument('--label')
    ap.add_argument('--concurrency', type=int, default=4)
    ap.add_argument('--pool', default='prefork')
    ap.add_argument('--n', type=int, default=20000)
    ap.add_argument('--latency-n', type=int, default=500)
    ARGS = args = ap.parse_args()
    label = args.label or Path(args.venv).name.lstrip('.').removeprefix('venv-')
    ENV = env = {**{k: v for k, v in os.environ.items() if not k.startswith('PYTHON')},
                 'PYTHONPATH': str(HERE), 'PYTHONHASHSEED': '0'}

    import redis
    r = redis.Redis(*REDIS)
    r.flushall()

    worker = subprocess.Popen(
        [venv_python(args.venv), '-m', 'celery', '-A', 'benchapp', 'worker', '-P', args.pool,
         '-c', str(args.concurrency), '-l', 'warning', '--without-gossip'],
        cwd=HERE, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, start_new_session=True)
    try:
        wait_ready(worker)
        time.sleep(3)
        idle_total, idle_per = pss_kb(worker.pid)

        import threading
        peak = [0]
        sampling = threading.Event()

        def sample():
            while not sampling.is_set():
                peak[0] = max(peak[0], pss_kb(worker.pid)[0])
                time.sleep(0.05)
        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()

        pub = client(args.venv, PUBLISH.format(n=args.n), env)
        cnt = redis.Redis(*REDIS, db=2)
        t0 = time.perf_counter()
        while int(cnt.get('bench:done') or 0) < args.n:
            if time.perf_counter() - t0 > 600:
                raise SystemExit('timeout in attesa del completamento')
            time.sleep(0.02)
        sampling.set()
        sampler.join()
        peak = peak[0]
        # end-to-end: dal primo publish all'ultimo task eseguito (il worker consuma già durante
        # la pubblicazione); drain = la coda rimasta dopo che il client ha finito
        drain_s = time.perf_counter() - t0
        total_s = time.time() - pub['wall_start']

        lat = client(args.venv, LATENCY.format(n=args.latency_n), env)
        after_total, after_per = pss_kb(worker.pid)
    finally:
        os.killpg(worker.pid, signal.SIGTERM)
        try:
            worker.wait(30)
        except subprocess.TimeoutExpired:
            os.killpg(worker.pid, signal.SIGKILL)

    lat_ms = sorted(x * 1000 for x in lat)
    res = {
        'label': label, 'venv': args.venv, 'pool': args.pool, 'concurrency': args.concurrency,
        'n': args.n,
        'pss_idle_kb': idle_total, 'pss_idle_per_process_kb': idle_per,
        'pss_peak_kb': peak, 'pss_after_kb': after_total, 'pss_after_per_process_kb': after_per,
        'client_publish_per_s': args.n / pub['publish_s'],
        'client_maxrss_kb': pub['maxrss_kb'],
        'end_to_end_s': total_s,
        'end_to_end_tasks_per_s': args.n / total_s,
        'drain_after_publish_s': drain_s,
        'latency_ms': {'p50': statistics.median(lat_ms),
                       'p99': lat_ms[int(len(lat_ms) * 0.99) - 1], 'max': lat_ms[-1]},
        'python': sys.version.split()[0],
    }
    out = HERE / 'results' / f'{label}.json'
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(res, indent=1))
    print(json.dumps(res, indent=1))


if __name__ == '__main__':
    main()
