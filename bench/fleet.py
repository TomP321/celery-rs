"""Quanti worker `-P prefork -c N` entrano in un budget di RAM: riferimento vs celery-rs.

    tools/dev/with-brokers .venv-rs/bin/python bench/fleet.py --venv .venv-rs --budget-mb 8192

Avvia un worker alla volta (stessa coda, nomi diversi) finché la PSS totale di tutti gli alberi di
processi supera il budget; poi invia un carico di task `pure` a tutta la flotta e rimisura.
La PSS somma correttamente le pagine condivise (librerie, copy-on-write) fra tutti i processi.
Salva bench/results/fleet-<label>.json.
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from bench import pss_kb, tree  # noqa: E402

REDIS = os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390')


def total_pss_mb(pids):
    return sum(pss_kb(pid)[0] for pid in pids) / 1024


def process_count(pids):
    return sum(len(tree(pid)) for pid in pids)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--venv', required=True)
    ap.add_argument('--label')
    ap.add_argument('--concurrency', type=int, default=10)
    ap.add_argument('--budget-mb', type=float, default=8192)
    ap.add_argument('--max-workers', type=int, default=200)
    ap.add_argument('--tasks', type=int, default=50000)
    args = ap.parse_args()
    label = args.label or Path(args.venv).name.lstrip('.').removeprefix('venv-')
    done_file = Path(f'/tmp/fleet-{label}-{os.getpid()}.done')
    done_file.unlink(missing_ok=True)
    env = {**{k: v for k, v in os.environ.items() if not k.startswith('PYTHON')},
           'PYTHONPATH': str(HERE), 'BENCH_BROKER': REDIS + '/0', 'BENCH_BACKEND': REDIS + '/1',
           'BENCH_QUEUE': f'fleet-{label}', 'BENCH_PURE_DONE': str(done_file)}
    python = str(Path(args.venv).resolve() / 'bin' / 'python')
    subprocess.run([python, '-c', 'import redis,os; redis.Redis.from_url(os.environ["BENCH_BROKER"]).flushdb()'],
                   env=env, check=False)
    workers, series = [], []
    try:
        while len(workers) < args.max_workers:
            n = len(workers)
            proc = subprocess.Popen(
                [python, '-m', 'celery', '-A', 'benchapp', 'worker', '-P', 'prefork',
                 '-c', str(args.concurrency), '-l', 'warning', '-n', f'fleet{n}@%h',
                 '-Q', env['BENCH_QUEUE'], '--without-gossip', '--without-mingle'],
                cwd=HERE, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True)
            workers.append(proc)
            # pronto quando tutti i figli esistono e la PSS si è assestata
            expected = args.concurrency + 1
            deadline = time.time() + 60
            while time.time() < deadline and len(tree(proc.pid)) < expected:
                time.sleep(0.2)
            time.sleep(1.5)
            pids = [w.pid for w in workers]
            total = total_pss_mb(pids)
            series.append({'workers': len(workers), 'pss_mb': round(total, 1),
                           'processes': process_count(pids)})
            print(f'{label}: {len(workers)} worker, {total:.0f} MB', flush=True)
            if total > args.budget_mb:
                break
        fit_idle = max(s['workers'] for s in series if s['pss_mb'] <= args.budget_mb)
        idle_total = series[-1]['pss_mb']
        # carico: task pure distribuiti su tutta la flotta
        publish = ('from benchapp import noop_pure\n'
                   f'for _ in range({args.tasks}): noop_pure.delay()\n')
        t0 = time.time()
        subprocess.run([python, '-c', publish], cwd=HERE, env=env, check=True)
        deadline = time.time() + 900
        while time.time() < deadline:
            if done_file.exists() and done_file.stat().st_size >= args.tasks:
                break
            time.sleep(0.5)
        elapsed = time.time() - t0
        time.sleep(3)
        pids = [w.pid for w in workers]
        loaded_total = total_pss_mb(pids)
        per_worker_loaded = loaded_total / len(workers)
        result = {
            'label': label, 'venv': args.venv, 'concurrency': args.concurrency,
            'budget_mb': args.budget_mb, 'series': series,
            'workers_fit_idle': fit_idle,
            'idle_total_mb': idle_total, 'idle_per_worker_mb': round(idle_total / len(workers), 1),
            'loaded_total_mb': round(loaded_total, 1),
            'loaded_per_worker_mb': round(per_worker_loaded, 1),
            'workers_fit_loaded': int(args.budget_mb // per_worker_loaded),
            'tasks': args.tasks, 'tasks_completed': done_file.stat().st_size if done_file.exists() else 0,
            'tasks_per_s': round(args.tasks / elapsed, 1),
        }
        out = HERE / 'results' / f'fleet-{label}.json'
        out.write_text(json.dumps(result, indent=1))
        print(json.dumps({k: v for k, v in result.items() if k != 'series'}, indent=1))
    finally:
        for w in workers:
            try:
                os.killpg(w.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        for w in workers:
            try:
                w.wait(30)
            except subprocess.TimeoutExpired:
                os.killpg(w.pid, signal.SIGKILL)
        done_file.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
