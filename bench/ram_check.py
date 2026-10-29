"""Compare fleet PSS with MemAvailable loss and page tables.

Run under tools/dev/with-brokers with --venv and --workers. PSS excludes
per-process kernel memory: also sum VmPTE and measure system MemAvailable
before and after starting idle prefork c10 workers."""
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


def mem_available_kb():
    with open('/proc/meminfo') as fh:
        for line in fh:
            if line.startswith('MemAvailable:'):
                return int(line.split()[1])
    return 0


def status_kb(pid, field):
    try:
        with open(f'/proc/{pid}/status') as fh:
            for line in fh:
                if line.startswith(field + ':'):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


def settle_available(seconds=6):
    """Average MemAvailable over several seconds to reduce background process noise."""
    samples = []
    for _ in range(seconds * 2):
        samples.append(mem_available_kb())
        time.sleep(0.5)
    return sum(samples) / len(samples)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--venv', required=True)
    ap.add_argument('--workers', type=int, default=10)
    ap.add_argument('--concurrency', type=int, default=10)
    args = ap.parse_args()
    label = Path(args.venv).name.lstrip('.').removeprefix('venv-')
    env = {**{k: v for k, v in os.environ.items() if not k.startswith('PYTHON')},
           'PYTHONPATH': str(HERE), 'BENCH_BROKER': REDIS + '/0', 'BENCH_BACKEND': REDIS + '/1',
           'BENCH_QUEUE': f'ramcheck-{label}'}
    python = str(Path(args.venv).resolve() / 'bin' / 'python')
    subprocess.run(['sync'])
    before = settle_available()
    workers = []
    try:
        for n in range(args.workers):
            workers.append(subprocess.Popen(
                [python, '-m', 'celery', '-A', 'benchapp', 'worker', '-P', 'prefork',
                 '-c', str(args.concurrency), '-l', 'warning', '-n', f'ramcheck{n}@%h',
                 '-Q', env['BENCH_QUEUE'], '--without-gossip', '--without-mingle'],
                cwd=HERE, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True))
        deadline = time.time() + 120
        while time.time() < deadline and any(len(tree(w.pid)) < args.concurrency + 1 for w in workers):
            time.sleep(0.5)
        time.sleep(3)
        after = settle_available()
        pids = [p for w in workers for p in tree(w.pid)]
        pss = sum(pss_kb(w.pid)[0] for w in workers)
        pte = sum(status_kb(p, 'VmPTE') for p in pids)
        result = {
            'label': label, 'workers': args.workers, 'concurrency': args.concurrency,
            'processes': len(pids),
            'pss_mb_per_worker': round(pss / 1024 / args.workers, 1),
            'page_tables_mb_per_worker': round(pte / 1024 / args.workers, 2),
            'memavailable_drop_mb_per_worker': round((before - after) / 1024 / args.workers, 1),
        }
        print(json.dumps(result, indent=1))
        (HERE / 'results' / f'ramcheck-{label}.json').write_text(json.dumps(result, indent=1))
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


if __name__ == '__main__':
    main()
