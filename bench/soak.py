#!/usr/bin/env python3
"""Compare c10 workers and beat across four lanes with --compare --minutes 60.

Write results even on failure or interruption."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'bench/results'


def descendants(root):
    found = []
    todo = [(root, None)]
    while todo:
        pid, parent = todo.pop()
        try:
            proc = Path(f'/proc/{pid}')
            # The comm field can contain spaces and parentheses.
            stat = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
            if stat[0] == 'Z':
                continue
            memory = {parts[0].rstrip(':'): int(parts[1]) for line in
                      (proc / 'smaps_rollup').read_text().splitlines()
                      if len(parts := line.split()) == 3 and parts[2] == 'kB'}
            pss = memory['Pss']
            status = (proc / 'status').read_text().splitlines()
            threads = next(int(line.split()[1]) for line in status if line.startswith('Threads:'))
            children = (proc / 'task' / str(pid) / 'children').read_text().split()
            found.append({'pid': pid, 'parent': parent, 'start': stat[19], 'pss_kib': pss,
                          'fds': len(list((proc / 'fd').iterdir())), 'threads': threads,
                          'children': [int(p) for p in children], 'memory_kib': memory})
            todo.extend((int(child), pid) for child in children)
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    return found


def analyze(samples, warmup=300):
    series = {}
    for sample in samples:
        if sample['seconds'] < warmup:
            continue
        for proc in sample['processes']:
            key = f"{proc['role']}:{proc['pid']}:{proc['start']}"
            series.setdefault(key, []).append((sample['seconds'], proc))
    rows = []
    for key, points in series.items():
        times = [t / 3600 for t, _ in points]
        values = [p['pss_kib'] / 1024 for _, p in points]
        mean_t = statistics.mean(times)
        denominator = sum((t - mean_t) ** 2 for t in times)
        slope = (sum((t - mean_t) * (v - statistics.mean(values))
                     for t, v in zip(times, values)) / denominator) if denominator else None
        fd_values = [p['fds'] for _, p in points]
        thread_values = [p['threads'] for _, p in points]
        growth = max(values) - values[0]
        enough = len(points) >= 3
        passed = (enough and slope is not None and slope < 1 and
                  ('child' not in key or growth <= 3) and
                  min(fd_values) == max(fd_values) and min(thread_values) == max(thread_values))
        rows.append({'process': key, 'samples': len(points), 'baseline_mib': values[0],
                     'max_growth_mib': growth, 'slope_mib_hour': slope,
                     'fds': [min(fd_values), max(fd_values)],
                     'threads': [min(thread_values), max(thread_values)], 'passed': passed})
    return rows


def save(path, report):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(report, indent=2))
    temporary.replace(path)


def restart_broker(kind):
    lane = int(os.environ['CELERY_RS_LANE'])
    project = 'celery-rs' if lane == 0 else f'celery-rs-lane-{lane}'
    subprocess.run(['docker', 'compose', '-f', str(ROOT / 'infra/docker-compose.yml'),
                    '-p', project, 'restart', 'redis' if kind == 'redis' else 'rabbitmq'],
                   check=True, timeout=120)


def load():
    from celery import chord, group
    from soak_app import app, add, fail, large, pure, retry, total
    cycles = 0
    while True:
        try:
            results = []
            for _ in range(10):
                pure.delay(cycles)
            results.extend([fail.delay(), retry.delay(), add.apply_async((1, 2), countdown=2)])
            revoked = add.apply_async((1, 2), countdown=5)
            app.control.revoke(revoked.id)
            results.append(revoked)
            group_result = group(add.s(i, 1) for i in range(4)).apply_async()
            results.extend(group_result.results)
            results.append(chord([add.s(i, 1) for i in range(4)])(total.s()))
            results.append(large.delay())
            # Wait and release results to bound the backlog and client cache.
            for result in results:
                try:
                    result.get(timeout=20, propagate=False)
                finally:
                    result.forget()
            group_result.forget()
            if cycles % 10 == 0:
                print(json.dumps({'cycle': cycles, 'stats': app.control.inspect(timeout=5).stats(),
                                  'ping': app.control.ping(timeout=5)}), flush=True)
            cycles += 1
            print(json.dumps({'completed_cycle': cycles, 'monotonic': time.monotonic()}), flush=True)
            time.sleep(1)
        except Exception as exc:
            print(json.dumps({'cycle': cycles, 'error': repr(exc)}), flush=True)
            time.sleep(2)


def interpreter(impl):
    return str(ROOT / f'.venv-{impl}/bin/python')


def run(args):
    if os.environ.get('CELERY_RS_BROKER_LOCKED') != '1':
        return subprocess.call([str(ROOT / 'tools/dev/with-brokers'), interpreter(args.impl),
                                __file__, *sys.argv[1:]])
    token = f'{args.impl}-{args.broker}-{uuid.uuid4().hex[:10]}'
    redis = os.environ['CELERY_RS_REDIS_URL']
    env = dict(os.environ, SOAK_QUEUE=f'soak-{token}', SOAK_BACKEND=redis + '/13',
               SOAK_BROKER=redis + '/12' if args.broker == 'redis' else os.environ['CELERY_RS_AMQP_URL'])
    OUT.mkdir(exist_ok=True)
    path = OUT / f'soak-{time.strftime("%Y%m%d-%H%M%S")}-{token}.json'
    report = {'implementation': args.impl, 'broker': args.broker, 'minutes': args.minutes,
              'warmup_seconds': 300, 'sample_seconds': args.sample_seconds,
              'lane': os.environ['CELERY_RS_LANE'], 'samples': [], 'events': [], 'complete': False}
    processes = []
    logs = []
    start = time.monotonic()
    try:
        with tempfile.TemporaryDirectory(prefix='celery-soak-') as work:
            commands = [
                ['-m', 'celery', '-A', 'soak_app:app', 'worker', '-P', 'prefork', '-c', '10',
                 '-n', f'soak-{token}@%h', '-l', 'ERROR'],
                ['-m', 'celery', '-A', 'soak_app:app', 'beat', '-l', 'ERROR',
                 '--schedule', str(Path(work) / 'schedule'), '--pidfile', str(Path(work) / 'beat.pid')],
                [__file__, '--load'],
            ]
            for label, command in zip(('worker', 'beat', 'load'), commands):
                logfile = path.with_suffix(f'.{label}.log').open('w')
                logs.append(logfile)
                processes.append(subprocess.Popen([interpreter(args.impl), *command], cwd=ROOT / 'bench',
                                                   env=env, stdout=logfile, stderr=subprocess.STDOUT,
                                                   start_new_session=True))
            next_sample = start
            restarted = False
            while time.monotonic() - start <= args.minutes * 60:
                now = time.monotonic()
                for label, process in zip(('worker', 'beat', 'load'), processes):
                    if process.poll() is not None:
                        raise RuntimeError(f'{label} exited: {process.returncode}')
                if now >= next_sample:
                    sample = {'seconds': now - start, 'processes': []}
                    for label, root in zip(('worker', 'beat'), processes):
                        for proc in descendants(root.pid):
                            proc['role'] = label if proc['pid'] == root.pid else (
                                'zygote' if label == 'worker' and proc['children'] else label + '-child')
                            sample['processes'].append(proc)
                    report['samples'].append(sample)
                    save(path, report)
                    next_sample += args.sample_seconds
                if not restarted and now - start >= args.minutes * 30:
                    report['events'].append({'seconds': now - start, 'event': 'restart broker'})
                    restart_broker(args.broker)
                    restarted = True
                time.sleep(min(1, max(0.01, next_sample - time.monotonic())))
            report['complete'] = True
    except BaseException as exc:
        report['error'] = repr(exc)
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
        for process in reversed(processes):
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        for log in logs:
            log.close()
        completed = []
        load_log = path.with_suffix('.load.log')
        if load_log.exists():
            for line in load_log.read_text(errors='replace').splitlines():
                try:
                    event = json.loads(line)
                except (ValueError, TypeError):
                    continue
                if 'completed_cycle' in event:
                    completed.append(event)
        report['completed_cycles'] = len(completed)
        report['load_recovered_after_restart'] = any(
            event['monotonic'] - start > args.minutes * 30 for event in completed)
        report['topology_valid'] = all(
            sum(p['role'] == 'worker-child' for p in sample['processes']) == 10 and
            sum(p['role'] == 'beat' for p in sample['processes']) == 1 and
            sum(p['role'] == 'worker' for p in sample['processes']) == 1
            for sample in report['samples'] if sample['seconds'] >= 300)
        report['analysis'] = analyze(report['samples'])
        report['passed'] = (report['complete'] and report['topology_valid'] and
                            report['load_recovered_after_restart'] and bool(report['analysis']) and
                            all(row['passed'] for row in report['analysis']))
        save(path, report)
    print(path, flush=True)
    return 0 if report['passed'] else 1


def compare(args):
    OUT.mkdir(exist_ok=True)
    jobs = []
    for impl in ('ref', 'rs'):
        for broker in ('redis', 'amqp'):
            log = (OUT / f'soak-{impl}-{broker}-runner.log').open('w')
            command = [str(ROOT / 'tools/dev/with-brokers'),
                       interpreter(impl), __file__, '--impl', impl,
                       '--broker', broker, '--minutes', str(args.minutes),
                       '--sample-seconds', str(args.sample_seconds)]
            jobs.append((subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT), log))
    codes = [job.wait() for job, _ in jobs]
    for _, log in jobs:
        log.close()
    return int(any(codes))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--minutes', type=float, default=60)
    parser.add_argument('--sample-seconds', type=float, default=30)
    parser.add_argument('--impl', choices=['ref', 'rs'], default='rs')
    parser.add_argument('--broker', choices=['redis', 'amqp'], default='redis')
    parser.add_argument('--compare', action='store_true')
    parser.add_argument('--load', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.minutes <= 0 or args.sample_seconds <= 0:
        parser.error('duration and interval must be positive')
    if args.load:
        load()
    else:
        sys.exit(compare(args) if args.compare else run(args))
