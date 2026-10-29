"""Break down master PSS, traced Python heap and libc allocations.

Run under tools/dev/with-brokers. Tracemalloc adds memory overhead;
its module attribution cannot be compared with unprofiled PSS."""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

APP = r'''
import os
from celery import Celery, signals
app = Celery('memory_probe', broker=os.environ['BENCH_BROKER'],
             backend=os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/1')
app.conf.worker_hijack_root_logger = False

@signals.worker_ready.connect(weak=False)
def report(**kwargs):
    import json, re, sys
    modules = sorted(sys.modules)
    groups = {}
    for mapping in re.split(r'(?=^[0-9a-f]+-[0-9a-f]+ )',
                            open('/proc/self/smaps').read(), flags=re.M):
        match = re.search(r'^Pss:\s+(\d+)', mapping, re.M)
        if not match:
            continue
        header = mapping.splitlines()[0].split(maxsplit=5)
        name = header[5] if len(header) == 6 else '[anonymous]'
        groups[name] = groups.get(name, 0) + int(match[1])
    import ctypes, tracemalloc
    class Mallinfo(ctypes.Structure):
        _fields_ = [(name, ctypes.c_size_t) for name in (
            'arena', 'ordblks', 'smblks', 'hblks', 'hblkhd', 'usmblks',
            'fsmblks', 'uordblks', 'fordblks', 'keepcost')]
    libc = ctypes.CDLL(None)
    libc.mallinfo2.restype = Mallinfo
    info = libc.mallinfo2()
    payload = {
        'pid': os.getpid(), 'pss_kb': sum(groups.values()),
        'threads': len(os.listdir('/proc/self/task')),
        'mappings_pss_kb': dict(sorted(groups.items(), key=lambda row: -row[1])),
        'modules': modules,
        'libc_bytes': {name: getattr(info, name) for name, _ in info._fields_},
        'python_traced_bytes': {},
    }
    import _celery_rs
    if hasattr(_celery_rs, '_allocator_stats'):
        live, peak = _celery_rs._allocator_stats()
        payload['rust_heap_bytes'] = {'live': live, 'peak': peak}
    if tracemalloc.is_tracing():
        payload['tracer_bytes'] = tracemalloc.get_tracemalloc_memory()
        for stat in tracemalloc.take_snapshot().statistics('filename'):
            payload['python_traced_bytes'][stat.traceback[0].filename] = stat.size
    destination = os.environ['MEMORY_REPORT']
    with open(destination + '.tmp', 'w') as output:
        json.dump(payload, output, indent=2)
    os.replace(destination + '.tmp', destination)
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--venv', default='.venv-rs')
    parser.add_argument('--label', default='08b-master')
    parser.add_argument('--native', help='directory containing the diagnostic _celery_rs build')
    args = parser.parse_args()
    data = {}
    with tempfile.TemporaryDirectory(prefix='celery-memory-') as directory:
        Path(directory, 'memory_app.py').write_text(APP)
        for pool in ('solo', 'prefork'):
            for traced in (False, True):
                key = f'{pool}-' + ('traced' if traced else 'physical')
                report = Path(directory, f'{key}.json')
                env = {k: v for k, v in os.environ.items()
                       if not k.startswith('PYTHON')}
                pythonpath = directory
                if args.native:
                    pythonpath = os.pathsep.join((str(Path(args.native).resolve()), directory))
                env.update(PYTHONPATH=pythonpath, MEMORY_REPORT=str(report),
                           BENCH_BROKER=os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/0')
                command = [str(ROOT / args.venv / 'bin/python')]
                if traced:
                    command += ['-X', 'tracemalloc']
                command += ['-m', 'celery', '-A', 'memory_app', 'worker',
                            '-P', pool, '-c', '4', '-l', 'warning', '--without-gossip']
                with tempfile.TemporaryFile(mode='w+') as log:
                    process = subprocess.Popen(command, cwd=directory, env=env,
                                               stdout=log, stderr=log,
                                               start_new_session=True)
                    try:
                        deadline = time.monotonic() + 90
                        while not report.exists():
                            if process.poll() is not None or time.monotonic() > deadline:
                                log.seek(0)
                                raise RuntimeError(log.read()[-4000:])
                            time.sleep(0.1)
                        data[key] = json.loads(report.read_text())
                        print(key, data[key]['pss_kb'], flush=True)
                    finally:
                        os.killpg(process.pid, signal.SIGTERM)
                        try:
                            process.wait(30)
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid, signal.SIGKILL)
                            process.wait()
    destination = ROOT / 'bench/results' / f'{args.label}.json'
    destination.write_text(json.dumps(data, indent=2))
    print(destination)


if __name__ == '__main__':
    main()
