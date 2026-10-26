"""Misure riproducibili della spec 08f, sotto tools/dev/with-brokers.

Include tutti i processi discendenti nel costo di worker -B. Il profilo
tracemalloc gira separatamente per non contaminare la misura della PSS.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import uuid

from bench_beat import BEAT_APP, BROKERS, RESULTS, child, TICK, SENDS, venv_python

PROBE = '''
app.conf.update(task_default_queue=os.environ['BENCH_QUEUE'],
                task_default_exchange=os.environ['BENCH_QUEUE'],
                task_default_routing_key=os.environ['BENCH_QUEUE'])
from celery import signals
@signals.beat_init.connect(weak=False)
def profile(sender, **kwargs):
    import json, sys
    from pathlib import Path
    data = {'modules': sorted(sys.modules)}
    if os.environ.get('BENCH_TRACE'):
        import tracemalloc
        snapshot = tracemalloc.take_snapshot()
        data['python_heap_bytes'] = sum(s.size for s in snapshot.statistics('filename'))
        data['allocations'] = [(str(s.traceback), s.size) for s in snapshot.statistics('filename')[:60]]
    Path(os.environ['BENCH_PROFILE']).write_text(json.dumps(data))
'''


def smaps(pid):
    groups = {'python': 0, 'rust': 0, 'libraries': 0, 'anonymous': 0}
    try:
        for line in Path(f'/proc/{pid}/smaps').read_text().splitlines():
            if '-' in line.split()[0]:
                path = line.split(maxsplit=5)[-1] if len(line.split()) >= 6 else ''
                group = ('rust' if '_celery_rs' in path else
                         'python' if 'libpython' in path or '/bin/python3.' in path else
                         'libraries' if path.startswith('/') else 'anonymous')
            elif line.startswith('Pss:'):
                groups[group] += int(line.split()[1])
    except (FileNotFoundError, ProcessLookupError):
        pass
    return groups


def tree(pid):
    result = [pid]
    for parent in result:
        try:
            result.extend(map(int, Path(f'/proc/{parent}/task/{parent}/children').read_text().split()))
        except FileNotFoundError:
            pass
    return result



def cleanup_queue(env):
    code = """
import json, os
from kombu import Connection
with Connection(os.environ['BENCH_BROKER']) as connection:
    connection.channel().queue_delete(os.environ['BENCH_QUEUE'])
print(json.dumps(True))
"""
    child(str(Path(__file__).resolve().parents[1] / '.venv-ref'), code,
          {k: v for k, v in env.items() if not k.startswith('PYTHON')})

def measure_process(python, directory, broker, count, pool=None, embedded=False,
                    trace=False, source=None):
    token = uuid.uuid4().hex
    work = directory / token
    work.mkdir()
    (work / 'bench_beat_app.py').write_text(BEAT_APP + PROBE)
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(('PYTHON', 'CELERY_'))}
    env.update(BENCH_BROKER=broker, BENCH_ENTRIES=str(count),
               BENCH_PROFILE=str(work / 'profile.json'), BENCH_QUEUE='08f-' + token,
               PYTHONPATH=os.pathsep.join(filter(None, [source, str(work)])))
    if trace:
        env.update(PYTHONTRACEMALLOC='1', BENCH_TRACE='1')
    command = [python, '-m', 'celery', '-A', 'bench_beat_app']
    schedule = str(work / 'schedule')
    if pool:
        command += ['worker', '-P', pool, '-c', '10' if pool == 'prefork' else '1',
                    '-Q', '08f-' + token, '-n', '08f-' + token + '@%h', '-l', 'info']
        if embedded:
            command += ['-B', '-s', schedule]
    else:
        command += ['beat', '-s', schedule, '-l', 'info']
    log_path = work / 'process.log'
    with log_path.open('w') as log:
        process = subprocess.Popen(command, env=env, cwd=work, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
    try:
        marker = ' ready.' if pool else 'beat: Starting...'
        deadline = time.monotonic() + 60
        while marker not in log_path.read_text():
            if process.poll() is not None or time.monotonic() >= deadline:
                raise RuntimeError(log_path.read_text()[-5000:])
            time.sleep(.05)
        time.sleep(3)
        memory = {str(pid): smaps(pid) for pid in tree(process.pid)}
        result = {'pss_kb': sum(sum(v.values()) for v in memory.values()),
                  'processes': memory}
        if (work / 'profile.json').exists():
            result['profile'] = json.loads((work / 'profile.json').read_text())
        return result
    finally:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(30)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        # Il worker può lasciare discendenti dopo un errore all'avvio.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        cleanup_queue(env)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--venv', required=True)
    parser.add_argument('--label', required=True)
    parser.add_argument('--source', help='guscio Python alternativo per la baseline')
    parser.add_argument('--workers', action='store_true')
    parser.add_argument('--trace', action='store_true')
    args = parser.parse_args()
    out = {}
    with tempfile.TemporaryDirectory(prefix='08f-') as directory:
        for name, broker in BROKERS.items():
            row = {}
            for count in (3, 1000, 10000):
                row[str(count)] = measure_process(
                    venv_python(args.venv), Path(directory), broker, count,
                    source=args.source)
            if args.trace:
                row['trace'] = measure_process(
                    venv_python(args.venv), Path(directory), broker, 3,
                    trace=True, source=args.source)
            if args.workers:
                for pool in ('solo', 'prefork'):
                    for embedded in (False, True):
                        row[f'{pool}-{embedded}'] = measure_process(
                            venv_python(args.venv), Path(directory), broker, 3,
                            pool=pool, embedded=embedded, source=args.source)
            env = dict(os.environ, BENCH_BROKER=broker)
            if args.source:
                env['PYTHONPATH'] = args.source
            row['tick'] = child(args.venv, TICK, env)
            queue = '08f-sends-' + uuid.uuid4().hex
            env['BENCH_QUEUE'] = queue
            sends = SENDS.replace('now = app.now()',
                "app.conf.update(task_default_queue=os.environ['BENCH_QUEUE'], "
                "task_default_exchange=os.environ['BENCH_QUEUE'], "
                "task_default_routing_key=os.environ['BENCH_QUEUE'])\nnow = app.now()")
            row['sends'] = child(args.venv, sends, env)
            cleanup_queue(env)
            out[name] = row
            print(name, {k: v['pss_kb'] for k, v in row.items() if 'pss_kb' in v}, flush=True)
    RESULTS.mkdir(exist_ok=True)
    path = RESULTS / f'08f-{args.label}.json'
    path.write_text(json.dumps(out, indent=2))
    print(path)


if __name__ == '__main__':
    main()
