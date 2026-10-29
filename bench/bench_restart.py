"""Measure prefork c10 PSS across two restarts under with-brokers (under five minutes)."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import uuid

from soak import ROOT, descendants, restart_broker

APP = '''
import json, os, sys, time
from celery import Celery, signals
app = Celery('restart', broker=os.environ['PROBE_BROKER'],
             backend=os.environ['CELERY_RS_REDIS_URL'] + '/14')
app.conf.update(task_default_queue=os.environ['PROBE_QUEUE'],
                worker_hijack_root_logger=False)
def record(sender=None, **kwargs):
    path = os.environ['PROBE_MODULES']
    with open(path + '.tmp', 'w') as stream:
        json.dump(sorted(sys.modules), stream)
    os.replace(path + '.tmp', path)
@signals.worker_ready.connect(weak=False)
def ready(sender=None, **kwargs):
    record()
    sender.timer.call_repeatedly(1, record)
@app.task
def probe():
    time.sleep(.05)
    return os.getpid()
'''


def measure(impl, broker, output, source=None):
    if os.environ.get('CELERY_RS_BROKER_LOCKED') != '1':
        raise RuntimeError('run under tools/dev/with-brokers')
    import redis
    client = redis.Redis.from_url(os.environ['CELERY_RS_REDIS_URL'] + '/14')
    url = (os.environ['CELERY_RS_REDIS_URL'] + '/15' if broker == 'redis'
           else os.environ['CELERY_RS_AMQP_URL'])
    output.parent.mkdir(parents=True, exist_ok=True)
    report = {'impl': impl, 'broker': broker, 'source': source, 'samples': []}
    with tempfile.TemporaryDirectory() as directory:
        Path(directory, 'restart_app.py').write_text(APP)
        modules = Path(directory, 'modules.json')
        env = dict(os.environ, PYTHONPATH=os.pathsep.join(filter(None, (source, directory))),
                   PROBE_BROKER=url,
                   PROBE_QUEUE='restart-' + uuid.uuid4().hex,
                   PROBE_MODULES=str(modules))
        with output.with_suffix('.log').open('w') as log:
            worker = subprocess.Popen([
                str(ROOT / f'.venv-{impl}/bin/python'), '-m', 'celery',
                '-A', 'restart_app', 'worker', '-P', 'prefork', '-c', '10',
                '-l', 'warning', '--without-gossip'], cwd=directory,
                env=env, stdout=log, stderr=log, start_new_session=True)
            try:
                deadline = time.monotonic() + 60
                while not modules.exists():
                    if worker.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError('worker not ready: ' + str(output))
                    time.sleep(.1)
                # The client interpreter differs from the reference worker interpreter.
                from celery import Celery
                app = Celery('probe-client', broker=url,
                             backend=os.environ['CELERY_RS_REDIS_URL'] + '/14')
                app.conf.task_default_queue = env['PROBE_QUEUE']
                for iteration in range(3):
                    if iteration:
                        restart_broker(broker)
                    deadline = time.monotonic() + 60
                    seen = set()
                    while len(seen) < 10:
                        if time.monotonic() > deadline:
                            raise RuntimeError('not all children ran the probe')
                        try:
                            results = [app.send_task('restart_app.probe')
                                       for _ in range(10)]
                            for result in results:
                                seen.add(result.get(timeout=5))
                                client.delete('celery-task-meta-' + result.id)
                        except Exception:
                            if time.monotonic() > deadline:
                                raise
                            time.sleep(1)
                    time.sleep(3)
                    processes = descendants(worker.pid)
                    for proc in processes:
                        proc['role'] = ('master' if proc['pid'] == worker.pid
                                        else 'zygote' if proc['parent'] == worker.pid
                                        and impl == 'rs' else 'child')
                    report['samples'].append({
                        'iteration': iteration, 'processes': processes,
                        'total_kib': sum(p['pss_kib'] for p in processes),
                        'modules': json.loads(modules.read_text())})
            finally:
                os.killpg(worker.pid, signal.SIGTERM)
                try:
                    worker.wait(20)
                except subprocess.TimeoutExpired:
                    os.killpg(worker.pid, signal.SIGKILL)
                    worker.wait()
    from kombu import Connection
    with Connection(url) as connection:
        connection.default_channel.queue_delete(env['PROBE_QUEUE'])
    logs = output.with_suffix('.log').read_text()
    report['task_errors'] = (logs.count('raised unexpected:')
                             + logs.count('Exception raised outside body:'))
    output.write_text(json.dumps(report, indent=2))
    print(json.dumps({'output': str(output), 'totals_kib': [
        sample['total_kib'] for sample in report['samples']]}))
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--impl', choices=('ref', 'rs'), default='rs')
    parser.add_argument('--broker', choices=('redis', 'amqp'), default='redis')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--source', type=Path,
                        help='alternate source for baseline comparisons')
    args = parser.parse_args()
    measure(args.impl, args.broker, args.output.resolve(),
            str(args.source.resolve()) if args.source else None)
