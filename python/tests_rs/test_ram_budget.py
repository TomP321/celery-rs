"""Child descriptors and free-page reclamation at startup."""
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def test_prefork_private_startup_pages(tmp_path):
    label = tmp_path / 'private-pages'
    result = subprocess.run([
        str(ROOT / '.venv-rs/bin/python'), str(ROOT / 'bench/bench.py'),
        '--venv', str(ROOT / '.venv-rs'), '--pool', 'prefork', '--concurrency', '10',
        '--task', 'pure', '--prefill', '--n', '10000', '--latency-n', '20',
        '--label', str(label)], cwd=ROOT, capture_output=True, text=True, timeout=180)
    assert result.returncode == 0, result.stdout + result.stderr
    data = json.loads(label.with_suffix('.json').read_text())
    assert data['completed_tasks'] == 10000
    children = [p for p in data['idle_processes'] if p['role'] == 'child']
    assert len(children) == 10
    assert max(p['private_dirty_kb'] for p in children) <= 3.5 * 1024, data
    master = next(p for p in data['idle_processes'] if p['role'] == 'master')
    assert master['private_dirty_kb'] <= 12 * 1024, data
    assert max(p['private_dirty_kb'] for p in data['after_processes']
               if p['role'] == 'child') <= 5.4 * 1024, data


def test_zygote_closes_runtime_fds_once_and_children_preserve_user_fds(tmp_path):
    code = r'''
import fcntl, json, os, time
from celery import Celery
from celery.concurrency import _rs_zygote
from celery.concurrency.rs_prefork import RsTaskPool
app = Celery('fds', broker='memory://', backend='cache+memory://')
app.finalize()
path = os.environ['CLOSE_CALLS']
original = _rs_zygote._close_inherited_fds
def close(keep, runtime_fds=None):
    with open(path, 'a') as output:
        output.write(str(os.getpid()) + '\n')
    return original(keep, runtime_fds)
_rs_zygote._close_inherited_fds = close
with open('/dev/null') as stream:
    master_fd = fcntl.fcntl(stream.fileno(), fcntl.F_DUPFD_CLOEXEC, 512)
def inspect_fds():
    os.fstat(master_fd)
    return os.getpid(), sorted(os.listdir('/proc/self/fd'))
pool = RsTaskPool(10, app=app, initargs=(app, 'fds@test'))
pool.start()
try:
    jobs = [pool.apply_async(inspect_fds) for _ in range(100)]
    deadline = time.monotonic() + 30
    while not all(job.ready() for job in jobs):
        assert time.monotonic() < deadline
        pool.wait(.01)
    results = [job.get() for job in jobs]
    assert len({pid for pid, _ in results}) == 10
    assert open(path).read().splitlines() == [str(pool._zygote_pid)]
    print(json.dumps(results))
finally:
    pool.stop()
    os.close(master_fd)
'''
    result = subprocess.run(
        [str(ROOT / '.venv-rs/bin/python'), '-c', code], cwd='/tmp',
        env=dict(os.environ, CLOSE_CALLS=str(tmp_path / 'calls')),
        capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
    assert len(json.loads(result.stdout.splitlines()[-1])) == 100


def test_worker_reclaims_startup_pages_without_changing_task_state(tmp_path):
    # Probe the real loop, including reconnects; the override records trims and delegates to the native function.
    module = tmp_path / 'trim_app.py'
    module.write_text('''
import gc, json, os, signal
import _celery_rs
from celery import Celery, signals
app = Celery('trim', broker=os.environ['CELERY_RS_REDIS_URL'] + '/0')
app.conf.worker_hijack_root_logger = False
original = _celery_rs._release_task_memory
before = None
def release(force=False):
    if force:
        with open(os.environ['TRIM_REPORT'], 'w') as output:
            json.dump({'pid': os.getpid(), 'gc': [gc.isenabled(), gc.get_threshold()],
                       'unchanged': before == (gc.isenabled(), gc.get_threshold(),
                                               signal.getsignal(signal.SIGUSR1))}, output)
    return original(force=force)
@signals.worker_ready.connect(weak=False)
def ready(**kwargs):
    global before
    before = (gc.isenabled(), gc.get_threshold(), signal.getsignal(signal.SIGUSR1))
    _celery_rs._release_task_memory = release
''')
    report = tmp_path / 'trim.json'
    import signal
    import time
    env = dict(os.environ, PYTHONPATH=str(tmp_path), TRIM_REPORT=str(report))
    with (tmp_path / 'worker.log').open('w+') as log:
        worker = subprocess.Popen([
            str(ROOT / '.venv-rs/bin/python'), '-m', 'celery', '-A', 'trim_app',
            'worker', '-P', 'prefork', '-c', '10', '-l', 'warning',
            '-Q', f'ram-budget-{os.getpid()}', '--without-gossip', '--without-mingle'],
            cwd=tmp_path, env=env, stdout=log, stderr=log, start_new_session=True)
        try:
            deadline = time.monotonic() + 40
            while not report.exists():
                if worker.poll() is not None or time.monotonic() > deadline:
                    log.seek(0)
                    raise AssertionError(log.read())
                time.sleep(.05)
            data = json.loads(report.read_text())
            assert data['pid'] == worker.pid
            assert data['unchanged']
        finally:
            os.killpg(worker.pid, signal.SIGTERM)
            try:
                worker.wait(20)
            except subprocess.TimeoutExpired:
                os.killpg(worker.pid, signal.SIGKILL)
                worker.wait()
