"""Python descriptor inheritance and zygote stability."""
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid

import pytest

ROOT = Path(__file__).resolve().parents[2]


def children(pid):
    result = []
    for task in Path(f'/proc/{pid}/task').iterdir():
        result.extend(map(int, (task / 'children').read_text().split()))
    return result


def wait_for(predicate, process, log, timeout=40):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        assert process.poll() is None, log.read_text()
        time.sleep(.05)
    raise AssertionError(log.read_text())


def test_imported_file_and_loopback_socket_match_reference_after_respawn(tmp_path):
    from celery import Celery

    broker = os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/0'
    (tmp_path / 'data.txt').write_text('hello inherited file')
    (tmp_path / 'inherited_app.py').write_text('''
import os, socket, time
from celery import Celery, signals
DATA = open(os.path.join(os.path.dirname(__file__), 'data.txt'), 'rb')
LISTENER = socket.socket()
LISTENER.bind(('127.0.0.1', 0))
LISTENER.listen(1)
CLIENT = socket.create_connection(LISTENER.getsockname(), timeout=5)
PEER, _ = LISTENER.accept()
PEER.settimeout(5)
app = Celery('inherited', broker=os.environ['FD_BROKER'], backend=os.environ['FD_BROKER'])
app.conf.update(result_expires=60, worker_prefetch_multiplier=1)
@signals.worker_ready.connect(weak=False)
def ready(**kwargs):
    with open(os.environ['FD_READY'], 'w') as stream:
        stream.write('ready')
@app.task(name='inherited.use')
def use():
    CLIENT.sendall(b'ping')
    assert PEER.recv(4) == b'ping'
    PEER.sendall(b'pong')
    return [os.pread(DATA.fileno(), 5, 0).decode(), CLIENT.recv(4).decode(), os.getpid()]
''')
    outcomes = []
    for implementation in ('ref', 'rs'):
        queue = 'zygote-fds-' + uuid.uuid4().hex
        ready = tmp_path / f'{implementation}.ready'
        log = tmp_path / f'{implementation}.log'
        env = dict(os.environ, PYTHONPATH=str(tmp_path), FD_BROKER=broker,
                   FD_READY=str(ready))
        with log.open('w') as stream:
            process = subprocess.Popen([
                str(ROOT / f'.venv-{implementation}/bin/python'), '-m', 'celery',
                '-A', 'inherited_app', 'worker', '-P', 'prefork', '-c', '2',
                '-Q', queue, '-n', queue + '@localhost', '-l', 'warning',
                '--max-tasks-per-child', '1', '--without-gossip', '--without-mingle'],
                cwd=tmp_path, env=env, stdout=stream, stderr=stream,
                start_new_session=True)
        client = Celery('fd-client', broker=broker, backend=broker)
        results = []
        try:
            wait_for(ready.exists, process, log)
            zygote = None
            if implementation == 'rs':
                direct = children(process.pid)
                assert len(direct) == 1, direct
                zygote = direct[0]
                wait_for(lambda: len(children(zygote)) == 2, process, log)
                initial_fds = set(os.listdir(f'/proc/{zygote}/fd'))
            pids = set()
            for _ in range(12):
                result = client.send_task('inherited.use', queue=queue)
                results.append(result)
                value = result.get(timeout=30)
                pids.add(value.pop())
                assert value == ['hello', 'pong']
                outcomes.append(value)
            assert len(pids) == 12
            if zygote is not None:
                wait_for(lambda: len(children(zygote)) == 2
                         and set(os.listdir(f'/proc/{zygote}/fd')) == initial_fds,
                         process, log)
                assert set(os.listdir(f'/proc/{zygote}/fd')) == initial_fds
        finally:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            for result in results:
                result.forget()
            with client.connection_for_write() as connection:
                connection.default_channel.queue_delete(queue)
            client.close()
        assert process.returncode == 0, log.read_text()
    assert outcomes[:12] == outcomes[12:]


@pytest.mark.parametrize('broker', ['redis', 'amqp'])
def test_only_native_runtime_descriptors_are_closed(broker):
    code = r'''
import json, os, select, socket, sys, time
from celery import Celery
from celery.concurrency.rs_prefork import RsTaskPool
from celery.concurrency._rs_zygote import _snapshot_runtime_fds
import _celery_rs
url = (os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/0'
       if sys.argv[1] == 'redis' else
       os.environ.get('CELERY_RS_AMQP_URL', 'amqp://guest:guest@127.0.0.1:5673//'))
# Create Python resources both before and after the Rust runtime.
stream = open('/dev/null')
left, right = socket.socketpair()
poller = select.epoll()
poller.register(left.fileno(), select.EPOLLIN)
app = Celery('native-fds', broker=url,
             backend=os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/1')
app.finalize()
connection = _celery_rs.Connection(url)
connection.declare_queue('zygote-native-' + str(os.getpid()), auto_delete=True)
app.backend.client.get('zygote-native-missing-' + str(os.getpid()))
late_stream = open('/dev/null')
pool = RsTaskPool(2, app=app, initargs=(app, 'fds@test'), maxtasksperchild=1)
owned = _snapshot_runtime_fds(_celery_rs._runtime_fds())
assert len(owned) >= 5, owned
python_fds = [stream.fileno(), late_stream.fileno(), left.fileno(), right.fileno(), poller.fileno()]
assert not set(python_fds).intersection(owned)
def inspect_fds():
    for fd, identity in owned.items():
        try:
            stat = os.fstat(fd)
        except OSError:
            continue
        assert (stat.st_dev, stat.st_ino) != identity, (fd, identity)
    for fd in python_fds:
        os.fstat(fd)
    left.sendall(b'x')
    assert right.recv(1) == b'x'
    # Inherited finalizers must not close the child's new runtime.
    assert app.backend.client.get('zygote-native-missing-' + str(os.getppid())) is None
    return os.getpid()
pool.start()
try:
    pids = []
    for _ in range(8):
        job = pool.apply_async(inspect_fds)
        deadline = time.monotonic() + 30
        while not job.ready():
            assert time.monotonic() < deadline
            pool.wait(.01)
        pids.append(job.get())
    assert len(set(pids)) == 8
    # Child cleanup must not deregister sockets in the parent's epoll.
    connection.delete_queue('zygote-native-' + str(os.getpid()))
    assert app.backend.client.get('zygote-native-missing-' + str(os.getpid())) is None
    print(json.dumps(pids))
finally:
    pool.stop()
    connection.close()
'''
    result = subprocess.run(
        [str(ROOT / '.venv-rs/bin/python'), '-c', code, broker], cwd='/tmp',
        capture_output=True, text=True, timeout=90)
    assert result.returncode == 0, result.stdout + result.stderr
    assert len(json.loads(result.stdout.splitlines()[-1])) == 8


def test_parent_epoll_changes_cannot_close_reused_python_fd():
    code = r'''
import os, select, socket
from celery.concurrency._rs_zygote import _close_inherited_fds, _snapshot_runtime_fds
poller = select.epoll()
stream = open('/dev/null')
user_fd = stream.fileno()
read_fd, write_fd = os.pipe()
owned = _snapshot_runtime_fds([poller.fileno()])
pid = os.fork()
if pid == 0:
    try:
        os.close(write_fd)
        assert os.read(read_fd, 1) == b'x'
        _close_inherited_fds({read_fd}, owned)
        assert stream.read() == ''
        os._exit(0)
    except BaseException:
        __import__('traceback').print_exc()
        os._exit(1)
os.close(read_fd)
# Reuse the exact descriptor number; the child still owns its Python resource there.
stream.close()
new_socket = socket.socket()
if new_socket.fileno() != user_fd:
    os.dup2(new_socket.fileno(), user_fd)
poller.register(user_fd, select.EPOLLIN)
os.write(write_fd, b'x')
_, status = os.waitpid(pid, 0)
assert os.waitstatus_to_exitcode(status) == 0
'''
    result = subprocess.run(
        [str(ROOT / '.venv-rs/bin/python'), '-c', code], cwd='/tmp',
        capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
