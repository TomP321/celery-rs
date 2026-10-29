"""Prevent child logs from reaching descriptors reused for broker sockets."""
import json
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('logger_name', ['', 'celery.worker.fd-regression'])
@pytest.mark.parametrize('handler_kind', ['file', 'queue'])
def test_zygote_preserves_logging_descriptors(logger_name, handler_kind):
    script = r'''
import json, logging, logging.handlers, os, socket, sys
from celery.concurrency._rs_zygote import _close_inherited_fds, _snapshot_runtime_fds
import _celery_rs

logger = logging.getLogger(sys.argv[1])
logger.setLevel(logging.ERROR)
logger.propagate = False
queue = None
if sys.argv[2] == 'file':
    handler = logging.FileHandler(os.devnull)
else:
    from billiard import Queue
    queue = Queue()
    handler = logging.handlers.QueueHandler(queue)
logger.addHandler(handler)
connection = _celery_rs.Connection("memory://")
connection.declare_queue("logging-fds")
runtime_fds = _snapshot_runtime_fds(_celery_rs._runtime_fds())
assert runtime_fds
read_fd, write_fd = os.pipe()
pid = os.fork()
if pid == 0:
    try:
        _close_inherited_fds({write_fd}, runtime_fds)
        _close_inherited_fds({write_fd}, runtime_fds)
        left, right = socket.socketpair()
        left.settimeout(0.3)
        right.settimeout(0.3)
        logger.error('log after fork')
        corrupted = False
        for sock in (left, right):
            try:
                corrupted |= bool(sock.recv(4096))
            except TimeoutError:
                pass
        os.write(write_fd, json.dumps({'corrupted': corrupted}).encode())
        os._exit(0)
    except BaseException:
        __import__('traceback').print_exc()
        os._exit(1)
os.close(write_fd)
data = os.read(read_fd, 4096)
os.close(read_fd)
_, status = os.waitpid(pid, 0)
assert os.waitstatus_to_exitcode(status) == 0
result = json.loads(data)
if queue is not None:
    try:
        result['delivered'] = queue.get(timeout=0.5).getMessage() == 'log after fork'
    except Exception:
        result['delivered'] = False
    queue.close()
else:
    result['delivered'] = True
print(json.dumps(result))
'''
    result = subprocess.run(
        [str(ROOT / '.venv-rs/bin/python'), '-I', '-c', script, logger_name, handler_kind],
        capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {'corrupted': False, 'delivered': True}
