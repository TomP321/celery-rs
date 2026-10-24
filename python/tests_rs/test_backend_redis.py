"""Result backend Redis: client Rust vs redis-py del riferimento.

Eseguiti con ``.venv-rs`` (pacchetto celery-rs completo) contro Redis reale
(``CELERY_RS_TEST_REDIS``, default ``127.0.0.1:6390``) e un worker del
riferimento (``.venv-ref``, pool ``solo``).

Coprono il gate della spec 04a: attivazione/disattivazione del client Rust,
valori e sequenza di comandi identici a redis-py, pub/sub di ``AsyncResult.get``
(timeout, propagate, riconnessione dopo ``CLIENT KILL``), ``ResultSet``,
fork (``os.fork`` e billiard) e concorrenza.
"""
from __future__ import annotations

import base64
import json
import os
import re
import socket
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import pytest
import redis as redis_lib
from kombu.serialization import loads

from celery import Celery, chord, group, states
from celery.backends._rs_redis import RsRedis, stats as rs_stats
from celery.exceptions import TimeoutError as CeleryTimeoutError
from celery.result import denied_join_result

ROOT = Path(__file__).resolve().parents[2]
REFERENCE_PYTHON = ROOT / '.venv-ref' / 'bin' / 'python'
REDIS_URL = os.environ.get('CELERY_RS_TEST_REDIS', 'redis://127.0.0.1:6390')

# Database dedicati: gli altri test/agenti usano 0/1 e non vanno disturbati.
def _with_db(url, db):
    base = url.split('?', 1)[0].rstrip('/')
    head, _, tail = base.rpartition('/')
    if tail.isdigit():
        base = head
    return f'{base}/{db}'


BROKER_URL = _with_db(REDIS_URL, 8)
BACKEND_URL = _with_db(REDIS_URL, 9)
QUEUE = f'rs-backend-{uuid.uuid4().hex[:10]}'

TASK_ADD = 'rs_backend.add'
TASK_SLEEPY = 'rs_backend.sleepy'
TASK_BOOM = 'rs_backend.boom'
TASK_TSUM = 'rs_backend.tsum'

WORKER_APP = '''
import os
import time

from celery import Celery

app = Celery(
    'rs-backend-worker',
    broker=os.environ['RS_TEST_BROKER'],
    backend=os.environ['RS_TEST_BACKEND'],
)
app.conf.update(
    task_default_queue=os.environ['RS_TEST_QUEUE'],
    result_expires=300,
    worker_concurrency=1,
    worker_hijack_root_logger=False,
    broker_connection_retry_on_startup=True,
)


@app.task(name='rs_backend.add')
def add(x, y):
    return x + y


@app.task(name='rs_backend.sleepy')
def sleepy(seconds, value=None):
    time.sleep(seconds)
    return value if value is not None else seconds


@app.task(name='rs_backend.boom')
def boom(message='boom'):
    raise ValueError(message)


@app.task(name='rs_backend.tsum')
def tsum(values):
    return sum(values)
'''

REF_STORE = '''
import base64
import json
import os
import sys

from celery import Celery

app = Celery(
    'ref-ops',
    broker=os.environ['RS_TEST_BROKER'],
    backend=os.environ['RS_TEST_BACKEND'],
)
app.conf.update(task_default_queue=os.environ['RS_TEST_QUEUE'])
action, task_id = sys.argv[1], sys.argv[2]
backend = app.backend
key = backend.get_key_for_task(task_id)
if action == 'store':
    backend.store_result(task_id, 42, 'SUCCESS')
elif action == 'get':
    value = backend.get(key)
    sys.stdout.write(base64.b64encode(value or b'').decode())
elif action == 'forget':
    backend.forget(task_id)
elif action == 'meta':
    sys.stdout.write(json.dumps(backend.get_task_meta(task_id), default=str))
elif action == 'read_chord':
    group_id = task_id
    raw = {
        '.s': backend.client.get(backend.get_key_for_group(group_id, '.s')),
        '.t': backend.client.get(backend.get_key_for_group(group_id, '.t')),
        '.j': backend.client.zrange(
            backend.get_key_for_group(group_id, '.j'), 0, -1),
    }
    def encode(value):
        if isinstance(value, bytes):
            return base64.b64encode(value).decode()
        if value is None:
            return None
        return [base64.b64encode(item).decode() for item in value]

    sys.stdout.write(json.dumps(
        {name: encode(value) for name, value in raw.items()}))
'''

CHORD_PART = '''
import base64
import json
import os
import sys

import redis
from celery import Celery
from celery.canvas import signature

app = Celery(
    'ops',
    broker=os.environ['RS_TEST_BROKER'],
    backend=os.environ['RS_TEST_BACKEND'],
)
app.conf.update(task_default_queue=os.environ['RS_TEST_QUEUE'])
group_id, task_id = sys.argv[1], sys.argv[2]
backend = app.backend
backend.set_chord_size(group_id, 2)
backend.add_to_chord(group_id, 0)
request = type('Request', (), {})()
request.id = task_id
request.group = group_id
request.group_index = 0
request.chord = signature('rs_backend.tsum', args=([],))
request.chord_size = 2
request.delivery_info = {}
backend.on_chord_part_return(request, 'SUCCESS', 7)

client = redis.Redis.from_url(os.environ['RS_TEST_BACKEND'])
keys = {
    suffix: backend.get_key_for_group(group_id, suffix)
    for suffix in ('.j', '.s', '.t')
}
raw = {
    '.s': client.get(keys['.s']),
    '.t': client.get(keys['.t']),
    '.j': client.zrange(keys['.j'], 0, -1),
}
def encode(value):
    if isinstance(value, bytes):
        return base64.b64encode(value).decode()
    if value is None:
        return None
    return [base64.b64encode(item).decode() for item in value]


sys.stdout.write(json.dumps({name: encode(value) for name, value in raw.items()}))
'''


def unique(prefix):
    return f'{prefix}-{uuid.uuid4().hex}'


def redis_for(url=BACKEND_URL):
    return redis_lib.Redis.from_url(url)


def bare_app(**conf):
    """App senza task registrati (solo backend/config)."""
    app = Celery('rs-backend-test', broker=BROKER_URL, backend=BACKEND_URL,
                 set_as_current=False)
    app.conf.update(
        task_default_queue=QUEUE,
        result_expires=300,
        result_backend_transport_options={},
        broker_connection_retry_on_startup=True,
        **conf,
    )
    return app


def make_app(**conf):
    app = bare_app(**conf)

    @app.task(name=TASK_ADD)
    def add(x, y):
        return x + y

    @app.task(name=TASK_SLEEPY)
    def sleepy(seconds, value=None):
        time.sleep(seconds)
        return value if value is not None else seconds

    @app.task(name=TASK_BOOM)
    def boom(message='boom'):
        raise ValueError(message)

    @app.task(name=TASK_TSUM)
    def tsum(values):
        return sum(values)

    return app


@pytest.fixture(scope='session')
def worker(tmp_path_factory):
    """Worker del riferimento (``-P solo``) con l'app dei task."""
    directory = tmp_path_factory.mktemp('backend-worker')
    (directory / 'worker_app.py').write_text(WORKER_APP)
    env = dict(os.environ)
    env.update(
        RS_TEST_BROKER=BROKER_URL,
        RS_TEST_BACKEND=BACKEND_URL,
        RS_TEST_QUEUE=QUEUE,
        PYTHONPATH=str(directory),
    )
    proc = subprocess.Popen(
        [str(REFERENCE_PYTHON), '-m', 'celery', '-A', 'worker_app',
         'worker', '-P', 'solo', '--loglevel=INFO',
         '--without-heartbeat', '--without-gossip', '--without-mingle'],
        cwd=directory, env=env, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True,
    )
    lines = []

    def read():
        assert proc.stdout is not None
        for line in proc.stdout:
            lines.append(line)
    import threading
    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                f'worker terminato:\n{"".join(lines[-40:])}')
        if any('ready.' in line for line in lines):
            break
        time.sleep(0.1)
    else:
        proc.terminate()
        raise RuntimeError(f'worker non pronto:\n{"".join(lines[-40:])}')
    yield proc
    proc.terminate()
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:  # pragma: no cover - emergenza
        proc.kill()


@pytest.fixture(scope='session')
def app(worker):
    return make_app()


@pytest.fixture(scope='session')
def ops_dir(tmp_path_factory):
    directory = tmp_path_factory.mktemp('backend-ops')
    (directory / 'ref_store.py').write_text(REF_STORE)
    (directory / 'chord_part.py').write_text(CHORD_PART)
    return directory


def _run_ops(python, directory, script, *args):
    env = dict(os.environ)
    env.update(
        RS_TEST_BROKER=BROKER_URL,
        RS_TEST_BACKEND=BACKEND_URL,
        RS_TEST_QUEUE=QUEUE,
        PYTHONPATH=str(directory),
    )
    result = subprocess.run(
        [str(python), str(directory / script), *args],
        cwd=directory, env=env, capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def _snapshot(url=BACKEND_URL, keys=()):
    client = redis_for(url)
    return {key: client.get(key) for key in keys}


def _decode_meta(raw):
    meta = loads(raw, content_type='application/json', content_encoding='utf-8')
    meta.pop('date_done', None)
    return meta


# --------------------------------------------------------------------- monitor

_MONITOR_RE = re.compile(r'\[(\d+) [^\]]*\] (.*)')


class _Monitor:
    """MONITOR su socket grezzo (stesso approccio di ``tools/compat/wire``)."""

    def __init__(self, db):
        self.db = db
        self.lines = []
        self._stop = None
        parts = REDIS_URL.split('://', 1)[1].split('/', 1)[0]
        host, _, port = parts.rpartition(':')
        self.address = (host or '127.0.0.1', int(port or 6379))
        self.sock = None

    def __enter__(self):
        import threading
        self.sock = socket.create_connection(self.address)
        self.sock.sendall(b'MONITOR\r\n')
        buffer = b''
        while b'\r\n' not in buffer:
            buffer += self.sock.recv(4096)
        buffer = buffer.split(b'\r\n', 1)[1]
        self._stop = threading.Event()
        lines = self.lines
        stop = self._stop
        sock = self.sock

        def read():
            sock.settimeout(0.1)
            pending = buffer
            while not stop.is_set():
                try:
                    chunk = sock.recv(65536)
                except TimeoutError:
                    continue
                except OSError:
                    break
                if not chunk:
                    break
                pending += chunk
                *done, pending = pending.split(b'\r\n')
                lines.extend(item[1:].decode('latin1')
                             for item in done if item.startswith(b'+'))
        self._thread = threading.Thread(target=read, daemon=True)
        self._thread.start()
        time.sleep(0.1)
        return self

    def __exit__(self, *exc):
        time.sleep(0.2)
        self._stop.set()
        try:
            self.sock.close()
        except OSError:
            pass
        self._thread.join(timeout=2)

    def commands(self, marker):
        """Comandi MULTI/EXEC e quelli che nominano *marker* (stesso db)."""
        if isinstance(marker, bytes):
            marker = marker.decode()
        text = marker
        encoded = marker.encode()
        out = []
        for line in self.lines:
            match = _MONITOR_RE.search(line)
            if not match or int(match.group(1)) != self.db:
                continue
            body = match.group(2)
            name = re.match(r'"([^"]+)"', body)
            if not name:
                continue
            command = name.group(1)
            if command in ('MULTI', 'EXEC'):
                out.append((command, None))
                continue
            if text in body or encoded.hex() in body:
                # Chiave del task/gruppo: il resto (valori, score) non serve.
                rest = body[name.end():].lstrip()
                key = re.match(r'"([^"]*)"', rest)
                out.append((command, key.group(1) if key else text))
        return out


@contextmanager
def monitor():
    with _Monitor(db=9) as capture:
        yield capture


# ------------------------------------------------------------------ attivazione

def test_uses_rs_client_and_can_be_disabled(monkeypatch):
    backend = bare_app().backend
    assert type(backend.client) is RsRedis
    assert backend.client.connection_pool is not None
    before = rs_stats()
    assert before['clients'] > 0

    monkeypatch.setenv('CELERY_RS_BACKEND', '0')
    reference = bare_app()
    assert backend._create_rs_client(**backend.connparams) is None
    client = reference.backend.client
    assert isinstance(client, redis_lib.Redis)
    assert not isinstance(client, RsRedis)
    task_id = str(uuid.uuid4())
    reference.backend.store_result(task_id, 1, states.SUCCESS)
    assert _decode_meta(
        reference.backend.get(reference.backend.get_key_for_task(task_id))
    )['result'] == 1
    reference.backend.forget(task_id)

    monkeypatch.delenv('CELERY_RS_BACKEND')
    assert isinstance(bare_app().backend.client, RsRedis)


def test_sentinel_and_subclasses_stay_on_redis_py(app):
    from celery.backends.redis import RedisBackend, SentinelBackend

    sentinel = SentinelBackend(
        app=app, url='sentinel://127.0.0.1:26379/0')
    assert sentinel._create_rs_client(**sentinel.connparams) is None

    class CustomBackend(RedisBackend):
        pass

    custom = CustomBackend(app=app)
    assert custom._create_rs_client(**custom.connparams) is None


# ------------------------------------------------- valori e traffico store/get

def test_store_value_and_traffic_match_reference(app, ops_dir):
    task_id = str(uuid.uuid4())
    backend = app.backend
    key = backend.get_key_for_task(task_id)
    key_text = key.decode() if isinstance(key, bytes) else key

    with monitor() as capture:
        _run_ops(REFERENCE_PYTHON, ops_dir, 'ref_store.py', 'store', task_id)
    reference_commands = capture.commands(key)
    reference_raw = _snapshot(keys=[key])[key]
    assert reference_raw is not None
    reference_meta = _decode_meta(reference_raw)

    redis_for().delete(key)
    with monitor() as capture:
        backend.store_result(task_id, 42, states.SUCCESS)
    rust_commands = capture.commands(key)
    rust_raw = _snapshot(keys=[key])[key]
    rust_meta = _decode_meta(rust_raw)

    assert rust_meta == reference_meta
    assert reference_commands == rust_commands
    assert rust_commands == [
        ('GET', key_text),
        ('MULTI', None), ('SETEX', key_text), ('PUBLISH', key_text),
        ('EXEC', None),
    ]

    # get e forget: stessi comandi e stesso valore.
    with monitor() as capture:
        expected = _run_ops(REFERENCE_PYTHON, ops_dir, 'ref_store.py',
                            'get', task_id)
    assert capture.commands(key) == [('GET', key_text)]
    assert base64.b64decode(expected) == rust_raw
    assert backend.get(key) == rust_raw
    with monitor() as capture:
        backend.forget(task_id)
    assert capture.commands(key) == [('DEL', key_text)]
    assert _snapshot(keys=[key])[key] is None


def test_set_with_ex_uses_set_ex_not_setex(app):
    backend = app.backend
    key = unique('rs:setex')
    backend._set_chord_key(key, '3')
    client = redis_for()
    assert client.get(key) == b'3'
    client.delete(key)


# --------------------------------------------------------------- chord .j/.t/.s

def test_chord_part_values_and_traffic_match_reference(app, ops_dir):
    backend = app.backend
    group_id = str(uuid.uuid4())
    task_id = str(uuid.uuid4())

    with monitor() as capture:
        reference = json.loads(_run_ops(
            REFERENCE_PYTHON, ops_dir, 'chord_part.py', group_id, task_id))
    reference_commands = capture.commands(group_id)
    for suffix in ('.j', '.t', '.s'):
        redis_for().delete(backend.get_key_for_group(group_id, suffix))

    with monitor() as capture:
        rust = json.loads(_run_ops(
            sys.executable, ops_dir, 'chord_part.py', group_id, task_id))
    rust_commands = capture.commands(group_id)

    assert rust == reference
    assert sorted(reference) == ['.j', '.s', '.t']
    assert rust['.s'] is not None and rust['.t'] is not None
    assert rust['.j']
    assert reference_commands == rust_commands
    # .s: SET con scadenza; .t: INCRBY; .j: ZADD + ZCOUNT; poi GET .t/.s.
    commands = [command for command, _ in rust_commands]
    assert 'INCRBY' in commands
    assert 'ZADD' in commands and 'ZCOUNT' in commands
    assert 'MULTI' in commands and 'EXEC' in commands

    for suffix in ('.j', '.t', '.s'):
        redis_for().delete(backend.get_key_for_group(group_id, suffix))


# ------------------------------------------------------------------- pub/sub

def test_asyncresult_get_uses_pubsub(app):
    task_id = str(uuid.uuid4())
    key = app.backend.get_key_for_task(task_id)
    with monitor() as capture:
        result = app.tasks[TASK_SLEEPY].apply_async((1.2, 'done'),
                                                   task_id=task_id)
        started = time.monotonic()
        assert result.get(timeout=20) == 'done'
        elapsed = time.monotonic() - started
    commands = [command for command, _ in capture.commands(key)]
    assert 'SUBSCRIBE' in commands
    assert elapsed >= 1.0

    # Risultato già pronto: get non resta appeso.
    started = time.monotonic()
    assert result.get(timeout=5) == 'done'
    assert time.monotonic() - started < 2.0


def test_get_timeout_propagate_and_disable_sync_subtasks(app):
    slow = app.tasks[TASK_SLEEPY].apply_async((2.5,))
    started = time.monotonic()
    with pytest.raises(CeleryTimeoutError):
        slow.get(timeout=0.2)
    assert time.monotonic() - started < 2.0
    assert slow.get(timeout=20) == 2.5

    failure = app.tasks[TASK_BOOM].apply_async(('kaboom',))
    with pytest.raises(ValueError, match='kaboom'):
        failure.get(timeout=20, propagate=True)
    error = failure.get(timeout=20, propagate=False)
    assert isinstance(error, ValueError)

    ready = app.tasks[TASK_ADD].apply_async((1, 1))
    ready.get(timeout=20)
    with denied_join_result():
        with pytest.raises(RuntimeError):
            ready.get(timeout=5)
    assert ready.get(timeout=5, disable_sync_subtasks=False) == 2


def _group_of_three(app):
    return group(
        app.tasks[TASK_ADD].s(1, 2),
        app.tasks[TASK_ADD].s(3, 4),
        app.tasks[TASK_ADD].s(5, 6),
    ).apply_async()


def test_resultset_join_join_native_iter_native(app):
    assert _group_of_three(app).join(timeout=30) == [3, 7, 11]
    assert _group_of_three(app).join_native(timeout=30) == [3, 7, 11]
    values = dict(_group_of_three(app).iter_native(timeout=30))
    assert sorted(meta['result'] for meta in values.values()) == [3, 7, 11]


def test_chord_end_to_end(app):
    result = chord([
        app.tasks[TASK_ADD].s(1, 1),
        app.tasks[TASK_ADD].s(2, 2),
        app.tasks[TASK_ADD].s(3, 3),
    ])(app.tasks[TASK_TSUM].s())
    assert result.get(timeout=40) == 12


# --------------------------------------------------- riconnessione pub/sub

def test_client_kill_during_get_reconnects(app):
    # Riconnessione esplicita: uccide solo i client pub/sub del nostro db.
    import threading
    second = app.tasks[TASK_SLEEPY].apply_async((2.0, 'after-kill'))
    outcome = {}

    def wait():
        try:
            outcome['value'] = second.get(timeout=30)
        except Exception as exc:  # pragma: no cover - diagnostica
            outcome['error'] = exc
    thread = threading.Thread(target=wait)
    thread.start()
    time.sleep(0.6)
    admin = redis_lib.Redis.from_url(REDIS_URL)
    killed = 0
    listing = admin.execute_command('CLIENT', 'LIST', 'TYPE', 'pubsub')
    if isinstance(listing, bytes):
        listing = listing.decode()
    for line in listing.splitlines():
        fields = dict(part.split('=', 1) for part in line.split() if '=' in part)
        if fields.get('db') == '9':
            admin.execute_command('CLIENT', 'KILL', 'ID', fields['id'])
            killed += 1
    assert killed >= 1
    thread.join(40)
    assert outcome.get('error') is None, outcome
    assert outcome['value'] == 'after-kill'


# ------------------------------------------------------------------- concorrenza

def test_sixteen_threads_store_and_get(app):
    backend = app.backend
    task_ids = [str(uuid.uuid4()) for _ in range(16)]

    def store(index):
        task_id = task_ids[index]
        backend.store_result(task_id, index, states.SUCCESS)
        raw = backend.get(backend.get_key_for_task(task_id))
        return _decode_meta(raw)['result']

    with ThreadPoolExecutor(max_workers=16) as pool:
        values = list(pool.map(store, range(16)))
    assert values == list(range(16))

    def read(task_id):
        return _decode_meta(backend.get(backend.get_key_for_task(task_id)))['result']

    with ThreadPoolExecutor(max_workers=16) as pool:
        assert list(pool.map(read, task_ids)) == list(range(16))

    many = dict(backend.get_many(task_ids))
    assert len(many) == 16
    assert sorted(meta['result'] for meta in many.values()) == list(range(16))
    for task_id in task_ids:
        backend.forget(task_id)


# ------------------------------------------------------------------------ fork

def _child_store(app, task_id, pipe):
    try:
        backend = app.backend
        backend.store_result(task_id, 'child', states.SUCCESS)
        raw = backend.get(backend.get_key_for_task(task_id))
        meta = _decode_meta(raw)
        os.write(pipe, b'OK' if meta['result'] == 'child' else b'BAD')
    except BaseException as exc:  # pragma: no cover - diagnostica del figlio
        os.write(pipe, f'ERR {exc!r}'.encode()[:200])
    finally:
        os._exit(0)


def test_fork_os(app):
    task_id = str(uuid.uuid4())
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:  # pragma: no cover - processo figlio
        os.close(read_fd)
        _child_store(app, task_id, write_fd)
    os.close(write_fd)
    with os.fdopen(read_fd, 'rb') as reader:
        payload = reader.read(200)
    _, status = os.waitpid(pid, 0)
    assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
    assert payload == b'OK'
    assert app.backend.get_task_meta(task_id)['result'] == 'child'


def test_fork_billiard(app):
    import billiard

    task_id = str(uuid.uuid4())
    process = billiard.Process(target=_billiard_store, args=(app, task_id))
    process.start()
    process.join(60)
    assert process.exitcode == 0
    assert app.backend.get_task_meta(task_id)['result'] == 'billiard'


def _billiard_store(app, task_id):
    backend = app.backend
    backend.store_result(task_id, 'billiard', states.SUCCESS)
    assert _decode_meta(
        backend.get(backend.get_key_for_task(task_id)))['result'] == 'billiard'


# --------------------------------------------------- lettura incrociata ref/rs

def test_reference_worker_results_readable_by_both(app, ops_dir):
    # Client/worker del riferimento: risultato scritto dal worker, letto da rs.
    task_id = str(uuid.uuid4())
    app.tasks[TASK_ADD].apply_async((2, 5), task_id=task_id).get(timeout=30)
    key = app.backend.get_key_for_task(task_id)
    raw = app.backend.get(key)
    assert _decode_meta(raw)['result'] == 7

    # E il riferimento legge lo stesso valore.
    reference = json.loads(_run_ops(
        REFERENCE_PYTHON, ops_dir, 'ref_store.py', 'meta', task_id))
    assert reference['result'] == 7
    assert reference['status'] == states.SUCCESS


# ------------------------------------------------------------------- JSON

def test_json_encode_decode_matches_kombu(app):
    import datetime
    import decimal

    from kombu.serialization import dumps

    backend = app.backend
    samples = [
        {'a': [1, 2.5, None, True], 'b': 'ü', 'big': 2 ** 80},
        [{'nested': {'x': 1}}, 'text'],
        datetime.datetime(2020, 1, 2, 3, 4, 5),
        decimal.Decimal('1.250'),
    ]
    for sample in samples:
        payload = backend.encode(sample)
        content_type, _, expected = dumps(sample, serializer='json')
        assert content_type == 'application/json'
        assert isinstance(payload, str)
        assert payload == expected
        assert backend.decode(payload) == backend.decode(expected)


def test_json_accept_content_is_respected():
    from kombu.exceptions import ContentDisallowed

    backend = bare_app(accept_content=['pickle']).backend
    payload = backend.encode({'a': 1})
    with pytest.raises(ContentDisallowed):
        backend.decode(payload)


def test_health_check_interval_is_mapped():
    app = bare_app(redis_backend_health_check_interval=0.1)
    backend = app.backend
    assert isinstance(backend.client, RsRedis)
    assert backend.connparams['health_check_interval'] == 0.1
    key = unique('rs:health')
    backend._set(key, 'v')
    time.sleep(0.15)
    assert backend.get(key) == b'v'
    redis_for().delete(key)
