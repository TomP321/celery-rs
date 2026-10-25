"""Worker ``-P solo`` con il consumer Rust su Redis e RabbitMQ.

I worker girano in un sottoprocesso (``.venv-rs`` per celery-rs, ``.venv-ref``
per il riferimento) e i client nei due venv; il processo pytest è ``.venv-rs``.
Copre: task semplici e con payload grandi, retry, countdown/eta/expires,
acks_late e requeue, priorità, rate limit, revoke, code multiple e
add/cancel_consumer a caldo, inspect, eventi ``-E``, prefetch, perdita del
broker con riconnessione, shutdown warm/cold, ``CELERY_RS_WORKER=0``.
"""
from __future__ import annotations

import base64
import json
import os
import re
import signal
import socket
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import pytest
import redis

from celery import Celery

TESTS_DIR = Path(__file__).resolve().parent
ROOT = TESTS_DIR.parents[1]
REF_VENV = ROOT / '.venv-ref'
RS_VENV = ROOT / '.venv-rs'
REF_PYTHON = REF_VENV / 'bin' / 'python'

RABBIT_API = os.environ.get(
    'CELERY_RS_TEST_RABBIT_API', 'http://127.0.0.1:15673/api')

REDIS_HOST, REDIS_PORT = '127.0.0.1', 6390

BROKERS = {
    'redis': {
        'url': f'redis://{REDIS_HOST}:{REDIS_PORT}/7',
        'backend': f'redis://{REDIS_HOST}:{REDIS_PORT}/8',
        'port': (REDIS_HOST, REDIS_PORT),
        'check': (REDIS_HOST, REDIS_PORT),
    },
    'amqp': {
        'url': 'amqp://guest:guest@127.0.0.1:5673//',
        'backend': f'redis://{REDIS_HOST}:{REDIS_PORT}/9',
        'port': ('127.0.0.1', 5673),
        'check': ('127.0.0.1', 15673),
    },
}

#: Contatori per isolare i worker dedicati (DB Redis dedicati e nomi unici).
_db_counter = [9]


def unique(prefix):
    return f'{prefix}.{uuid.uuid4().hex[:12]}'


def next_redis_db():
    # Redis di test ha 16 DB (0-15): 7 è il broker condiviso, 8/9 il backend.
    _db_counter[0] += 1
    if _db_counter[0] > 15:
        _db_counter[0] = 10
    return _db_counter[0]


def clean_env(**extra):
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('PYTHON') and not key.startswith('CELERY_')}
    env['PYTHONPATH'] = str(TESTS_DIR)
    env['PYTHONHASHSEED'] = '0'
    env.update({key: str(value) for key, value in extra.items()})
    return env


def tcp_ok(host, port):
    sock = socket.socket()
    sock.settimeout(1)
    try:
        sock.connect((host, port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def broker_available(name):
    host, port = BROKERS[name]['check']
    return tcp_ok(host, port)


@pytest.fixture(scope='session', autouse=True)
def require_brokers():
    missing = [name for name in BROKERS if not broker_available(name)]
    if missing:
        pytest.skip(f'broker non raggiungibili: {missing}')


# ---------------------------------------------------------------------------
# broker helpers
# ---------------------------------------------------------------------------

def redis_client(db=0):
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=db,
                       decode_responses=True)


def redis_queue_len(db, queue):
    client = redis_client(db)
    total = 0
    for key in client.keys(f'{queue}*'):
        if client.type(key) == 'list':
            total += client.llen(key)
    return total


def kill_redis_consumer(db):
    """Chiude la connessione del consumer Rust (comando BRPOP) nel DB dato."""
    client = redis_client(db)
    killed = 0
    for line in client.execute_command('CLIENT', 'LIST').split('\n'):
        if not line.strip():
            continue
        fields = dict(part.split('=', 1) for part in line.split(' ')
                      if '=' in part)
        if fields.get('cmd') == 'brpop' and fields.get('db') == str(db):
            client.execute_command('CLIENT', 'KILL', 'ID', fields['id'])
            killed += 1
    return killed


def rabbit_api(path, method='GET'):
    authenticator = 'Basic ' + base64.b64encode(b'guest:guest').decode()
    request = urllib.request.Request(
        f'{RABBIT_API}{path}', method=method,
        headers={'Authorization': authenticator})
    with urllib.request.urlopen(request, timeout=10) as response:
        payload = response.read()
        return json.loads(payload) if payload else None


def rabbit_queue(name):
    quoted = urllib.parse.quote(name, safe='')
    return rabbit_api(f'/queues/%2F/{quoted}')


def rabbit_queue_ready(name):
    """Messaggi pronti letti da rabbitmqctl (l'API management aggiorna a 5s)."""
    try:
        result = subprocess.run(
            ['docker', 'exec', 'celery-rs-rabbitmq-1', 'rabbitmqctl',
             'list_queues', 'name', 'messages_ready', '--formatter', 'json'],
            capture_output=True, text=True, timeout=30)
        for row in json.loads(result.stdout):
            if row.get('name') == name:
                return int(row.get('messages_ready', 0))
        return 0
    except Exception:
        try:
            return int(rabbit_queue(name).get('messages_ready', 0))
        except Exception:
            return 0


def rabbit_delete_queue(name):
    quoted = urllib.parse.quote(name, safe='')
    try:
        rabbit_api(f'/queues/%2F/{quoted}', method='DELETE')
    except Exception:
        pass


def rabbit_consumer_connection(queue):
    for consumer in rabbit_api('/consumers') or ():
        if consumer.get('queue', {}).get('name') == queue:
            details = consumer.get('channel_details') or {}
            name = details.get('connection_name')
            if name:
                return name
    return None


def wait_rabbit_consumer(queue, timeout=10):
    deadline = time.monotonic() + timeout
    name = rabbit_consumer_connection(queue)
    while name is None and time.monotonic() < deadline:
        time.sleep(0.1)
        name = rabbit_consumer_connection(queue)
    return name


def rabbit_kill_consumer(queue, timeout=10):
    name = rabbit_consumer_connection(queue)
    deadline = time.monotonic() + timeout
    while name is None and time.monotonic() < deadline:
        time.sleep(0.1)
        name = rabbit_consumer_connection(queue)
    if name is None:
        return 0
    rabbit_api(f'/connections/{urllib.parse.quote(name, safe="")}',
               method='DELETE')
    return 1


def rabbit_connection_product(name):
    for connection in rabbit_api('/connections') or ():
        if connection.get('name') == name:
            return connection.get('client_properties', {}).get('product')
    return None


# ---------------------------------------------------------------------------
# client del riferimento (sottoprocesso in .venv-ref)
# ---------------------------------------------------------------------------

REF_PRELUDE = '''
import datetime, json, os, time
from celery import Celery

app = Celery(
    'ref-client',
    broker=os.environ['CELERY_RS_TEST_WORKER_BROKER'],
    backend=os.environ['CELERY_RS_TEST_WORKER_BACKEND'],
    set_as_current=False)
app.conf.update(
    task_default_queue=os.environ['CELERY_RS_TEST_WORKER_QUEUE'],
    task_serializer='json', result_serializer='json',
    accept_content=['json'], result_expires=60,
    task_queue_max_priority=10,
    broker_connection_retry_on_startup=True)


def emit(value):
    print(json.dumps(value))


def submit(name, args=(), kwargs=None, **options):
    return app.send_task(name, args=list(args), kwargs=kwargs or {}, **options)
'''


def run_ref(code, worker, timeout=120):
    env = clean_env(
        CELERY_RS_TEST_WORKER_BROKER=worker.broker,
        CELERY_RS_TEST_WORKER_BACKEND=worker.backend,
        CELERY_RS_TEST_WORKER_QUEUE=worker.queue,
    )
    result = subprocess.run(
        [str(REF_PYTHON), '-c', REF_PRELUDE + code], cwd=TESTS_DIR,
        env=env, capture_output=True, text=True, timeout=timeout)
    assert result.returncode == 0, (
        f'client ref fallito ({result.returncode}):\n'
        f'{result.stdout[-4000:]}\n{result.stderr[-8000:]}')
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    return json.loads(lines[-1])


def run_ref_async(code, worker):
    env = clean_env(
        CELERY_RS_TEST_WORKER_BROKER=worker.broker,
        CELERY_RS_TEST_WORKER_BACKEND=worker.backend,
        CELERY_RS_TEST_WORKER_QUEUE=worker.queue,
    )
    return subprocess.Popen(
        [str(REF_PYTHON), '-c', REF_PRELUDE + code], cwd=TESTS_DIR,
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


RECEIVER_CODE = """
import json, os, sys
from celery import Celery
from celery.events import EventReceiver

app = Celery(
    'ref-receiver',
    broker=os.environ['CELERY_RS_TEST_WORKER_BROKER'],
    backend=os.environ['CELERY_RS_TEST_WORKER_BACKEND'],
    set_as_current=False)


def handle(event):
    print(json.dumps(dict(event)), flush=True)


with app.connection_for_read() as conn:
    receiver = EventReceiver(
        conn, app=app, node_id=os.environ.get('RS_RECEIVER_ID', 'receiver'),
        handlers={'*': handle})
    receiver.capture(limit=None, timeout=30)
"""


def start_event_receiver(worker, node_id='rs-receiver'):
    env = clean_env(
        CELERY_RS_TEST_WORKER_BROKER=worker.broker,
        CELERY_RS_TEST_WORKER_BACKEND=worker.backend,
        CELERY_RS_TEST_WORKER_QUEUE=worker.queue,
        RS_RECEIVER_ID=node_id,
    )
    return subprocess.Popen(
        [str(REF_PYTHON), '-c', RECEIVER_CODE], cwd=TESTS_DIR, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def stop_event_receiver(process, timeout=10):
    process.terminate()
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
    events = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith('{'):
            try:
                events.append(json.loads(line))
            except ValueError:
                pass
    return events, stderr


# ---------------------------------------------------------------------------
# worker
# ---------------------------------------------------------------------------

class Worker:
    def __init__(self, venv, broker_name, broker, backend, queue, nodename,
                 process, log_path, state_path, db, env, rust=True):
        self.venv = str(venv)
        self.broker_name = broker_name
        self.broker = broker
        self.backend = backend
        self.queue = queue
        self.nodename = nodename
        self.process = process
        self.log_path = Path(log_path)
        self.state_path = Path(state_path) if state_path else None
        self.db = db
        self.env = env
        self.rust = rust

    # ------------------------------------------------------------ diagnostica

    def log(self):
        try:
            return self.log_path.read_text(errors='replace')
        except OSError:
            return ''

    def wait_log(self, pattern, timeout=20, count=1):
        deadline = time.monotonic() + timeout
        regex = re.compile(pattern)
        while time.monotonic() < deadline:
            if len(regex.findall(self.log())) >= count:
                return True
            if self.process.poll() is not None:
                return len(regex.findall(self.log())) >= count
            time.sleep(0.05)
        return False

    def wait_started(self, task_name, count=1, timeout=30):
        return self.wait_log(
            rf'Task {re.escape(task_name)}\[', timeout=timeout, count=count)

    def stats(self):
        if self.state_path is None:
            return None
        try:
            return json.loads(self.state_path.read_text())
        except (OSError, ValueError):
            return None

    def wait_stats(self, predicate, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            stats = self.stats()
            if stats is not None and predicate(stats):
                return stats
            time.sleep(0.05)
        return self.stats()

    def ping(self, timeout=3):
        result = subprocess.run(
            [str(REF_PYTHON), '-m', 'celery', '-A', 'worker_consumer_app',
             'inspect', 'ping', '-d', self.nodename, '-t', str(timeout)],
            cwd=TESTS_DIR, env=self.env, capture_output=True, text=True)
        return 'pong' in result.stdout

    def inspect(self, command, timeout=5, *extra):
        result = subprocess.run(
            [str(REF_PYTHON), '-m', 'celery', '-A', 'worker_consumer_app',
             'inspect', command, '-d', self.nodename, '-t', str(timeout),
             *extra],
            cwd=TESTS_DIR, env=self.env, capture_output=True, text=True)
        return result

    def control(self, command, *args, timeout=5):
        result = subprocess.run(
            [str(REF_PYTHON), '-m', 'celery', '-A', 'worker_consumer_app',
             'control', command, *args, '-d', self.nodename,
             '-t', str(timeout)],
            cwd=TESTS_DIR, env=self.env, capture_output=True, text=True)
        return result

    # ------------------------------------------------------------ vita

    def ready(self, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(
                    f'worker terminato ({self.process.returncode}):\n'
                    f'{self.log()[-4000:]}')
            if self.rust and self.stats() is not None and self.ping(1):
                return self
            if not self.rust and self.ping(1):
                return self
            time.sleep(0.3)
        self.stop()
        raise RuntimeError(f'worker non pronto:\n{self.log()[-4000:]}')

    def stop(self, sig=signal.SIGTERM, timeout=30):
        if self.process.poll() is None:
            try:
                os.killpg(self.process.pid, sig)
            except ProcessLookupError:
                pass
        try:
            return self.process.wait(timeout)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            return self.process.wait(10)

    def cleanup(self):
        self.stop(signal.SIGKILL)
        if self.broker_name == 'amqp':
            rabbit_delete_queue(self.queue)


def start_worker(broker_name, *, queues=None, venv=RS_VENV, rust=True,
                 args=(), env_extra=None, nodename=None, db=None,
                 backend=None, multiplier=1, concurrency=1,
                 acks_on_failure=True, directory=None, full=False,
                 loglevel='info', pool='solo'):
    info = BROKERS[broker_name]
    queue = queues or unique(f'rs-worker-{broker_name}')
    nodename = nodename or (unique('rs-worker') + '@localhost')
    db = db if db is not None else next_redis_db()
    broker = (f'redis://{REDIS_HOST}:{REDIS_PORT}/{db}'
              if broker_name == 'redis' else info['url'])
    backend = backend or info['backend']
    directory = Path(directory or tempfile.mkdtemp(prefix='rs-worker-'))
    directory.mkdir(parents=True, exist_ok=True)
    log_path = directory / f'{nodename.split("@")[0]}.log'
    state_path = directory / f'{nodename.split("@")[0]}.json'
    env = clean_env(
        CELERY_RS_TEST_WORKER_BROKER=broker,
        CELERY_RS_TEST_WORKER_BACKEND=backend,
        CELERY_RS_TEST_WORKER_QUEUE=queue,
        CELERY_RS_TEST_PREFETCH_MULTIPLIER=multiplier,
        CELERY_RS_TEST_ACKS_ON_FAILURE=1 if acks_on_failure else 0,
    )
    if rust:
        env['CELERY_RS_WORKER_STATE'] = str(state_path)
    if env_extra:
        env.update({key: str(value) for key, value in env_extra.items()})
    command = [
        str(Path(venv) / 'bin' / 'python'), '-m', 'celery',
        '-A', 'worker_consumer_app', 'worker', '-P', pool,
        '-c', str(concurrency),
        '-l', loglevel, '-Q', queue, '-n', nodename,
        *(() if full else ('--without-gossip', '--without-mingle')),
        *args,
    ]
    with open(log_path, 'w') as log:
        process = subprocess.Popen(
            command, cwd=TESTS_DIR, env=env, stdout=log,
            stderr=subprocess.STDOUT, start_new_session=True)
    worker = Worker(venv, broker_name, broker, backend, queue, nodename,
                    process, log_path, state_path if rust else None, db, env,
                    rust=rust)
    return worker


def start_shared_worker(broker_name, directory):
    """Worker con gossip/mingle attivi e ``-E``: percorso completo del master."""
    return start_worker(
        broker_name, directory=directory, args=('-E',),
        acks_on_failure=False, multiplier=1, full=True)


@pytest.fixture(scope='module')
def workers(request, tmp_path_factory):
    directory = tmp_path_factory.mktemp('workers')
    started = {}
    for name in BROKERS:
        if broker_available(name):
            started[name] = start_shared_worker(
                name, directory / name).ready()
    yield started
    for worker in started.values():
        worker.stop()
        worker.cleanup()


@pytest.fixture(params=list(BROKERS))
def worker(request, workers):
    name = request.param
    if name not in workers:
        pytest.skip(f'broker {name} non raggiungibile')
    return workers[name]


@pytest.fixture
def dedicated(tmp_path):
    started = []

    def factory(broker_name, **kwargs):
        worker = start_worker(broker_name, directory=tmp_path, **kwargs)
        worker.ready()
        started.append(worker)
        return worker

    yield factory
    for worker in started:
        worker.stop(signal.SIGKILL)
        worker.cleanup()


# ---------------------------------------------------------------------------
# client rs (nel processo pytest)
# ---------------------------------------------------------------------------

def rs_app(worker):
    app = Celery('rs-client', broker=worker.broker, backend=worker.backend,
                 set_as_current=False)
    app.conf.update(
        task_default_queue=worker.queue,
        task_serializer='json', result_serializer='json',
        accept_content=['json'], result_expires=60,
        task_queue_max_priority=10,
        broker_connection_retry_on_startup=True,
    )
    return app


def submit(worker, name, args=(), kwargs=None, **options):
    app = rs_app(worker)
    try:
        return app.send_task(name, args=list(args), kwargs=kwargs or {},
                             **options)
    finally:
        app.close()


# ---------------------------------------------------------------------------
# test
# ---------------------------------------------------------------------------

def test_worker_uses_rust_consumer(worker):
    result = run_ref('''
r = submit('rs_worker.add', (2, 3))
emit({'value': r.get(timeout=15)})
''', worker)
    assert result['value'] == 5

    stats = worker.wait_stats(lambda value: value.get('messages', 0) >= 1)
    assert stats is not None and stats['consumer'] == 'rust'
    assert stats['connections'] >= 1
    assert stats['consumers'] >= 1

    inspect = worker.inspect('stats')
    assert inspect.returncode == 0
    assert 'prefetch_count' in inspect.stdout

    if worker.broker_name == 'amqp':
        # La connessione che consuma i task è di lapin (core Rust), non py-amqp.
        name = wait_rabbit_consumer(worker.queue)
        assert name is not None
        assert rabbit_connection_product(name) == 'lapin'


def test_large_args_and_kwargs(worker):
    payload = list(range(4000))
    kwargs = {f'key{index}': f'value-{index}-' + 'x' * 50
              for index in range(200)}
    result = run_ref('''
payload = list(range(4000))
kwargs = {f"key{i}": f"value-{i}-" + "x" * 50 for i in range(200)}
r = submit("rs_worker.big", (payload,), kwargs)
value = r.get(timeout=30)
emit({"value": value, "expected": {"len": 4000, "keys": sorted(kwargs),
                                   "checksum": sum(payload)}})
''', worker)
    assert result['value'] == result['expected']


def test_rs_client_against_rs_worker(worker):
    payload = list(range(500))
    result = submit(worker, 'rs_worker.big', (payload,), {'a': 1, 'b': 2})
    value = result.get(timeout=20)
    assert value == {'len': 500, 'keys': ['a', 'b'], 'checksum': sum(payload)}


def test_rs_client_against_ref_worker(tmp_path):
    worker = start_worker('redis', venv=REF_VENV, rust=False,
                          directory=tmp_path)
    worker.ready()
    try:
        app = rs_app(worker)
        try:
            result = app.send_task('rs_worker.add', args=(20, 22))
            assert result.get(timeout=20) == 42
        finally:
            app.close()
    finally:
        worker.stop()
        worker.cleanup()


def test_retry_with_countdown(worker):
    result = run_ref('''
r = submit('rs_worker.retry', (42,))
emit({'value': r.get(timeout=20)})
''', worker)
    assert result['value'] == 42


def test_eta_countdown_and_expires(worker):
    result = run_ref('''
scheduled = []
t0 = time.time()
r = submit('rs_worker.add', (3, 4), countdown=0.8)
scheduled.append({'kind': 'countdown', 'value': r.get(timeout=20),
                  'elapsed': time.time() - t0})
t0 = time.time()
r = submit('rs_worker.add', (5, 6),
           eta=datetime.datetime.now(datetime.UTC)
           + datetime.timedelta(seconds=0.8))
scheduled.append({'kind': 'eta', 'value': r.get(timeout=20),
                  'elapsed': time.time() - t0})
expired = submit('rs_worker.add', (1, 1),
                 expires=datetime.datetime.now(datetime.UTC)
                 - datetime.timedelta(seconds=1))
try:
    expired.get(timeout=10)
    outcome = 'success'
except Exception as exc:
    outcome = type(exc).__name__
emit({'scheduled': scheduled, 'expired': outcome})
''', worker)
    for item in result['scheduled']:
        assert item['value'] in (7, 11)
        assert item['elapsed'] >= 0.6, item


def test_expired_task_is_revoked(worker):
    result = run_ref("""
expired = submit('rs_worker.add', (1, 1),
                 expires=datetime.datetime.now(datetime.UTC)
                 - datetime.timedelta(seconds=1))
try:
    expired.get(timeout=10)
    outcome = 'success'
except Exception as exc:
    outcome = type(exc).__name__
emit({'expired': outcome})
""", worker)
    assert result['expired'] == 'TaskRevokedError'


def test_rate_limit(worker):
    result = run_ref('''
t0 = time.time()
results = [submit('rs_worker.rate', (index,)).get(timeout=20)
           for index in range(3)]
emit({'values': results, 'elapsed': time.time() - t0})
''', worker)
    assert result['values'] == [0, 1, 2]
    assert result['elapsed'] >= 0.8, result


def test_acks_late_failure_is_rejected_without_requeue(worker):
    path = Path(tempfile.mkdtemp(prefix='rs-acks-late-')) / 'attempt'
    result = run_ref(f"""
path = {str(path)!r}
r = submit('rs_worker.acks_late_fail_once', (path, 42))
try:
    r.get(timeout=15)
    outcome = 'success'
except Exception as exc:
    outcome = type(exc).__name__
# La coda deve restare vuota: con acks_late e
# task_acks_on_failure_or_timeout=False il fallimento scarta il messaggio.
time.sleep(1.0)
emit({{'outcome': outcome, 'attempt': os.path.exists(path)}})
""", worker, timeout=60)
    assert result['attempt'] is True
    assert result['outcome'] == 'RuntimeError'


def test_reject_requeue_redelivers(worker):
    path = Path(tempfile.mkdtemp(prefix='rs-reject-')) / 'attempt'
    result = run_ref(f"""
path = {str(path)!r}
r = submit('rs_worker.reject_once', (path, 77))
emit({{'value': r.get(timeout=20), 'attempt': os.path.exists(path)}})
""", worker, timeout=60)
    assert result['attempt'] is True
    assert result['value'] == 77


def _priority_scenario(worker):
    directory = Path(tempfile.mkdtemp(prefix='rs-priority-'))
    order = directory / 'order'
    return run_ref(f"""
order = {str(order)!r}
busy = submit('rs_worker.marker', (order, 'busy', 1.5))
time.sleep(0.1)
low1 = submit('rs_worker.marker', (order, 'low1', 0), priority=0)
low2 = submit('rs_worker.marker', (order, 'low2', 0), priority=0)
high = submit('rs_worker.marker', (order, 'high', 0), priority=9)
for r in (busy, low1, low2, high):
    r.get(timeout=30)
emit({{'order': open(order).read().split()}})
""", worker, timeout=60)['order']


def test_priority_order_matches_reference(worker):
    rs_order = _priority_scenario(worker)
    assert set(rs_order) == {'busy', 'low1', 'low2', 'high'}, rs_order
    ref_worker = start_worker(worker.broker_name, venv=REF_VENV, rust=False,
                              acks_on_failure=False)
    try:
        ref_worker.ready()
        ref_order = _priority_scenario(ref_worker)
    finally:
        ref_worker.stop()
        ref_worker.cleanup()
    assert rs_order == ref_order, (rs_order, ref_order)


def test_revoke_before_execution(worker):
    directory = Path(tempfile.mkdtemp(prefix='rs-revoke-'))
    marker = directory / 'executed'
    result = run_ref(f'''
import uuid
marker = {str(marker)!r}
busy = submit('rs_worker.marker', (marker, 'busy', 2.0))
target_id = str(uuid.uuid4())
# Revoca registrata prima che il messaggio esista: qualunque ordine di
# consegna del broker lo scarta.
app.control.revoke(target_id, reply=True, timeout=5)
target = submit('rs_worker.marker', (marker, 'target', 0), task_id=target_id)
busy.get(timeout=30)
time.sleep(0.5)
emit({{'marker': open(marker).read().split(),
       'state': app.AsyncResult(target.id).state}})
''', worker, timeout=60)
    assert result['marker'] == ['busy'], result
    assert result['state'] != 'SUCCESS'


def test_inspect_commands(worker):
    result = run_ref("""
scheduled = submit('rs_worker.add', (1, 2), countdown=5.0)
emit({'id': scheduled.id})
""", worker)
    assert result['id']
    try:
        active = worker.inspect('active')
        assert active.returncode == 0
        reserved = worker.inspect('reserved')
        assert reserved.returncode == 0
        scheduled = worker.inspect('scheduled')
        assert 'rs_worker.add' in scheduled.stdout, scheduled.stdout
        registered = worker.inspect('registered')
        assert 'rs_worker.add' in registered.stdout
        stats = worker.inspect('stats')
        assert 'prefetch_count' in stats.stdout
        active_queues = worker.inspect('active_queues')
        assert worker.queue in active_queues.stdout
    finally:
        time.sleep(4.5)


def test_events_are_emitted(worker):
    receiver = start_event_receiver(worker)
    try:
        time.sleep(2.0)
        result = run_ref("""
value = submit('rs_worker.add', (8, 9)).get(timeout=20)
emit({'value': value})
""", worker)
        time.sleep(2.0)
    finally:
        events, stderr = stop_event_receiver(receiver)
    assert result['value'] == 17
    assert events, stderr[-4000:]
    types = sorted({event['type'] for event in events})
    assert 'task-received' in types, types
    assert 'task-succeeded' in types, types
    assert 'worker-heartbeat' in types, types
    counting = [event for event in events if event['type'] == 'task-received']
    assert counting
    event = counting[0]
    fields = {
        'uuid': type(event.get('uuid')).__name__,
        'name': type(event.get('name')).__name__,
        'args': type(event.get('args')).__name__,
        'kwargs': type(event.get('kwargs')).__name__,
        'retries': type(event.get('retries')).__name__,
        'hostname': type(event.get('hostname')).__name__,
    }
    assert fields == {
        'uuid': 'str', 'name': 'str', 'args': 'str', 'kwargs': 'str',
        'retries': 'int', 'hostname': 'str',
    }, fields


def test_multiple_queues_and_hot_add_cancel(dedicated):
    queue_a = unique('rs-queues-a')
    queue_b = unique('rs-queues-b')
    extra = unique('rs-queues-extra')
    worker = dedicated('redis', queues=queue_a)
    # il worker deve ascoltare due code: riavvio con -Q multipla non è
    # possibile a caldo, quindi si aggiunge la seconda con add_consumer.
    added = worker.control('add_consumer', queue_b)
    assert added.returncode == 0
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        active = worker.inspect('active_queues').stdout
        if queue_b in active:
            break
        time.sleep(0.2)
    assert queue_b in worker.inspect('active_queues').stdout

    app = rs_app(worker)
    try:
        result = app.send_task('rs_worker.add', args=(1, 2), queue=queue_b)
        assert result.get(timeout=20) == 3
    finally:
        app.close()

    assert worker.control('add_consumer', extra).returncode == 0
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if extra in worker.inspect('active_queues').stdout:
            break
        time.sleep(0.2)
    assert extra in worker.inspect('active_queues').stdout

    app = rs_app(worker)
    try:
        result = app.send_task('rs_worker.add', args=(2, 2), queue=extra)
        assert result.get(timeout=20) == 4
        assert worker.control('cancel_consumer', extra).returncode == 0
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if extra not in worker.inspect('active_queues').stdout:
                break
            time.sleep(0.2)
        assert extra not in worker.inspect('active_queues').stdout
        # La coda non è più consumata: il task resta nel broker.
        pending = app.send_task('rs_worker.add', args=(3, 3), queue=extra)
        time.sleep(1.5)
        assert app.AsyncResult(pending.id).state == 'PENDING'
    finally:
        app.close()


def test_prefetch_multiplier_is_effective(dedicated):
    for broker_name in BROKERS:
        if not broker_available(broker_name):
            continue
        queue = unique(f'rs-prefetch-{broker_name}')
        release = Path(tempfile.mkdtemp(prefix='rs-prefetch-')) / 'release'
        worker = dedicated(broker_name, queues=queue, multiplier=2,
                           concurrency=2)
        app = rs_app(worker)
        try:
            results = [
                app.send_task('rs_worker.wait_file', args=(str(release), 60))
                for _ in range(10)]
            assert worker.wait_started('rs_worker.wait_file', timeout=20)
            time.sleep(1.0)
            # prefetch = concurrency(2) * worker_prefetch_multiplier(2) = 4:
            # il broker tiene le altre 6.
            deadline = time.monotonic() + 10
            while True:
                if broker_name == 'redis':
                    ready = redis_queue_len(worker.db, queue)
                else:
                    ready = rabbit_queue_ready(queue)
                if ready == 6 or time.monotonic() >= deadline:
                    break
                time.sleep(0.2)
            assert ready == 6, (broker_name, ready, worker.log()[-2000:])
            release.write_text('go')
            assert all(result.get(timeout=30) for result in results)
        finally:
            app.close()
            worker.cleanup()


def test_multiple_queues_with_dash_q(dedicated):
    queue_a = unique('rs-multi-a')
    queue_b = unique('rs-multi-b')
    worker = dedicated('redis', queues=f'{queue_a},{queue_b}')
    app = rs_app(worker)
    try:
        first = app.send_task('rs_worker.add', args=(1, 2), queue=queue_a)
        second = app.send_task('rs_worker.add', args=(3, 4), queue=queue_b)
        assert first.get(timeout=20) == 3
        assert second.get(timeout=20) == 7
        active = worker.inspect('active_queues').stdout
        assert queue_a in active and queue_b in active, active
    finally:
        app.close()


def test_broker_kill_reconnects_without_losing_tasks(dedicated):
    for broker_name in BROKERS:
        if not broker_available(broker_name):
            continue
        queue = unique(f'rs-reconnect-{broker_name}')
        worker = dedicated(broker_name, queues=queue)
        app = rs_app(worker)
        try:
            first = app.send_task('rs_worker.add', args=(1, 1))
            assert first.get(timeout=20) == 2
            if broker_name == 'redis':
                assert kill_redis_consumer(worker.db) >= 1
            else:
                assert rabbit_kill_consumer(queue) >= 1
            # Il worker deve riconnettere e continuare: nessun task perso.
            results = [app.send_task('rs_worker.add', args=(index, index))
                       for index in range(4)]
            assert [result.get(timeout=30) for result in results] == [
                0, 2, 4, 6]
            stats = worker.wait_stats(
                lambda value: value.get('connections', 0) >= 2, timeout=30)
            assert stats['connections'] >= 2, stats
            assert 'Connection to broker lost' in worker.log()
        finally:
            app.close()
            worker.cleanup()


def _shutdown_outcome(dedicated, broker_name, sig, soft_timeout=0,
                      venv=RS_VENV, rust=True):
    queue = unique(f'rs-shutdown-{broker_name}')
    extra = {'CELERY_RS_TEST_SOFT_SHUTDOWN': soft_timeout}
    worker = dedicated(broker_name, queues=queue, env_extra=extra,
                       venv=venv, rust=rust, loglevel='debug')
    marker = Path(tempfile.mkdtemp(prefix='rs-shutdown-')) / 'started'
    app = rs_app(worker)
    try:
        result = app.send_task('rs_worker.marker_slow',
                               args=(str(marker), 8, 7))
        assert worker.wait_started('rs_worker.marker_slow', timeout=20)
        deadline = time.monotonic() + 15
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert marker.exists(), 'il task non è partito'
        started = time.monotonic()
        code = worker.stop(sig=sig, timeout=40)
        elapsed = time.monotonic() - started
        time.sleep(0.5)
        if broker_name == 'redis':
            ready = redis_queue_len(worker.db, queue)
        else:
            ready = rabbit_queue_ready(queue)
        return {'code': code, 'ready': ready, 'elapsed': elapsed,
                'result_id': result.id}
    finally:
        app.close()
        worker.cleanup()


def test_warm_shutdown_with_running_task(dedicated):
    rs = _shutdown_outcome(dedicated, 'redis', signal.SIGTERM)
    ref = _shutdown_outcome(dedicated, 'redis', signal.SIGTERM,
                            venv=REF_VENV, rust=False)
    # Stessi esiti per il messaggio non confermato (requeue) e per il codice.
    assert rs['ready'] == ref['ready'], (rs, ref)
    assert rs['code'] == ref['code'], (rs, ref)


def test_cold_shutdown_with_running_task(dedicated):
    rs = _shutdown_outcome(dedicated, 'amqp', signal.SIGQUIT)
    ref = _shutdown_outcome(dedicated, 'amqp', signal.SIGQUIT,
                            venv=REF_VENV, rust=False)
    assert rs['ready'] == ref['ready'], (rs, ref)


def test_soft_shutdown_with_running_task(dedicated):
    rs = _shutdown_outcome(dedicated, 'redis', signal.SIGTERM,
                           soft_timeout=1)
    ref = _shutdown_outcome(dedicated, 'redis', signal.SIGTERM,
                            soft_timeout=1, venv=REF_VENV, rust=False)
    assert rs['elapsed'] >= 0.9, rs
    assert ref['elapsed'] >= 0.9, ref
    assert rs['ready'] == ref['ready'], (rs, ref)


def test_env_disables_rust_worker(dedicated):
    worker = dedicated('redis', rust=False,
                       env_extra={'CELERY_RS_WORKER': '0'})
    app = rs_app(worker)
    try:
        assert app.send_task('rs_worker.add', args=(4, 5)).get(timeout=20) == 9
    finally:
        app.close()
    assert worker.stats() is None
    assert 'Connected to' in worker.log()


def test_same_scenario_ref_vs_rs_worker(tmp_path):
    """Stesso scenario su worker ref e rs: stessi risultati e stessi eventi."""
    outcomes = {}
    for label, venv in (('ref', REF_VENV), ('rs', RS_VENV)):
        queue = unique(f'rs-diff-{label}')
        worker = start_worker(
            'redis', queues=queue, venv=venv, rust=(label == 'rs'),
            directory=tmp_path, acks_on_failure=False)
        worker.ready()
        receiver = None
        try:
            receiver = start_event_receiver(worker, node_id=f'rs-diff-{label}')
            time.sleep(2.0)
            outcome = run_ref("""
values = [submit('rs_worker.add', (index, 1)).get(timeout=20)
          for index in range(3)]
failure = submit('rs_worker.fail', ('boom',))
try:
    failure.get(timeout=20)
    failed = 'success'
except Exception as exc:
    failed = type(exc).__name__
time.sleep(1.5)
emit({'values': values, 'failed': failed})
""", worker, timeout=120)
            outcome['queue_ready'] = redis_queue_len(worker.db, queue)
            time.sleep(1.0)
            events, stderr = stop_event_receiver(receiver)
            receiver = None
            assert events, stderr[-4000:]
            normalized = sorted(json.dumps(
                {'type': event['type'], 'name': event.get('name'),
                 'state': event.get('state')}, sort_keys=True)
                for event in events
                if event['type'].startswith('task-'))
            outcome['events'] = normalized
        finally:
            if receiver is not None:
                stop_event_receiver(receiver)
            worker.stop()
            if label == 'rs':
                stats = worker.stats()
                assert stats and stats['consumer'] == 'rust'
            worker.cleanup()
        outcomes[label] = outcome
    assert outcomes['ref']['values'] == outcomes['rs']['values']
    assert outcomes['ref']['failed'] == outcomes['rs']['failed'] == 'ValueError'
    assert outcomes['ref']['queue_ready'] == outcomes['rs']['queue_ready'] == 0
    assert outcomes['ref']['events'] == outcomes['rs']['events'], outcomes
