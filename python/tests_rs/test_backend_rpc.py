"""Backend ``rpc://``: client celery-rs, worker Celery di riferimento.

I test usano RabbitMQ (``127.0.0.1:5673``) e avviano un worker
``.venv-ref/bin/celery worker -P solo``; il client gira in ``.venv-rs`` e deve
consumare i risultati con :class:`RsResultConsumer`, senza aprire connessioni
py-amqp.
"""
from __future__ import annotations

import base64
import json
import os
from urllib.parse import urlsplit
import signal
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import pytest
from kombu.exceptions import ContentDisallowed

from celery import Celery, group, states
from celery.app.task import Context
from celery.backends import rpc as rpc_module
from celery.backends._rs_rpc import RsMessage, RsResultConsumer
from celery.backends.rpc import BacklogLimitExceeded, ResultConsumer
from celery.exceptions import TimeoutError

TESTS_DIR = Path(__file__).resolve().parent
ROOT = TESTS_DIR.parents[1]
REF_PYTHON = ROOT / '.venv-ref' / 'bin' / 'python'
AMQP_URL = os.environ.get('CELERY_RS_AMQP_URL', os.environ.get('CELERY_RS_TEST_AMQP', 'amqp://guest:guest@127.0.0.1:5673//'))
RABBIT_API = os.environ.get('CELERY_RS_RABBIT_API', os.environ.get('CELERY_RS_TEST_RABBIT_API', 'http://127.0.0.1:15673/api'))


def unique(prefix):
    return f'{prefix}.{uuid.uuid4().hex}'


def rabbit_api(path, method='GET'):
    authenticator = 'Basic ' + base64.b64encode(b'guest:guest').decode()
    request = urllib.request.Request(
        f'{RABBIT_API}{path}', method=method,
        headers={'Authorization': authenticator})
    with urllib.request.urlopen(request, timeout=10) as response:
        payload = response.read()
        return json.loads(payload) if payload else None


def rabbit_connections(connection_name):
    return [
        connection for connection in rabbit_api('/connections')
        if connection.get('client_properties', {}).get('connection_name')
        == connection_name
    ]


def wait_connections(connection_name, minimum=1, timeout=10):
    """Le connessioni compaiono nel management API con un piccolo ritardo."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        connections = rabbit_connections(connection_name)
        if len(connections) >= minimum:
            return connections
        time.sleep(0.1)
    return rabbit_connections(connection_name)


def close_connections(connection_name, minimum=1, wait=True):
    connections = (wait_connections(connection_name, minimum=minimum,
                                    timeout=10)
                   if wait else rabbit_connections(connection_name))
    for connection in connections:
        name = urllib.parse.quote(connection['name'], safe='')
        rabbit_api(f'/connections/{name}', method='DELETE')
    return len(connections)


def delete_queue(queue):
    name = urllib.parse.quote(queue, safe='')
    try:
        rabbit_api(f'/queues/%2F/{name}', method='DELETE')
    except Exception:
        pass


@pytest.fixture(scope='module', autouse=True)
def broker():
    for host, port in ((urlsplit(os.environ.get('CELERY_RS_AMQP_URL', 'amqp://guest:guest@127.0.0.1:5673//')).hostname, urlsplit(os.environ.get('CELERY_RS_AMQP_URL', 'amqp://guest:guest@127.0.0.1:5673//')).port), (urlsplit(os.environ.get('CELERY_RS_RABBIT_API', 'http://127.0.0.1:15673/api')).hostname, urlsplit(os.environ.get('CELERY_RS_RABBIT_API', 'http://127.0.0.1:15673/api')).port)):
        sock = socket.socket()
        sock.settimeout(1)
        try:
            sock.connect((host, port))
        except OSError as exc:  # pragma: no cover - infrastruttura assente
            pytest.skip(f'broker non raggiungibile su {host}:{port}: {exc}')
        finally:
            sock.close()


class Worker:
    def __init__(self, process, queue, nodename, env):
        self.process = process
        self.queue = queue
        self.nodename = nodename
        self.env = env


def _worker_env(queue):
    env = {k: v for k, v in os.environ.items()
           if not k.startswith('PYTHON')}
    env['CELERY_RS_TEST_RPC_QUEUE'] = queue
    env['PYTHONPATH'] = str(TESTS_DIR)
    env['CELERY_RS_TEST_AMQP'] = AMQP_URL
    return env


def _ping(env, nodename):
    result = subprocess.run(
        [str(REF_PYTHON), '-m', 'celery', '-A', 'rpc_worker_app',
         'inspect', 'ping', '-d', nodename, '-t', '1'],
        cwd=TESTS_DIR, env=env, capture_output=True, text=True)
    return 'pong' in result.stdout


@pytest.fixture(scope='module')
def worker(broker):
    queue = unique('rs.rpc.tasks')
    nodename = unique('rs-rpc-worker') + '@localhost'
    env = _worker_env(queue)
    process = subprocess.Popen(
        [str(REF_PYTHON), '-m', 'celery', '-A', 'rpc_worker_app',
         'worker', '-P', 'solo', '-l', 'warning', '-Q', queue,
         '-n', nodename, '--without-gossip', '--without-mingle'],
        cwd=TESTS_DIR, env=env, stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE, start_new_session=True)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if process.poll() is not None:
            error = process.stderr.read().decode(errors='replace')
            raise RuntimeError(f'worker terminato: {error[-2000:]}')
        if _ping(env, nodename):
            break
        time.sleep(0.5)
    else:  # pragma: no cover - worker lento o non avviato
        os.killpg(process.pid, signal.SIGTERM)
        raise RuntimeError('worker non pronto')
    instance = Worker(process, queue, nodename, env)
    try:
        yield instance
    finally:
        try:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(30)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        delete_queue(queue)


@pytest.fixture
def app(worker):
    connection_name = unique('rs-rpc-client')
    app = Celery('rs-rpc-client', broker=AMQP_URL, backend='rpc://',
                 set_as_current=False)
    app.conf.update(
        task_default_queue=worker.queue,
        broker_transport_options={'connection_name': connection_name},
        task_serializer='json',
        result_serializer='json',
        accept_content=['json'],
        result_expires=60,
        broker_connection_retry_on_startup=True,
        task_publish_retry_policy={
            'max_retries': 3, 'interval_start': 0.1,
            'interval_step': 0.1, 'interval_max': 0.5,
        },
    )
    app._rs_test_connection_name = connection_name
    try:
        yield app
    finally:
        try:
            app.backend.result_consumer.stop()
        except Exception:
            pass
        app.close()
        close_connections(connection_name, wait=False)


def submit(app, name, args=(), **options):
    return app.send_task(name, args=args, **options)


def direct_result(app, task_id, state, result):
    """Pubblica un risultato direttamente sulla coda di risposta del client."""
    request = Context(reply_to=app.backend.oid, correlation_id=task_id)
    return app.backend.store_result(task_id, result, state, request=request)


def declare_reply_queue(app, task_id):
    with app.producer_or_acquire() as producer:
        app.backend.on_task_call(producer, task_id)


# ---------------------------------------------------------------------------
# messaggio Rust compatibile con kombu.Message
# ---------------------------------------------------------------------------

class _FakeConsumer:
    _rs_connection = None


def test_rs_message_decodes_json_and_honours_accept():
    raw = (
        b'{"result": {"__type__": "datetime",'
        b' "__value__": "2026-10-24T10:00:00"}}',
        {},
        {'content_type': 'application/json', 'content_encoding': 'utf-8'},
        {},
        1,
    )
    message = RsMessage(_FakeConsumer(), raw, accept={'application/json'})
    assert message.payload['result'].isoformat() == '2026-10-24T10:00:00'
    assert message.content_type == 'application/json'
    assert message.delivery_info == {}

    denied = RsMessage(_FakeConsumer(), raw, accept={'application/x-pickle'})
    with pytest.raises(ContentDisallowed):
        denied.payload


# ---------------------------------------------------------------------------
# consumer Rust e assenza di py-amqp
# ---------------------------------------------------------------------------

def test_consumer_is_rust_and_no_pyamqp_connection(app):
    pyamqp_loaded = 'kombu.transport.pyamqp' in sys.modules
    result = submit(app, 'rs_rpc.add', (2, 3))
    assert result.get(timeout=15) == 5

    consumer = app.backend.result_consumer
    assert isinstance(consumer, RsResultConsumer)
    # Il consumer non ha istanziato il trasporto py-amqp di kombu (se un
    # altro test non lo aveva già importato nel processo).
    if not pyamqp_loaded:
        assert 'kombu.transport.pyamqp' not in sys.modules

    connections = wait_connections(
        app._rs_test_connection_name, minimum=2)
    assert connections, 'connessioni del client non visibili'
    products = {connection.get('client_properties', {}).get('product')
                for connection in connections}
    assert 'py-amqp' not in products, products
    assert 'lapin' in products, products
    # Una connessione lapin per il producer e una per il consumer.
    lapin = [connection for connection in connections
             if connection.get('client_properties', {}).get('product')
             == 'lapin']
    assert len(lapin) == 2, lapin
    result.forget()


def test_get_propagates_exception(app):
    result = submit(app, 'rs_rpc.fail', ('boom rpc',))
    with pytest.raises(ValueError, match='boom rpc'):
        result.get(timeout=15)


def test_many_tasks_in_flight(app):
    results = [submit(app, 'rs_rpc.add', (index, 1)) for index in range(8)]
    assert [result.get(timeout=20) for result in results] == list(range(1, 9))


def test_resultset_join_native_and_iter_native(app):
    canvas = group(
        app.signature('rs_rpc.add', args=(index, 1)) for index in range(10))
    result = canvas.apply_async()
    joined = result.join_native(timeout=20)
    assert sorted(joined) == list(range(1, 11))

    native = dict(result.iter_native(timeout=20))
    assert set(native) == {node.id for node in result.results}
    assert sorted(meta['result'] for meta in native.values()) == list(
        range(1, 11))


def test_group_get_and_intermediate_states(app):
    messages = []
    result = submit(app, 'rs_rpc.progress', (2, 4, 5))
    assert result.get(timeout=20, on_message=messages.append) == 9
    states_seen = [message['status'] for message in messages]
    assert 'PROGRESS' in states_seen
    assert states_seen[-1] == states.SUCCESS

    canvas = group(
        app.signature('rs_rpc.add', args=(index, index)) for index in range(3))
    assert sorted(canvas.apply_async().get(timeout=20)) == [0, 2, 4]


def test_forget_clears_cache_and_pending_messages(app):
    result = submit(app, 'rs_rpc.add', (5, 6))
    assert result.get(timeout=15) == 11
    assert result._cache['result'] == 11
    result.forget()
    assert result._cache is None
    assert app.backend.get_task_meta(result.id)['status'] == states.PENDING


def test_timeout_matches_celery(app):
    result = app.AsyncResult(str(uuid.uuid4()))
    with pytest.raises(TimeoutError) as error:
        result.get(timeout=0.4)
    assert str(error.value) == 'The operation timed out.'
    assert error.value.args == ('The operation timed out.',)


# ---------------------------------------------------------------------------
# basic.get, backlog e BacklogLimitExceeded
# ---------------------------------------------------------------------------

def test_get_task_meta_with_backlog(app):
    task_id = str(uuid.uuid4())
    declare_reply_queue(app, task_id)
    direct_result(app, task_id, states.STARTED, None)
    direct_result(app, task_id, 'PROGRESS', {'step': 1})
    direct_result(app, task_id, states.SUCCESS, 42)

    meta = app.backend.get_task_meta(task_id)
    assert meta['status'] == states.SUCCESS
    assert meta['result'] == 42
    # La cache serve i poll successivi senza tornare sulla coda.
    assert app.backend.get_task_meta(task_id)['result'] == 42


def test_get_task_meta_backlog_limit(app):
    task_id = str(uuid.uuid4())
    declare_reply_queue(app, task_id)
    direct_result(app, task_id, states.STARTED, None)
    with pytest.raises(BacklogLimitExceeded):
        app.backend.get_task_meta(task_id, backlog_limit=1)


# ---------------------------------------------------------------------------
# riconnessione e fork
# ---------------------------------------------------------------------------

def test_connection_closed_by_management_api_is_recovered(app, tmp_path):
    import threading

    trigger = tmp_path / 'release'
    result = submit(app, 'rs_rpc.wait_file', (str(trigger), 6, 7))
    backend = app.backend
    reconnected = threading.Event()

    def kill_consumer_connection():
        consumer = backend.result_consumer
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if getattr(consumer, '_rs_connection', None) is not None:
                break
            time.sleep(0.05)
        before = consumer._rs_reconnects
        # Due connessioni: producer e consumer di risultati. Il management API
        # può mostrare la prima prima della seconda: attendere entrambe.
        assert close_connections(app._rs_test_connection_name, minimum=2) >= 2
        # Il worker aspetta il file: rilasciarlo solo dopo la riconnessione.
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if consumer._rs_reconnects > before:
                reconnected.set()
                trigger.write_text('go')
                return
            time.sleep(0.05)

    thread = threading.Thread(target=kill_consumer_connection)
    thread.start()
    try:
        assert result.get(timeout=40) == 13
    finally:
        thread.join(30)
    assert reconnected.is_set()
    # Il consumer ha ricreato la connessione dopo la caduta.
    assert isinstance(backend.result_consumer, RsResultConsumer)
    assert backend.result_consumer._rs_connection is not None


def _clean_env():
    env = {k: v for k, v in os.environ.items()
           if not k.startswith('PYTHON') and k != 'CELERY_RS_BACKEND'}
    env['PYTHONHASHSEED'] = '0'
    return env


def _run_script(source, queue, timeout=180):
    env = _clean_env()
    env['CELERY_RS_TEST_AMQP'] = AMQP_URL
    env['CELERY_RS_TEST_RPC_QUEUE'] = queue
    result = subprocess.run(
        [sys.executable, '-c', source], cwd=TESTS_DIR, env=env,
        capture_output=True, text=True, timeout=timeout)
    assert result.returncode == 0, (
        f'script figlio fallito ({result.returncode}):\n'
        f'{result.stdout[-4000:]}\n{result.stderr[-8000:]}')


FORK_SCRIPT = r'''
import os
import sys

from celery import Celery

app = Celery(
    'rs-rpc-fork', broker=os.environ['CELERY_RS_TEST_AMQP'],
    backend='rpc://', set_as_current=False)
app.conf.update(
    task_default_queue=os.environ['CELERY_RS_TEST_RPC_QUEUE'],
    accept_content=['json'], result_expires=60)

assert app.send_task('rs_rpc.add', args=(1, 1)).get(timeout=30) == 2
pid = os.fork()
if pid == 0:
    code = 0
    try:
        if app.send_task('rs_rpc.add', args=(20, 22)).get(timeout=30) != 42:
            code = 3
    except BaseException:
        code = 1
    finally:
        os._exit(code)
_, status = os.waitpid(pid, 0)
code = os.waitstatus_to_exitcode(status)
if code == 0:
    assert app.send_task('rs_rpc.add', args=(2, 2)).get(timeout=30) == 4
app.close()
sys.exit(code)
'''


BILLIARD_SCRIPT = r'''
import os

import billiard
from celery import Celery

BROKER = os.environ['CELERY_RS_TEST_AMQP']
QUEUE = os.environ['CELERY_RS_TEST_RPC_QUEUE']


def make_app():
    app = Celery('rs-rpc-billiard', broker=BROKER, backend='rpc://',
                 set_as_current=False)
    app.conf.update(task_default_queue=QUEUE, accept_content=['json'],
                    result_expires=60)
    return app


def child():
    app = make_app()
    try:
        return app.send_task('rs_rpc.add', args=(4, 5)).get(timeout=30)
    finally:
        app.close()


if __name__ == '__main__':
    parent = make_app()
    try:
        assert parent.send_task('rs_rpc.add', args=(1, 1)).get(timeout=30) == 2
        with billiard.Pool(processes=1) as pool:
            assert pool.apply(child) == 9
    finally:
        parent.close()
'''


@pytest.mark.skipif(not hasattr(os, 'fork'), reason='os.fork non disponibile')
def test_fork_child_receives_its_own_results(worker):
    _run_script(FORK_SCRIPT, worker.queue)


def test_billiard_child_receives_its_own_results(worker):
    pytest.importorskip('billiard')
    _run_script(BILLIARD_SCRIPT, worker.queue)


# ---------------------------------------------------------------------------
# attivazione e ritorno al consumer kombu
# ---------------------------------------------------------------------------

def test_env_disables_rust_consumer(app, monkeypatch):
    monkeypatch.setenv('CELERY_RS_BACKEND', '0')
    result = submit(app, 'rs_rpc.add', (3, 4))
    assert result.get(timeout=15) == 7
    consumer = app.backend.result_consumer
    assert type(consumer) is ResultConsumer
    assert not isinstance(consumer, RsResultConsumer)
    assert rpc_module.ResultConsumer is ResultConsumer
