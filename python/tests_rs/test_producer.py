"""Producer Rust: percorso veloce, retry, fork e concorrenza.

Eseguiti con ``.venv-rs`` (pacchetto celery-rs completo) su Redis
``127.0.0.1:6390``; il broker è configurabile con ``CELERY_RS_TEST_REDIS``.
"""
from __future__ import annotations

import os
import threading
import uuid

import pytest
import redis as redis_lib
from kombu import Connection as KombuConnection
from kombu import Producer as KombuProducer
from kombu.exceptions import OperationalError

from celery import Celery
from celery.app import _rs_producer
from celery.app._rs_producer import FastProducerPool, RsProducer, use_fast_producer

REDIS_URL = os.environ.get('CELERY_RS_TEST_REDIS', 'redis://127.0.0.1:6390')
AMQP_URL = os.environ.get('CELERY_RS_TEST_AMQP', 'amqp://guest:guest@127.0.0.1:5673//')


def with_db(url, db):
    """Stesso broker su un altro database Redis."""
    base = url.split('?', 1)[0].rstrip('/')
    head, _, tail = base.rpartition('/')
    if tail.isdigit():
        base = head
    return f'{base}/{db}'


BACKEND_URL = with_db(REDIS_URL, 1)
UNREACHABLE = 'redis://127.0.0.1:6399/0'


def unique(prefix):
    return f'{prefix}.{uuid.uuid4().hex}'


def redis_client(url=REDIS_URL):
    return redis_lib.Redis.from_url(url)


def make_app(queue=None, **conf):
    """App con una coda dedicata e senza result backend sul percorso caldo."""
    queue = queue or unique('producer')
    app = Celery('rs-producer', broker=REDIS_URL, backend=BACKEND_URL,
                 set_as_current=False)
    app.conf.update(
        task_default_queue=queue,
        task_ignore_result=True,
        task_publish_retry=True,
        task_publish_retry_policy={
            'max_retries': 1, 'interval_start': 0,
            'interval_step': 0, 'interval_max': 0,
        },
        **conf,
    )

    @app.task(name=unique('rs.add'), ignore_result=True)
    def add(x, y):
        return x + y

    return app, add, queue


def queue_size(queue):
    """Messaggi in coda, come li conta il trasporto virtuale di kombu."""
    conn = KombuConnection(REDIS_URL)
    try:
        channel = conn.default_channel
        return channel._size(queue)
    finally:
        conn.release()


@pytest.fixture(scope='module', autouse=True)
def broker():
    client = redis_client()
    try:
        client.ping()
    except Exception as exc:  # pragma: no cover - infra non disponibile
        pytest.skip(f'Redis non raggiungibile su {REDIS_URL}: {exc}')
    return client


@pytest.fixture(autouse=True)
def clean_stats():
    """I contatori sono globali al processo: i test usano solo differenze."""
    yield


def test_pool_uses_rust_producer_and_counts_publish():
    app, add, queue = make_app()
    pool = app.amqp.producer_pool
    assert isinstance(pool, FastProducerPool)

    before = _rs_producer.stats()
    add.apply_async((1, 2), task_id=str(uuid.uuid4()))
    after = _rs_producer.stats()

    assert after['publishes'] - before['publishes'] == 1
    assert after['connections'] >= before['connections']
    assert queue_size(queue) == 1
    app.close()


def test_producer_exposes_lazy_connection_without_opening_it():
    """Il descrittore di connessione non importa kombu (spec 08b)."""
    app, _, _ = make_app()
    with app.producer_or_acquire() as producer:
        assert isinstance(producer, RsProducer)
        assert isinstance(producer.connection, _rs_producer.FastConnection)
        # Nessun canale kombu finché non lo si chiede esplicitamente.
        assert producer._channel is producer.connection
        assert producer.connection.connected is False
        # La vista kombu esiste solo se qualcuno la usa davvero.
        assert producer.connection._kombu_view is None
    app.close()


def test_wire_scenarios_publish_through_rust(monkeypatch):
    """Gli scenari di `compat wire` usano davvero il percorso Rust."""
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root / 'tools' / 'compat'))
    try:
        from celery_compat import wire_scenarios
    finally:
        sys.path.pop(0)

    def boom(*args, **kwargs):  # pragma: no cover - fallisce solo se chiamata
        raise AssertionError('publish passato da kombu invece che da Rust')

    monkeypatch.setattr(KombuProducer, 'publish', boom)

    app = wire_scenarios.make_app(REDIS_URL, 'redis://127.0.0.1:6390/1')
    try:
        assert isinstance(app.amqp.producer_pool, FastProducerPool)
        before = _rs_producer.stats()['publishes']
        for name in ('delay_basic', 'apply_async_options',
                     'apply_async_compression', 'apply_async_payload_types',
                     'canvas_chain', 'sent_event'):
            wire_scenarios.SCENARIOS[name](app)
        published = _rs_producer.stats()['publishes'] - before
        assert published >= 6
    finally:
        app.close()


def test_retry_matches_kombu_on_unreachable_broker(monkeypatch):
    """Stessa eccezione e stesso numero di tentativi del riferimento."""
    import kombu.utils.functional as functional

    sleeps = []
    monkeypatch.setattr(functional, 'sleep', lambda value: sleeps.append(value))

    policy = {'max_retries': 2, 'interval_start': 0.05,
              'interval_step': 0.05, 'interval_max': 0.05}

    ref_conn = KombuConnection(UNREACHABLE)
    try:
        with pytest.raises(OperationalError) as ref_error:
            KombuProducer(ref_conn).publish(
                {'a': 1}, routing_key='q', retry=True, retry_policy=policy)
    finally:
        ref_conn.release()
    ref_attempts = len(sleeps)

    ours = RsProducer(KombuConnection(UNREACHABLE))
    before = _rs_producer.stats()['retries']
    with pytest.raises(OperationalError) as got_error:
        ours.publish({'a': 1}, routing_key='q', retry=True,
                     retry_policy=policy)
    ours_attempts = _rs_producer.stats()['retries'] - before

    assert type(got_error.value) is type(ref_error.value)
    assert ours_attempts == ref_attempts == 2
    assert str(got_error.value)


def test_retry_disabled_raises_original_error():
    ours = RsProducer(KombuConnection(UNREACHABLE))
    with pytest.raises(ConnectionError):
        ours.publish({'a': 1}, routing_key='q')


def test_after_fork_child_publishes_and_parent_keeps_working():
    app, add, queue = make_app()
    try:
        add.apply_async((1, 1), task_id=str(uuid.uuid4()))
        pid = os.fork()
        if pid == 0:  # pragma: no cover - processo figlio
            code = 0
            try:
                add.apply_async((2, 2), task_id=str(uuid.uuid4()))
                if _rs_producer.stats()['forks'] < 1:
                    code = 2
            except BaseException:
                code = 1
            finally:
                os._exit(code)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0

        add.apply_async((3, 3), task_id=str(uuid.uuid4()))
        assert queue_size(queue) == 3
    finally:
        app.close()


def _child_publish(broker, queue):
    from celery import Celery

    app = Celery('rs_fork', broker=broker, set_as_current=False)
    app.conf.update(task_default_queue=queue, task_ignore_result=True)

    @app.task(name='rs_fork.add', ignore_result=True)
    def add(x, y):
        return x + y

    add.apply_async((1, 1), task_id=str(uuid.uuid4()))
    app.close()


def test_prefork_pool_child_publishes():
    billiard = pytest.importorskip('billiard')
    app, add, queue = make_app()
    try:
        # Il padre crea una connessione Rust prima del fork.
        add.apply_async((1, 1), task_id=str(uuid.uuid4()))
        with billiard.Pool(processes=1) as pool:
            pool.apply(_child_publish, (REDIS_URL, queue))
        assert queue_size(queue) == 2
    finally:
        app.close()


def test_concurrent_publish_from_sixteen_threads():
    app, add, queue = make_app()
    errors = []
    barrier = threading.Barrier(16)

    def work(index):
        try:
            barrier.wait(10)
            for value in range(25):
                add.apply_async((index, value),
                                task_id=str(uuid.uuid4()))
        except BaseException as exc:  # pragma: no cover - solo su errore
            errors.append(exc)

    threads = [threading.Thread(target=work, args=(index,)) for index in range(16)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)
        assert not errors
        assert queue_size(queue) == 16 * 25
    finally:
        app.close()


def test_env_disables_fast_producer(monkeypatch):
    monkeypatch.setenv('CELERY_RS_PRODUCER', '0')
    app, add, queue = make_app()
    try:
        assert not isinstance(app.amqp.producer_pool, FastProducerPool)
        before = _rs_producer.stats()['publishes']
        add.apply_async((1, 2), task_id=str(uuid.uuid4()))
        assert _rs_producer.stats()['publishes'] == before
        assert queue_size(queue) == 1
    finally:
        app.close()


def test_task_repr_maxlevels_unlimited():
    """`task_repr_maxlevels` può essere None o 0 (nessun limite), come nel riferimento."""
    for maxlevels in (None, 0):
        app, _, _ = make_app(task_repr_maxlevels=maxlevels)
        try:
            message = app.amqp.as_task_v2(
                str(uuid.uuid4()), 'rs.repr',
                args=[{'a': ['b', {'c': 'd'}]}],
            )
            assert message.headers['argsrepr'] == "[{'a': ['b', {'c': 'd'}]}]"
        finally:
            app.close()


def test_use_fast_producer_rejects_unsupported_connections():
    assert not use_fast_producer(KombuConnection('memory://'))
    assert not use_fast_producer(KombuConnection(
        REDIS_URL, transport_options={'socket_keepalive': True}))
    assert not use_fast_producer(KombuConnection(
        f'{REDIS_URL};redis://127.0.0.1:6391/0'))
    assert use_fast_producer(KombuConnection(REDIS_URL))
    assert use_fast_producer(KombuConnection(AMQP_URL))


# ---------------------------------------------------------------------------
# 1. memoria: il percorso veloce non importa redis-py / py-amqp
# ---------------------------------------------------------------------------

IMPORT_PROBE = '''
import json, sys
from celery import Celery
app = Celery('imports', broker=sys.argv[1], backend=None)
app.conf.update(task_ignore_result=True, task_publish_retry=False)
app.send_task('x', args=(1,), task_id='00000000-0000-0000-0000-000000000001')
names = ('redis', 'amqp', 'kombu.transport.redis', 'kombu.transport.pyamqp',
         'opentelemetry')
print(json.dumps(sorted(name for name in names if name in sys.modules)))
'''

CLIENT_MODULES = ('redis', 'kombu.transport.redis', 'kombu.transport.pyamqp',
                  'opentelemetry')


@pytest.mark.parametrize('broker', [REDIS_URL, AMQP_URL], ids=['redis', 'amqp'])
def test_fast_path_does_not_import_client_libraries(broker, tmp_path):
    """Nessun import di redis-py/py-amqp sul percorso Rust (sottoprocesso pulito)."""
    import json
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, '-c', IMPORT_PROBE, broker],
        capture_output=True, text=True, cwd=tmp_path, check=True,
        env={k: v for k, v in os.environ.items() if not k.startswith('PYTHON')},
    )
    loaded = set(json.loads(result.stdout.strip().splitlines()[-1]))
    assert not (loaded & set(CLIENT_MODULES)), loaded
    # Spec 08b: il percorso Rust non importa nemmeno py-amqp.
    assert 'amqp' not in loaded


def test_broker_url_always_states_heartbeat_for_amqp():
    app = Celery('hb-url', broker=AMQP_URL, backend=None)
    try:
        connection = _rs_producer.fast_connection(app)
        url = _rs_producer._broker_url(connection)
        # None → 0 esplicito, come py-amqp; lapin poi negozia col server.
        assert 'heartbeat=0' in url
    finally:
        app.close()
    conn = KombuConnection(AMQP_URL, heartbeat=2)
    assert 'heartbeat=2' in _rs_producer._broker_url(conn)


# ---------------------------------------------------------------------------
# 2. prestazioni: la chiave della connessione nativa è calcolata una volta
# ---------------------------------------------------------------------------

def test_native_key_is_computed_once_per_producer(monkeypatch):
    app, add, queue = make_app()
    calls = []
    original = _rs_producer._native_key

    def counted(connection):
        calls.append(connection)
        return original(connection)

    monkeypatch.setattr(_rs_producer, '_native_key', counted)
    try:
        with app.producer_or_acquire() as producer:
            for _ in range(5):
                producer.publish({'x': 1}, routing_key=queue)
            assert len(calls) == 1
            producer.revive(producer.connection)
            producer.publish({'x': 1}, routing_key=queue)
            assert len(calls) == 2
        assert queue_size(queue) == 6
    finally:
        app.close()


# ---------------------------------------------------------------------------
# 3. retry: gli errori di canale non recuperabili non vengono ritentati
# ---------------------------------------------------------------------------

def declare_exchange_with_kombu(name, kind, url=AMQP_URL):
    """Dichiarazione con la libreria di riferimento (py-amqp)."""
    from kombu import Exchange

    conn = KombuConnection(url)
    try:
        channel = conn.default_channel
        Exchange(name, type=kind, durable=False).declare(channel=channel)
    finally:
        conn.release()


def delete_exchange_with_kombu(name, url=AMQP_URL):
    conn = KombuConnection(url)
    try:
        conn.default_channel.exchange_delete(name)
    finally:
        conn.release()


def test_precondition_failed_matches_kombu_and_is_not_retried():
    pytest.importorskip('amqp')
    import time

    exchange = unique('rev')

    def reference_error():
        delete_exchange_with_kombu(exchange)
        declare_exchange_with_kombu(exchange, 'direct')
        from kombu import Exchange

        conn = KombuConnection(AMQP_URL)
        try:
            channel = conn.default_channel
            Exchange(exchange, type='topic', durable=False).declare(channel=channel)
        except BaseException as exc:
            return exc
        finally:
            conn.release()
        raise AssertionError('il riferimento non ha sollevato')

    ref_exc = reference_error()
    # Messaggio identico: stesso exchange, stessa vhost.
    delete_exchange_with_kombu(exchange)
    topic_app = Celery('rev-topic', broker=AMQP_URL, backend=None,
                       set_as_current=False)
    direct_app = Celery('rev-direct', broker=AMQP_URL, backend=None,
                        set_as_current=False)
    try:
        direct_app.conf.update(
            task_ignore_result=True, task_publish_retry=False,
            task_default_exchange=exchange,
            task_default_exchange_type='direct',
            task_default_queue=unique('revqd'),
        )
        # Prima dichiarazione con il nostro percorso (direct).
        direct_app.send_task('x', args=(1,), task_id=str(uuid.uuid4()))

        topic_app.conf.update(
            task_ignore_result=True,
            task_publish_retry=True,
            task_publish_retry_policy={
                'max_retries': 3, 'interval_start': 0.5,
                'interval_step': 0.5, 'interval_max': 0.5,
            },
            task_default_exchange=exchange,
            task_default_exchange_type='topic',
            task_default_queue=unique('revqt'),
        )
        before = _rs_producer.stats()['retries']
        started = time.monotonic()
        with pytest.raises(type(ref_exc)) as ours:
            topic_app.send_task('x', args=(1,), task_id=str(uuid.uuid4()))
        elapsed = time.monotonic() - started
        assert _rs_producer.stats()['retries'] == before  # nessun retry
        assert elapsed < 0.5
        assert str(ours.value) == str(ref_exc)
        assert ours.value.args == ref_exc.args
    finally:
        direct_app.close()
        topic_app.close()
        delete_exchange_with_kombu(exchange)


# ---------------------------------------------------------------------------
# 4. messaggi di errore identici al riferimento
# ---------------------------------------------------------------------------

def reference_connection_error(url, transport_options=None, connect_timeout=5):
    options = dict(transport_options or {})
    # Come il publish senza retry: una sola connessione, un solo tentativo.
    options.setdefault('max_retries', 0)
    conn = KombuConnection(url, transport_options=options,
                           connect_timeout=connect_timeout)
    try:
        with conn._reraise_as_library_errors():
            KombuProducer(conn).publish({'a': 1}, routing_key='q', retry=False)
    except OperationalError as exc:
        return str(exc)
    finally:
        conn.release()
    raise AssertionError('il riferimento non ha sollevato')


def rust_connection_error(url, transport_options=None, connect_timeout=5):
    app = Celery('msg', broker=url, backend=None, set_as_current=False)
    app.conf.update(
        task_ignore_result=True,
        task_publish_retry=False,
        broker_transport_options=transport_options or {},
        broker_connection_timeout=connect_timeout,
    )
    try:
        with pytest.raises(OperationalError) as ours:
            app.send_task('x', args=(1,), task_id=str(uuid.uuid4()))
        return str(ours.value)
    finally:
        app.close()


@pytest.mark.parametrize('label, url, options, timeout', [
    ('redis-refused', 'redis://127.0.0.1:6399/0', {}, 5),
    ('redis-nohost', 'redis://no-such-host.invalid:6390/0', {}, 5),
    ('amqp-refused', 'amqp://guest:guest@127.0.0.1:5699//', {}, 5),
    ('amqp-nohost', 'amqp://guest:guest@no-such-host.invalid:5673//', {}, 5),
    ('amqp-timeout', 'amqp://guest:guest@192.0.2.1:5673//', {}, 1),
])
def test_connection_error_messages_match_kombu(label, url, options, timeout):
    reference = reference_connection_error(url, options, timeout)
    ours = rust_connection_error(url, options, timeout)
    assert ours == reference, label


# ---------------------------------------------------------------------------
# 5. heartbeat AMQP: connessione viva dopo un periodo di riposo
# ---------------------------------------------------------------------------

RABBIT_API = 'http://127.0.0.1:15673/api'


def rabbit_api(path, method='GET'):
    import base64
    import json
    import urllib.request

    authenticator = 'Basic ' + base64.b64encode(b'guest:guest').decode()
    request = urllib.request.Request(
        f'{RABBIT_API}{path}', method=method,
        headers={'Authorization': authenticator})
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read() or b'null')


def rabbit_connections(name):
    return [c for c in rabbit_api('/connections')
            if c.get('client_properties', {}).get('connection_name') == name]


def rabbit_wait_connection(name, timeout=10):
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        found = rabbit_connections(name)
        if found:
            return found[0]
        time.sleep(0.2)
    raise AssertionError(f'connessione {name!r} non visibile nel management API')


def test_heartbeat_keeps_amqp_connection_alive_while_idle():
    import time

    name = unique('hb')
    connection = KombuConnection(
        AMQP_URL, heartbeat=2, transport_options={'connection_name': name})
    producer = RsProducer(connection)
    try:
        producer.publish({'a': 1}, routing_key=name)
        before = rabbit_wait_connection(name)
        assert before['timeout'] == 2
        connected_at = before['connected_at']

        time.sleep(10)  # 5 intervalli senza pubblicare

        # Senza retry: se la connessione fosse stata chiusa da RabbitMQ
        # fallirebbe subito con "no heartbeat received".
        producer.publish({'a': 1}, routing_key=name)
        after = rabbit_wait_connection(name)
        assert after['connected_at'] == connected_at
        assert after['timeout'] == 2
    finally:
        conn = KombuConnection(AMQP_URL)
        try:
            try:
                conn.default_channel.queue_delete(name)
            except Exception:
                pass
        finally:
            conn.release()


# ---------------------------------------------------------------------------
# 6. connessione morta: scartata anche senza retry
# ---------------------------------------------------------------------------

def test_dead_redis_connection_is_discarded_without_retry():
    # Un database dedicato: la connessione Rust è nuova (chiave URL diversa),
    # quindi il diff di CLIENT LIST identifica solo i suoi socket.
    url = with_db(REDIS_URL, 14)
    queue = unique('killq')
    app = Celery('kill', broker=url, backend=None, set_as_current=False)
    app.conf.update(task_ignore_result=True, task_publish_retry=False,
                    task_default_queue=queue)
    client = redis_client(url)
    try:
        before = {c['id'] for c in client.client_list()}
        app.send_task('x', args=(1,), task_id=str(uuid.uuid4()))
        after = {c['id'] for c in client.client_list()}
        killed = after - before
        assert killed
        for conn_id in killed:
            client.execute_command('CLIENT', 'KILL', 'ID', conn_id)

        errors = 0
        for _ in range(3):
            try:
                app.send_task('x', args=(1,), task_id=str(uuid.uuid4()))
            except OperationalError:
                errors += 1
            else:
                break
        assert errors <= 1
        app.send_task('x', args=(1,), task_id=str(uuid.uuid4()))
        assert client.llen(queue) >= 2
    finally:
        app.close()
        for key in client.keys(queue + '*'):
            client.delete(key)


def test_dead_amqp_connection_is_discarded_without_retry():
    import time
    import urllib.parse

    name = unique('killed')
    queue = unique('killq')
    app = Celery('kill', broker=AMQP_URL, backend=None, set_as_current=False)
    app.conf.update(
        task_ignore_result=True,
        task_publish_retry=False,
        task_default_queue=queue,
        broker_transport_options={'connection_name': name},
    )

    def publish():
        app.send_task('x', args=(1,), task_id=str(uuid.uuid4()))

    try:
        publish()
        connection = rabbit_wait_connection(name)
        rabbit_api('/connections/' + urllib.parse.quote(connection['name'], safe=''),
                   method='DELETE')
        time.sleep(0.3)

        errors = 0
        for _ in range(3):
            try:
                publish()
            except OperationalError:
                errors += 1
            else:
                break
        assert errors <= 1
        publish()  # deve riconnettere
    finally:
        app.close()
        conn = KombuConnection(AMQP_URL)
        try:
            conn.default_channel.queue_delete(queue)
        finally:
            conn.release()

