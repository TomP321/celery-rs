"""Dieta degli import (spec 08b): cosa carica davvero ogni processo.

I test girano in sottoprocessi con interprete pulito, sia con il pacchetto
celery-rs (``.venv-rs``) sia con il riferimento (``.venv-ref``), e confrontano
l'elenco dei moduli proibiti nel processo client. Le parti "pigre" verificano
che i nomi pubblici restino gli stessi oggetti di kombu e che i percorsi di
fallback (serializer, ``CELERY_RS_*=0``) continuino a funzionare.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).resolve().parent
ROOT = TESTS_DIR.parents[1]
REF_PYTHON = ROOT / '.venv-ref' / 'bin' / 'python'
RS_PYTHON = ROOT / '.venv-rs' / 'bin' / 'python'

REDIS_URL = 'redis://127.0.0.1:6390/7'
AMQP_URL = 'amqp://guest:guest@127.0.0.1:5673//'
BACKEND_URL = 'redis://127.0.0.1:6390/8'

#: Moduli che il client non deve caricare con producer/backend Rust (spec 08b).
#:
#: Il riferimento li carica già tutti (kombu, py-amqp, billiard, click,
#: greenlet, yaml/msgpack, dateutil...) quando crea l'app: la decisione 08b
#: vuole gli oggetti veri di kombu anche nel client, quindi qui restano solo
#: le librerie che *noi* possiamo evitare: il client del broker (redis-py) e i
#: suoi consumatori si usano solo se qualcuno torna al percorso del riferimento.
CLIENT_FORBIDDEN = (
    'redis', 'kombu.transport.redis', 'kombu.transport.pyamqp',
    'opentelemetry',
)


def clean_env(tmpdir, **extra):
    env = {k: v for k, v in os.environ.items() if not k.startswith('PYTHON')}
    env.update(PYTHONPATH=str(tmpdir), PYTHONHASHSEED='0')
    env.update(extra)
    return env


def run_python(python, code, cwd, env, timeout=180):
    out = subprocess.run(
        [str(python), '-c', code], cwd=cwd, env=env,
        capture_output=True, text=True, timeout=timeout)
    if out.returncode:
        raise AssertionError(out.stderr[-4000:])
    return out.stdout


def loaded_forbidden():
    return [name for name in CLIENT_FORBIDDEN
            if any(m == name or m.startswith(name + '.') for m in sys.modules)]


CLIENT_PROBE = r'''
import json, sys

def forbidden():
    names = %r
    return [name for name in names
            if any(m == name or m.startswith(name + '.') for m in sys.modules)]

steps = {}
import celery
steps['import'] = forbidden()
from client_app import add
steps['app'] = forbidden()
result = add.delay(2, 3)
steps['delay'] = forbidden()
assert result.get(timeout=30) == 5
steps['get'] = forbidden()

from celery import chain, chord, group
group(add.s(1, 2), add.s(3, 4)).apply_async()
chain(add.s(1, 2), add.s(3)).apply_async()
chord([add.s(1, 2)], add.s(3)).apply_async()
steps['canvas'] = forbidden()
print(json.dumps(steps))
''' % (CLIENT_FORBIDDEN,)

APP = '''
from celery import Celery
app = Celery('diet', broker={broker!r}, backend={backend!r})
app.conf.broker_connection_retry_on_startup = True

@app.task
def add(x, y):
    return x + y
'''


@pytest.fixture(scope='module')
def probe_dir():
    with tempfile.TemporaryDirectory() as tmp:
        yield Path(tmp)


def start_ref_worker(directory, broker):
    env = clean_env(directory)
    worker = subprocess.Popen(
        [str(REF_PYTHON), '-m', 'celery', '-A', 'client_app', 'worker',
         '-P', 'solo', '-l', 'warning', '--without-gossip', '--without-mingle'],
        cwd=directory, env=env, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, start_new_session=True)
    deadline = time.time() + 40
    while time.time() < deadline:
        if worker.poll() is not None:
            raise AssertionError('worker del riferimento terminato')
        check = subprocess.run(
            [str(REF_PYTHON), '-m', 'celery', '-A', 'client_app', 'inspect',
             'ping', '-t', '1'],
            cwd=directory, env=env, capture_output=True, text=True)
        if 'pong' in check.stdout:
            return worker
        time.sleep(0.5)
    worker.terminate()
    raise AssertionError('worker del riferimento non pronto')


def stop_worker(worker):
    try:
        os.killpg(worker.pid, signal.SIGTERM)
        worker.wait(30)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            os.killpg(worker.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


@pytest.mark.parametrize('broker', [REDIS_URL, AMQP_URL], ids=['redis', 'amqp'])
def test_client_import_diet(probe_dir, broker):
    """Import, app+task, delay, get e canvas non caricano i moduli proibiti."""
    queue = f'diet-{uuid.uuid4().hex}'
    app_text = APP.format(broker=broker, backend=BACKEND_URL)
    # La coda è unica per non rubare messaggi ad altri agenti.
    app_text = app_text.replace("app.conf.broker_connection_retry_on_startup = True",
                                "app.conf.broker_connection_retry_on_startup = True\n"
                                f"app.conf.task_default_queue = {queue!r}")
    (probe_dir / 'client_app.py').write_text(app_text)
    worker = start_ref_worker(probe_dir, broker)
    try:
        raw = run_python(RS_PYTHON, CLIENT_PROBE, probe_dir, clean_env(probe_dir))
    finally:
        stop_worker(worker)
    steps = json.loads(raw.strip().splitlines()[-1])
    for step, loaded in steps.items():
        assert not loaded, (step, loaded)


def test_client_does_not_import_worker_or_broker_client(probe_dir):
    """L'app non carica `celery.worker` né redis-py/py-amqp con il percorso Rust."""
    code = (
        'import json, sys\n'
        'from celery import Celery\n'
        'app = Celery("nokombu", broker=%r, backend=%r)\n'
        'app.conf.task_default_queue = "diet-apponly"\n'
        'from client_app import add\n'
        'add.delay(1, 2)\n'
        "print(json.dumps(sorted(m for m in sys.modules if m in ("
        "'celery.worker', 'redis', 'kombu.transport.redis', "
        "'kombu.transport.pyamqp'))))\n"
    ) % (REDIS_URL, BACKEND_URL)
    (probe_dir / 'client_app.py').write_text(
        APP.format(broker=REDIS_URL, backend=BACKEND_URL))
    raw = run_python(RS_PYTHON, code, probe_dir, clean_env(probe_dir))
    assert json.loads(raw.strip().splitlines()[-1]) == []


# ---------------------------------------------------------------------------
# nomi pigri: stessi oggetti del riferimento
# ---------------------------------------------------------------------------

def test_lazy_names_are_the_kombu_objects():
    import kombu.log
    import kombu.utils.functional
    import kombu.utils.imports
    import kombu.utils.objects
    import kombu.utils.uuid

    import celery.utils
    import celery.utils.functional
    import celery.utils.imports
    import celery.utils.log
    import celery.utils.threads

    assert celery.utils.cached_property is kombu.utils.objects.cached_property
    assert celery.utils.memoize is kombu.utils.functional.memoize
    from kombu.utils.uuid import uuid as kombu_uuid
    assert celery.utils.uuid is kombu_uuid
    assert celery.utils.gen_unique_id is kombu_uuid
    assert celery.utils.get_cls_by_name is kombu.utils.imports.symbol_by_name
    assert celery.utils.log.LOG_LEVELS is kombu.log.LOG_LEVELS
    import greenlet
    assert celery.utils.threads.get_ident is greenlet.getcurrent
    for module, reference in (
            (celery.utils.functional, kombu.utils.functional),
            (celery.utils.imports, kombu.utils.imports)):
        for name in ('maybe_list', 'is_list', 'symbol_by_name', 'memoize'):
            if hasattr(module, name) and hasattr(reference, name):
                assert getattr(module, name) is getattr(reference, name), name


def test_module_dir_matches_public_names():
    """``dir()`` (nomi pubblici) resta quello del riferimento."""
    public = lambda mod: {n for n in dir(mod) if not n.startswith('_')}  # noqa: E731
    import celery.utils
    assert {'cached_property', 'gen_unique_id', 'memoize', 'uuid',
            'LOG_LEVELS', 'get_cls_by_name'} <= public(celery.utils)
    import celery.utils.functional as functional
    assert {'LRUCache', 'lazy', 'maybe_list', 'memoize', 'dictfilter'} <= public(functional)


def test_mlazy_pickle_roundtrip():
    import pickle

    from celery.utils.functional import lazy, mlazy

    holder = mlazy(int, '42')
    restored = pickle.loads(pickle.dumps(holder))
    assert type(restored) is type(holder)
    assert restored() == 42
    # `mlazy` estende davvero la lazy di kombu (stessa classe, non un nome
    # riscritto); il nome pubblico `lazy` resta l'oggetto di kombu.
    import kombu.utils.functional
    assert issubclass(mlazy, kombu.utils.functional.lazy)
    assert mlazy.__mro__[1] is kombu.utils.functional.lazy
    assert lazy is kombu.utils.functional.lazy


# ---------------------------------------------------------------------------
# fallback: serialer espliciti e CELERY_RS_*=0
# ---------------------------------------------------------------------------

FALLBACK_PROBE = r'''
import json, sys
from celery import Celery
app = Celery('fallback', broker=%r, backend=%r)
app.conf.task_serializer = %r
app.conf.broker_connection_retry_on_startup = True

@app.task
def add(x, y):
    return x + y

add.delay(2, 3)
print(json.dumps(sorted(m for m in sys.modules if m in
                        ('yaml', 'msgpack', 'kombu.connection',
                         'kombu.serialization', 'redis'))))
'''


@pytest.mark.parametrize('serializer, expected', [
    ('yaml', {'yaml', 'kombu.serialization'}),
    ('pickle', {'kombu.serialization'}),
    ('msgpack', {'msgpack', 'kombu.serialization'}),
])
def test_explicit_serializer_uses_kombu(probe_dir, serializer, expected):
    env = clean_env(probe_dir)
    raw = run_python(
        RS_PYTHON,
        FALLBACK_PROBE % (REDIS_URL, BACKEND_URL, serializer),
        probe_dir, env)
    assert expected <= set(json.loads(raw.strip().splitlines()[-1]))


def test_producer_disabled_returns_to_reference_percorso(probe_dir):
    env = clean_env(probe_dir, CELERY_RS_PRODUCER='0')
    raw = run_python(
        RS_PYTHON,
        FALLBACK_PROBE % (REDIS_URL, BACKEND_URL, 'json'),
        probe_dir, env)
    loaded = set(json.loads(raw.strip().splitlines()[-1]))
    assert 'kombu.connection' in loaded


def test_backend_disabled_returns_to_redis_py(probe_dir):
    code = (
        'import sys\n'
        'from client_app import add\n'
        'add.delay(1, 2).get(timeout=30)\n'
        "print('redis' in sys.modules)\n"
    )
    (probe_dir / 'client_app.py').write_text(
        APP.format(broker=REDIS_URL, backend=BACKEND_URL))
    worker = start_ref_worker(probe_dir, REDIS_URL)
    try:
        env = clean_env(probe_dir, CELERY_RS_BACKEND='0')
        raw = run_python(RS_PYTHON, code, probe_dir, env)
    finally:
        stop_worker(worker)
    assert raw.strip().splitlines()[-1] == 'True'


# ---------------------------------------------------------------------------
# worker: il master non importa redis-py
# ---------------------------------------------------------------------------

def test_worker_master_does_not_import_redis_py(probe_dir):
    state = probe_dir / 'worker-state.json'
    (probe_dir / 'client_app.py').write_text(
        APP.format(broker=REDIS_URL, backend=BACKEND_URL))
    env = clean_env(probe_dir, CELERY_RS_WORKER_STATE=str(state))
    worker = subprocess.Popen(
        [str(RS_PYTHON), '-m', 'celery', '-A', 'client_app', 'worker',
         '-P', 'solo', '-l', 'warning', '--without-gossip', '--without-mingle'],
        cwd=probe_dir, env=env, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, start_new_session=True)
    try:
        deadline = time.time() + 40
        while time.time() < deadline and not state.exists():
            if worker.poll() is not None:
                raise AssertionError('worker terminato prima di scrivere lo stato')
            time.sleep(0.2)
        payload = json.loads(state.read_text())
        loaded = payload['loaded']
        assert payload['consumer'] == 'rust'
        # `greenlet`/`dateutil` sono caricati anche dal riferimento (bootsteps
        # e celery.result); quello che il percorso Rust evita è il client del
        # broker e i trasporti kombu.
        assert not [m for m in loaded
                    if m == 'redis' or m.startswith(
                        ('redis.', 'kombu.transport.redis',
                         'kombu.transport.pyamqp'))]
    finally:
        stop_worker(worker)
