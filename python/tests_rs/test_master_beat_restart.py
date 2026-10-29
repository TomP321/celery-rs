"""Master imports and PSS after reconnection."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]


def run(impl, code):
    env = {k: v for k, v in os.environ.items() if not k.startswith('PYTHON')}
    result = subprocess.run([
        str(ROOT / f'.venv-{impl}/bin/python'), '-c', code],
        cwd=ROOT / 'bench', env=env, capture_output=True, text=True,
        check=True, timeout=30)
    return json.loads(result.stdout)


def test_worker_imports_do_not_load_unused_pool_or_diagnostics():
    code = '''
import json, sys
from celery.apps.worker import Worker
from celery.concurrency.prefork import TaskPool
from celery.concurrency.rs_prefork import RsTaskPool
for name in ('celery.concurrency.asynpool', 'billiard.pool',
             'billiard.queues', 'celery.utils.debug', 'psutil'):
    assert name not in sys.modules, name
print(json.dumps(True))
'''
    assert run('rs', code)


@pytest.mark.parametrize('first', ['class', 'instance'])
def test_lazy_pool_classes_are_real_objects_from_class_and_instance(first):
    code = '''
import json, sys
from celery.concurrency.prefork import TaskPool
assert 'celery.concurrency.asynpool' not in sys.modules
assert 'billiard.pool' not in sys.modules
instance = TaskPool(1)
target = TaskPool if FIRST == 'class' else instance
asynchronous, blocking = target.Pool, target.BlockingPool
from celery.concurrency.asynpool import AsynPool
from billiard.pool import Pool
assert asynchronous is AsynPool
assert blocking is Pool
assert TaskPool.Pool is instance.Pool is AsynPool
assert TaskPool.BlockingPool is instance.BlockingPool is Pool
print(json.dumps(True))
'''.replace('FIRST', repr(first))
    assert run('rs', code)


def test_lazy_public_objects_match_reference():
    code = '''
import json, pickle
from celery.concurrency import prefork
from celery.apps import worker
from celery.utils.debug import cry
from celery.concurrency.asynpool import AsynPool
from billiard.pool import Pool, CLOSE, RUN
assert prefork.TaskPool.Pool is AsynPool
assert prefork.TaskPool.BlockingPool is Pool
assert prefork.AsynPool is AsynPool
assert prefork.BlockingPool is Pool
assert prefork.CLOSE is CLOSE
assert prefork.RUN is RUN
assert worker.cry is cry
dir(prefork)
assert vars(prefork.TaskPool)['Pool'] is AsynPool
assert vars(prefork.TaskPool)['BlockingPool'] is Pool
class Custom(prefork.TaskPool):
    pass
assert Custom.Pool is AsynPool
Custom.Pool = Pool
assert prefork.TaskPool.Pool is AsynPool
names = ('AsynPool', 'BlockingPool', 'CLOSE', 'RUN')
print(json.dumps({
    'exports': [name for name in names if name in dir(prefork)],
    'cry_export': 'cry' in dir(worker),
    'pickle': pickle.dumps(prefork.TaskPool).hex(),
    'class': [prefork.TaskPool.__module__, prefork.TaskPool.__qualname__]}))
'''
    assert run('rs', code) == run('ref', code)


def test_zygote_does_not_preload_unused_redis_backend():
    assert run('rs', '''
import json, sys
from celery import Celery
from celery.concurrency._rs_zygote import _warm_backend_errors
app = Celery('memory-child', broker='memory://', backend='cache+memory://')
_warm_backend_errors(app)
assert 'celery.backends.redis' not in sys.modules
assert 'celery.backends.asynchronous' not in sys.modules
assert 'redis' not in sys.modules
print(json.dumps(True))
''')


@pytest.mark.parametrize('broker,expected', [
    ('redis://127.0.0.1:6390/0', True),
    ('redis://127.0.0.1:6390/15', True),
    ('redis://127.0.0.1:6391/0', False),
    ('redis://other:6390/0', False),
    ('rediss://127.0.0.1:6390/0', False),
    ('amqp://127.0.0.1:6390//', False),
])
def test_backend_reset_only_discards_sockets_on_the_restarted_server(broker, expected):
    from types import SimpleNamespace
    from celery.backends._rs_redis import RsRedis, _reset_on_broker_restart

    client = RsRedis(host='127.0.0.1', port=6390, db=14)
    resets = []
    client.reset = lambda: resets.append(True)
    app = SimpleNamespace(_backend=SimpleNamespace(client=client))
    assert _reset_on_broker_restart(app, broker) is expected
    assert bool(resets) is expected
    app._backend = None
    assert not _reset_on_broker_restart(app, broker)

    class CustomClient(RsRedis):
        def reset(self):
            raise AssertionError('custom client')

    app._backend = SimpleNamespace(client=CustomClient(host='127.0.0.1', port=6390))
    assert not _reset_on_broker_restart(app, broker)
    default = RsRedis(host=None, port=None)
    default.reset = lambda: None
    app._backend = SimpleNamespace(client=default)
    assert _reset_on_broker_restart(app, 'redis://localhost/15')


@pytest.mark.parametrize('broker', ['redis', 'amqp'])
def test_restart_pss_is_bounded(broker, tmp_path):
    if os.environ.get('CELERY_RS_BROKER_LOCKED') != '1':
        pytest.skip('requires an exclusive tools/dev/with-brokers lane')
    sys.path.insert(0, str(ROOT / 'bench'))
    try:
        spec = importlib.util.spec_from_file_location(
            'bench_restart', ROOT / 'bench/bench_restart.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        report = module.measure('rs', broker, tmp_path / 'restart.json')
    finally:
        sys.path.pop(0)
    initial = report['samples'][0]
    assert report['task_errors'] == 0
    assert 'psutil' not in initial['modules']
    assert 'celery.utils.debug' not in initial['modules']
    assert 'celery.concurrency.asynpool' not in initial['modules']
    assert not any(name == 'billiard.pool' or name == 'billiard.queues'
                   for name in initial['modules'])
    previous = initial
    for sample in report['samples'][1:]:
        assert sample['total_kib'] - previous['total_kib'] <= 5 * 1024
        assert set(sample['modules']) == set(initial['modules'])
        assert {p['pid'] for p in sample['processes']} == {
            p['pid'] for p in initial['processes']}
        previous = sample
