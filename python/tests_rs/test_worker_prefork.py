"""Compare Rust and reference prefork workers, including limits, failures and shutdown."""
from __future__ import annotations

import os
import signal
import tempfile
import time
from pathlib import Path

import pytest

from test_worker_consumer import (
    BROKERS,
    REF_VENV,
    RS_VENV,
    broker_available,
    kill_redis_consumer,
    rabbit_kill_consumer,
    redis_client,
    redis_queue_len,
    rs_app,
    run_ref,
    start_worker,
    submit,
)

TIMEOUT = 30



def children_of(pid):
    out = []
    try:
        for thread in os.listdir(f'/proc/{pid}/task'):
            with open(f'/proc/{pid}/task/{thread}/children') as handle:
                out += [int(value) for value in handle.read().split()]
    except OSError:
        pass
    return out


def pool_children(worker):
    """Return pool child pids, using the zygote's children for Rust workers."""
    direct = children_of(worker.process.pid)
    if worker.rust:
        descendants = []
        for zygote in direct:
            descendants += children_of(zygote)
        return descendants
    return direct


def wait_until(predicate, timeout=TIMEOUT, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return predicate()


def start_prefork(broker_name='redis', venv=RS_VENV, rust=True, **kwargs):
    kwargs.setdefault('concurrency', 4)
    worker = start_worker(broker_name, pool='prefork', venv=venv, rust=rust,
                          **kwargs)
    worker.ready()
    return worker


def pool_info(worker):
    """Read the pool section of ``inspect stats``."""
    import json
    import re
    output = worker.inspect('stats', 5).stdout
    match = re.search(r'"pool": (\{.*?\n        \})', output, re.S)
    if not match:
        return {}
    try:
        return json.loads(match.group(1))
    except ValueError:
        return {}


def result_outcome(result, timeout=TIMEOUT):
    try:
        return ('ok', result.get(timeout=timeout))
    except Exception as exc:  # noqa: BLE001 - compare exception types
        return (type(exc).__name__, None)



@pytest.fixture(scope='module', autouse=True)
def require_brokers():
    missing = [name for name in BROKERS if not broker_available(name)]
    if missing:
        pytest.skip(f'brokers unreachable: {missing}')


@pytest.fixture(scope='module')
def workers(tmp_path_factory):
    directory = tmp_path_factory.mktemp('prefork-workers')
    started = {}
    for name in BROKERS:
        if broker_available(name):
            started[name] = start_prefork(
                name, directory=directory / name, full=True)
    yield started
    for worker in started.values():
        worker.stop()
        worker.cleanup()


@pytest.fixture(params=list(BROKERS))
def worker(request, workers):
    name = request.param
    if name not in workers:
        pytest.skip(f'broker {name} unreachable')
    return workers[name]


@pytest.fixture
def dedicated(tmp_path):
    started = []

    def factory(broker_name='redis', **kwargs):
        worker = start_prefork(broker_name, directory=tmp_path, **kwargs)
        started.append(worker)
        return worker

    yield factory
    for worker in started:
        worker.stop(signal.SIGKILL)
        worker.cleanup()



def test_worker_uses_rust_consumer_and_pool(worker):
    result = run_ref('''
r = submit('rs_worker.add', (2, 3))
emit({'value': r.get(timeout=15)})
''', worker)
    assert result['value'] == 5

    stats = worker.wait_stats(lambda value: value.get('messages', 0) >= 1)
    assert stats is not None and stats['consumer'] == 'rust'

    info = pool_info(worker)
    assert info, worker.inspect('stats').stdout
    assert info['implementation'] == (
        'celery.concurrency.rs_prefork:RsTaskPool')
    assert info['max-concurrency'] == 4
    assert len(info['processes']) == 4, info
    for key in ('max-tasks-per-child', 'put-guarded-by-semaphore',
                'timeouts', 'writes'):
        assert key in info, info

    # The zygote, not the master, forks pool children.
    direct = children_of(worker.process.pid)
    assert len(direct) == 1, direct
    assert wait_until(lambda: len(pool_children(worker)) == 4), \
        pool_children(worker)


def test_same_scenarios_as_solo_match_reference(dedicated):
    scenario = '''
values = [submit('rs_worker.add', (i, 10)).get(timeout=20) for i in range(3)]
failure = submit('rs_worker.fail', ('boom',))
try:
    failure.get(timeout=20)
    failed = 'success'
except Exception as exc:
    failed = type(exc).__name__
retry = submit('rs_worker.retry', (7,)).get(timeout=20)
countdown = submit('rs_worker.add', (1, 1), countdown=0.5).get(timeout=20)
late = submit('rs_worker.acks_late_slow', (0.1, 4)).get(timeout=20)
emit({'values': values, 'failed': failed, 'retry': retry,
      'countdown': countdown, 'late': late})
'''
    outcomes = {}
    for label, venv, rust in (('rs', RS_VENV, True), ('ref', REF_VENV, False)):
        worker = dedicated(venv=venv, rust=rust, acks_on_failure=False)
        outcomes[label] = run_ref(scenario, worker, timeout=120)
    assert outcomes['rs'] == outcomes['ref'] == {
        'values': [10, 11, 12],
        'failed': 'ValueError',
        'retry': 7,
        'countdown': 2,
        'late': 4,
    }


def test_time_limits_match_reference(dedicated):
    scenario = '''
hard = submit('rs_worker.sleep_add', (3, 1, 1), time_limit=1)
try:
    hard.get(timeout=20)
    hard_out = 'success'
except Exception as exc:
    hard_out = type(exc).__name__
soft = submit('rs_worker.sleep_add', (3, 1, 1), soft_time_limit=0.5)
try:
    soft.get(timeout=20)
    soft_out = 'success'
except Exception as exc:
    soft_out = type(exc).__name__
emit({'hard': hard_out, 'soft': soft_out})
'''
    outcomes = {}
    for label, venv, rust in (('rs', RS_VENV, True), ('ref', REF_VENV, False)):
        worker = dedicated(venv=venv, rust=rust)
        outcomes[label] = run_ref(scenario, worker, timeout=120)
    assert outcomes['rs'] == outcomes['ref'] == {
        'hard': 'TimeLimitExceeded', 'soft': 'SoftTimeLimitExceeded'}



def _pids_scenario(worker, task, args, count):
    app = rs_app(worker)
    try:
        return [app.send_task(task, args=args).get(timeout=30)
                for _ in range(count)]
    finally:
        app.close()


def test_max_tasks_per_child_matches_reference(dedicated):
    counts = {}
    for label, venv, rust in (('rs', RS_VENV, True), ('ref', REF_VENV, False)):
        worker = dedicated(venv=venv, rust=rust,
                           args=('--max-tasks-per-child=1',))
        pids = _pids_scenario(worker, 'rs_worker.pid', (), 6)
        # One task per child must produce a new pid for each execution.
        counts[label] = len(set(pids))
        assert wait_until(lambda: len(pool_children(worker)) == 4), \
            pool_children(worker)
    assert counts['rs'] == counts['ref'] == 6, counts


def test_max_memory_per_child_matches_reference(dedicated):
    counts = {}
    for label, venv, rust in (('rs', RS_VENV, True), ('ref', REF_VENV, False)):
        worker = dedicated(venv=venv, rust=rust,
                           args=('--max-memory-per-child=1024',))
        pids = _pids_scenario(worker, 'rs_worker.allocate', (2,), 4)
        counts[label] = len({pid for pid, _size in pids})
        assert wait_until(lambda: len(pool_children(worker)) == 4), \
            pool_children(worker)
    assert counts['rs'] == counts['ref'] == 4, counts


def test_autoscale_grows_and_shrinks(dedicated):
    outcomes = {}
    for label, venv, rust in (('rs', RS_VENV, True), ('ref', REF_VENV, False)):
        worker = dedicated(venv=venv, rust=rust, concurrency=1,
                           args=('--autoscale=4,1',),
                           env_extra={'AUTOSCALE_KEEPALIVE': '2'})
        app = rs_app(worker)
        try:
            results = [app.send_task('rs_worker.sleep_add', args=(1.2, i, 1))
                       for i in range(4)]
            peak = wait_until(lambda: (
                len(pool_children(worker))
                if len(pool_children(worker)) >= 4 else None), timeout=15)
            values = [result.get(timeout=30) for result in results]
            low = wait_until(lambda: (
                True if len(pool_children(worker)) <= 1 else None),
                timeout=25)
        finally:
            app.close()
        outcomes[label] = {
            'values': values, 'peak': peak or len(pool_children(worker)),
            'shrunk': bool(low),
        }
    assert outcomes['rs']['values'] == outcomes['ref']['values'] == [1, 2, 3, 4]
    assert outcomes['rs']['peak'] >= 4, outcomes
    assert outcomes['ref']['peak'] >= 4, outcomes
    assert outcomes['rs']['shrunk'] and outcomes['ref']['shrunk'], outcomes



def test_revoke_terminate_matches_reference(dedicated):
    outcomes = {}
    for label, venv, rust in (('rs', RS_VENV, True), ('ref', REF_VENV, False)):
        worker = dedicated(venv=venv, rust=rust)
        directory = Path(tempfile.mkdtemp(prefix='rs-prefork-revoke-'))
        marker = directory / 'started'
        app = rs_app(worker)
        try:
            result = app.send_task('rs_worker.marker_slow',
                                   args=(str(marker), 8, 5))
            assert worker.wait_started('rs_worker.marker_slow', timeout=20)
            assert wait_until(marker.exists, timeout=10)
            app.control.revoke(result.id, terminate=True, signal='SIGKILL',
                               reply=True, timeout=5)
            state = result.state
            outcome = result_outcome(result, timeout=10)
            # The worker must survive and accept more tasks.
            alive = app.send_task('rs_worker.add', args=(1, 2)).get(timeout=20)
        finally:
            app.close()
        outcomes[label] = {'state': state, 'outcome': outcome, 'alive': alive}
    assert outcomes['rs'] == outcomes['ref'], outcomes
    assert outcomes['rs']['outcome'][0] == 'TaskRevokedError'
    assert outcomes['rs']['alive'] == 3


def _kill_children_scenario(dedicated, task, args, timeout=TIMEOUT):
    worker = dedicated()
    app = rs_app(worker)
    try:
        result = app.send_task(task, args=args)
        assert worker.wait_started(task, timeout=20)
        time.sleep(0.3)
        victims = pool_children(worker)
        assert len(victims) == 4, victims
        for pid in victims:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        outcome = result_outcome(result, timeout=timeout)
        # The live zygote lets the supervisor replace children.
        assert wait_until(lambda: len(pool_children(worker)) == 4, timeout=20)
        ready = redis_queue_len(worker.db, worker.queue)
        return {'outcome': outcome, 'ready': ready}
    finally:
        app.close()


def test_child_killed_reports_worker_lost(dedicated):
    outcomes = {}
    for label, venv, rust in (('rs', RS_VENV, True), ('ref', REF_VENV, False)):
        directory = Path(tempfile.mkdtemp(prefix='rs-prefork-lost-'))
        outcomes[label] = _kill_children_scenario(
            lambda **kwargs: dedicated(venv=venv, rust=rust, **kwargs),
            'rs_worker.wait_file', (str(directory / 'never'), 30))
    assert outcomes['rs'] == outcomes['ref'], outcomes
    assert outcomes['rs']['outcome'][0] == 'WorkerLostError'
    assert outcomes['rs']['ready'] == 0


def test_child_killed_acks_late_is_redelivered(dedicated):
    outcomes = {}
    for label, venv, rust in (('rs', RS_VENV, True), ('ref', REF_VENV, False)):
        outcomes[label] = _kill_children_scenario(
            lambda **kwargs: dedicated(venv=venv, rust=rust, **kwargs),
            'rs_worker.acks_late_slow', (1.5, 9))
    assert outcomes['rs'] == outcomes['ref'], outcomes
    # reject_on_worker_lost must requeue and execute the message again.
    assert outcomes['rs']['outcome'] == ('ok', 9)
    assert outcomes['rs']['ready'] == 0


def test_zygote_lost_shuts_down_with_failure(dedicated):
    worker = dedicated(concurrency=2, loglevel='debug')
    directory = Path(tempfile.mkdtemp(prefix='rs-prefork-zygote-'))
    marker = directory / 'started'
    app = rs_app(worker)
    try:
        app.send_task('rs_worker.marker_slow', args=(str(marker), 30, 5))
        assert worker.wait_started('rs_worker.marker_slow', timeout=20)
        assert wait_until(marker.exists, timeout=10)
        zygote = children_of(worker.process.pid)
        assert len(zygote) == 1, zygote
        os.kill(zygote[0], signal.SIGKILL)
        code = worker.process.wait(timeout=40)
        time.sleep(0.5)
        log = worker.log()
        # acks_late and reject_on_worker_lost return the in-flight message to the broker.
        ready = redis_queue_len(worker.db, worker.queue)
        descendants = pool_children(worker)
    finally:
        app.close()
    assert code != 0, (code, log[-4000:])
    assert 'CRITICAL' in log and 'zygote' in log.lower(), log[-4000:]
    assert 'WorkerLostError' in log, log[-4000:]
    assert ready == 1, ready
    assert descendants == [], descendants



def _shutdown_scenario(dedicated, venv, rust, sig, seconds=3):
    worker = dedicated(venv=venv, rust=rust, acks_on_failure=False)
    directory = Path(tempfile.mkdtemp(prefix='rs-prefork-shutdown-'))
    marker = directory / 'started'
    app = rs_app(worker)
    try:
        result = app.send_task('rs_worker.marker_slow',
                               args=(str(marker), seconds, 7))
        assert worker.wait_started('rs_worker.marker_slow', timeout=20)
        assert wait_until(marker.exists, timeout=10)
        time.sleep(0.3)
        # Match systemd: signal the master rather than the entire process group.
        started = time.monotonic()
        os.kill(worker.process.pid, sig)
        code = worker.process.wait(timeout=45)
        elapsed = time.monotonic() - started
        outcome = result_outcome(result, timeout=5)
        time.sleep(0.3)
        ready = redis_queue_len(worker.db, worker.queue)
        return {'code': code, 'outcome': outcome, 'ready': ready,
                'elapsed': elapsed}
    finally:
        app.close()


def test_warm_shutdown_completes_running_task(dedicated):
    outcomes = {}
    for label, venv, rust in (('rs', RS_VENV, True), ('ref', REF_VENV, False)):
        outcomes[label] = _shutdown_scenario(dedicated, venv, rust,
                                             signal.SIGTERM)
    assert outcomes['rs']['outcome'] == outcomes['ref']['outcome'] == ('ok', 7)
    assert outcomes['rs']['ready'] == outcomes['ref']['ready'] == 0
    assert outcomes['rs']['code'] == outcomes['ref']['code'] == 0


def test_cold_shutdown_requeues_running_task(dedicated):
    outcomes = {}
    for label, venv, rust in (('rs', RS_VENV, True), ('ref', REF_VENV, False)):
        outcomes[label] = _shutdown_scenario(dedicated, venv, rust,
                                             signal.SIGQUIT)
    assert outcomes['rs']['outcome'] == outcomes['ref']['outcome']
    assert outcomes['rs']['outcome'][0] != 'ok'
    assert outcomes['rs']['ready'] == outcomes['ref']['ready'] == 1


def test_soft_shutdown_waits_for_running_task(dedicated):
    outcomes = {}
    for label, venv, rust in (('rs', RS_VENV, True), ('ref', REF_VENV, False)):
        worker = dedicated(venv=venv, rust=rust, acks_on_failure=False,
                           env_extra={'CELERY_RS_TEST_SOFT_SHUTDOWN': '1'})
        directory = Path(tempfile.mkdtemp(prefix='rs-prefork-soft-'))
        marker = directory / 'started'
        app = rs_app(worker)
        try:
            result = app.send_task('rs_worker.marker_slow',
                                   args=(str(marker), 3, 7))
            assert worker.wait_started('rs_worker.marker_slow', timeout=20)
            assert wait_until(marker.exists, timeout=10)
            started = time.monotonic()
            os.kill(worker.process.pid, signal.SIGTERM)
            code = worker.process.wait(timeout=45)
            elapsed = time.monotonic() - started
            outcome = result_outcome(result, timeout=5)
        finally:
            app.close()
        outcomes[label] = {'code': code, 'outcome': outcome,
                           'elapsed': elapsed}
    assert outcomes['rs']['outcome'] == outcomes['ref']['outcome'] == ('ok', 7)
    assert outcomes['rs']['code'] == outcomes['ref']['code'] == 0
    assert outcomes['rs']['elapsed'] >= 0.9, outcomes


def test_cancel_long_running_tasks_on_connection_loss(dedicated):
    outcomes = {}
    for label, venv, rust in (('rs', RS_VENV, True), ('ref', REF_VENV, False)):
        worker = dedicated(venv=venv, rust=rust,
                           env_extra={'CELERY_RS_TEST_CANCEL_LONG': '1'})
        app = rs_app(worker)
        try:
            result = app.send_task('rs_worker.acks_late_slow', args=(10, 5))
            assert worker.wait_started('rs_worker.acks_late_slow', timeout=20)
            time.sleep(0.3)
            assert kill_redis_consumer(worker.db) >= 1
            outcome = result_outcome(result, timeout=45)
            state = result.state
        finally:
            app.close()
        outcomes[label] = {'outcome': outcome, 'state': state}
    assert outcomes['rs'] == outcomes['ref'], outcomes
    assert outcomes['rs']['outcome'] == ('ok', 5)


def test_env_disables_rust_worker_and_pool(dedicated):
    worker = dedicated(rust=False, env_extra={'CELERY_RS_WORKER': '0'})
    assert submit(worker, 'rs_worker.add', (4, 5)).get(timeout=20) == 9
    assert worker.stats() is None
    info = pool_info(worker)
    assert info['implementation'] == 'celery.concurrency.prefork:TaskPool'
    assert 'Connected to' in worker.log()


def test_worker_lifecycle_signals(dedicated):
    outcomes = {}
    for label, venv, rust in (('rs', RS_VENV, True), ('ref', REF_VENV, False)):
        directory = Path(tempfile.mkdtemp(prefix='rs-prefork-signals-'))
        child_log = directory / 'children.log'
        worker_log = directory / 'worker.log'
        worker = dedicated(
            venv=venv, rust=rust,
            env_extra={'CELERY_RS_TEST_CHILD_LIFECYCLE': str(child_log),
                       'CELERY_RS_TEST_WORKER_LIFECYCLE': str(worker_log)})
        assert submit(worker, 'rs_worker.add', (1, 2)).get(timeout=20) == 3
        os.kill(worker.process.pid, signal.SIGTERM)
        worker.process.wait(timeout=45)
        time.sleep(0.5)
        children = child_log.read_text().splitlines() if child_log.exists() else []
        master = worker_log.read_text().splitlines() if worker_log.exists() else []
        outcomes[label] = {
            'init': len([line for line in children if line.startswith('init')]),
            'shutdown': len([line for line in children
                             if line.startswith('shutdown')]),
            'ready': any(line.startswith('ready') for line in master),
            'shutting_down': any(line.startswith('shutting_down')
                                 for line in master),
        }
    assert outcomes['rs'] == outcomes['ref'] == {
        'init': 4, 'shutdown': 4, 'ready': True, 'shutting_down': True,
    }, outcomes



@pytest.mark.parametrize('broker_name', list(BROKERS))
def test_broker_loss_keeps_in_flight_task(dedicated, broker_name):
    if not broker_available(broker_name):
        pytest.skip(f'broker {broker_name} unreachable')
    outcomes = {}
    for label, venv, rust in (('rs', RS_VENV, True), ('ref', REF_VENV, False)):
        worker = dedicated(broker_name, venv=venv, rust=rust,
                           acks_on_failure=False)
        app = rs_app(worker)
        try:
            slow = app.send_task('rs_worker.acks_late_slow', args=(2, 5))
            assert worker.wait_started('rs_worker.acks_late_slow', timeout=20)
            time.sleep(0.3)
            if broker_name == 'redis':
                killed = kill_redis_consumer(worker.db)
            else:
                killed = rabbit_kill_consumer(worker.queue)
            assert killed >= 1, killed
            quick = [app.send_task('rs_worker.add', args=(index, index))
                     for index in range(3)]
            values = [result.get(timeout=40) for result in quick]
            slow_outcome = result_outcome(slow, timeout=30)
            log = worker.log()
        finally:
            app.close()
        outcomes[label] = {
            'values': values, 'slow': slow_outcome,
            'reconnected': 'Connection to broker lost' in log,
        }
    assert outcomes['rs'] == outcomes['ref'], outcomes
    assert outcomes['rs']['values'] == [0, 2, 4]
    assert outcomes['rs']['slow'] == ('ok', 5)
    assert outcomes['rs']['reconnected']



@pytest.mark.parametrize('pool', ['solo', 'prefork'])
def test_binding_maintenance_renews_repeatedly(pool):
    from test_worker_consumer import next_redis_db

    db = next_redis_db()
    redis_client(db).flushdb()
    worker = start_worker(
        'redis', pool=pool, concurrency=2, full=True, db=db,
        env_extra={'CELERY_RS_TEST_BINDING_SWEEP': '1'},
        loglevel='info')
    worker.ready()
    try:
        client = redis_client(worker.db)
        samples = []
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            samples.append((
                client.zscore('_kombu.binding_deadlines', 'celery.pidbox'),
                client.zscore('_kombu.binding_deadlines', 'celeryev'),
            ))
            time.sleep(1.0)
        # The test worker uses a short binding TTL; periodic renewal must advance the deadline.
        for index in (0, 1):
            values = [sample[index] for sample in samples if sample[index]]
            assert len(set(values)) >= 3, (pool, samples)
            assert values == sorted(values), (pool, samples)
        assert client.exists('_kombu.binding.celery.pidbox')
        assert client.exists('_kombu.binding.celeryev')
    finally:
        worker.stop()
        worker.cleanup()
