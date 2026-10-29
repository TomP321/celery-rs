"""Compare worker control, mingle and gossip over Rust transport with reference workers."""
from __future__ import annotations

import json
import os
import re
import signal
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
    rs_app,
    run_ref,
    start_worker,
    submit,
    unique,
)
from test_worker_prefork import pool_info, wait_until

TIMEOUT = 30



def start(broker_name, *, venv=RS_VENV, rust=True, directory=None, **kwargs):
    kwargs.setdefault('full', True)
    worker = start_worker(broker_name, venv=venv, rust=rust,
                          directory=directory, **kwargs)
    worker.ready()
    return worker


def stop_all(workers):
    for worker in workers:
        worker.stop()
        worker.cleanup()


def wait_state(result, state, timeout=TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result.state == state:
            return result
        time.sleep(0.05)
    raise AssertionError(f'{result.id}: state {result.state}, expected {state}')


def replies_keys(replies):
    return sorted(replies or ())


def control(worker, code):
    """Run a control command through the reference client."""
    return run_ref(code, worker)


def ping(worker, timeout=3, nodename=None):
    env_worker = worker
    if nodename is not None:
        env_worker = _with_nodename(worker, nodename)
    result = env_worker.inspect('ping', timeout)
    return 'pong' in result.stdout


class _WithNodename:
    def __init__(self, worker, nodename):
        self._worker = worker
        self.nodename = nodename

    def __getattr__(self, name):
        return getattr(self._worker, name)


def _with_nodename(worker, nodename):
    return _WithNodename(worker, nodename)


def wait_ping(worker, timeout=TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if ping(worker, 1):
            return True
        time.sleep(0.2)
    return False



@pytest.fixture(scope='module')
def solo_workers(tmp_path_factory):
    directory = tmp_path_factory.mktemp('control-solo')
    started = {}
    for name in BROKERS:
        if broker_available(name):
            started[name] = start(
                name, pool='solo', concurrency=1, directory=directory / name)
    yield started
    stop_all(started.values())


@pytest.fixture(scope='module')
def prefork_workers(tmp_path_factory):
    directory = tmp_path_factory.mktemp('control-prefork')
    started = {}
    for name in BROKERS:
        if broker_available(name):
            started[name] = start(
                name, pool='prefork', concurrency=2,
                directory=directory / name)
    yield started
    stop_all(started.values())


@pytest.fixture(params=[('solo', name) for name in BROKERS] +
                       [('prefork', name) for name in BROKERS])
def worker(request, solo_workers, prefork_workers):
    pool, name = request.param
    if not broker_available(name):
        pytest.skip(f'broker {name} unreachable')
    store = solo_workers if pool == 'solo' else prefork_workers
    if name not in store:
        pytest.skip(f'worker {pool}/{name} not started')
    worker = store[name]
    worker.pool = pool
    return worker


@pytest.fixture
def dedicated(tmp_path):
    started = []

    def factory(broker_name='redis', **kwargs):
        worker = start(broker_name, directory=tmp_path, **kwargs)
        started.append(worker)
        return worker

    yield factory
    for worker in started:
        worker.stop(signal.SIGKILL)
        worker.cleanup()


@pytest.fixture(scope='module', params=list(BROKERS))
def broker_workers(request, tmp_path_factory):
    name = request.param
    if not broker_available(name):
        pytest.skip(f'broker {name} unreachable')
    directory = tmp_path_factory.mktemp(f'control-{name}')
    rs_worker = start(name, directory=directory / 'rs')
    ref_worker = start(name, venv=REF_VENV, rust=False,
                       directory=directory / 'ref')
    yield name, rs_worker, ref_worker
    stop_all((rs_worker, ref_worker))



def test_inspect_commands(worker):
    for command in ('ping', 'clock', 'registered', 'active_queues', 'stats',
                    'active', 'reserved', 'scheduled', 'revoked'):
        result = worker.inspect(command, 5)
        assert result.returncode == 0, (
            f'{command}: {result.stdout[-2000:]}\n{result.stderr[-2000:]}')
        assert worker.nodename in result.stdout, (command, result.stdout[-2000:])

    # Query a running task with the reference client.
    # As in the reference, a solo task blocks the process and prevents pidbox replies.
    if worker.pool != 'prefork':
        return
    app = rs_app(worker)
    try:
        task_id = unique('query')
        submit(worker, 'rs_worker.sleep_add', (5, 1, 2),
               task_id=task_id, queue=worker.queue)
        assert worker.wait_started('rs_worker.sleep_add', timeout=20)
        replies = control(worker, f'''
replies = app.control.inspect(timeout=2, destination=[{worker.nodename!r}]) \
    .query_task({task_id!r}) or {{}}
emit({{node: {{task: value[0] for task, value in (reply or {{}}).items()}}
      for node, reply in replies.items()}})
''')
        assert any(task_id in value for value in replies.values()), replies
    finally:
        app.close()


def test_control_commands(worker, dedicated):
    app = rs_app(worker)
    try:
        # Check that rate_limit reaches the worker panel and replies.
        assert worker.control('rate_limit', 'rs_worker.rate', '10/s').returncode == 0

        result = worker.control('time_limit', 'rs_worker.add', '2', '5')
        assert result.returncode == 0, result.stderr
    finally:
        app.close()

    # Use a dedicated worker for add_consumer and cancel_consumer queue isolation.
    queue = unique('control-extra')
    result = control(worker, f'''
replies = app.control.add_consumer({queue!r}, destination=[{worker.nodename!r}])
emit({{'replies': replies}})
''')
    assert result['replies'] is None or result['replies']
    assert wait_until(lambda: queue in worker.inspect('active_queues').stdout)
    control(worker, f'''
app.control.cancel_consumer({queue!r}, destination=[{worker.nodename!r}])
emit({{'ok': True}})
''')
    assert wait_until(lambda: queue not in worker.inspect('active_queues').stdout)

    # The dispatcher must emit worker-heartbeat.
    from test_worker_consumer import start_event_receiver, stop_event_receiver
    receiver = start_event_receiver(worker, node_id=unique('hb-receiver'))
    try:
        time.sleep(0.5)
        control(worker, "app.control.heartbeat(destination=[%r]); emit({'ok': True})"
                % worker.nodename)
        time.sleep(1.5)
    finally:
        events, _ = stop_event_receiver(receiver)
    assert any(event.get('type') == 'worker-heartbeat' for event in events), \
        [event.get('type') for event in events][-20:]


def test_revoke_and_terminate(worker):
    task_id = unique('revoke')
    control(worker, f"app.control.revoke({task_id!r}, destination=[{worker.nodename!r}]); "
                    "emit({'ok': True})")
    time.sleep(0.3)
    result = submit(worker, 'rs_worker.add', (1, 2), task_id=task_id,
                    queue=worker.queue)
    wait_state(result, 'REVOKED')


def test_election_matches_reference(dedicated):
    """Check election requests and acknowledgements sent through gossip events."""
    outcomes = {}
    from test_worker_consumer import start_event_receiver, stop_event_receiver
    for label, venv, rust in (('rs', RS_VENV, True), ('ref', REF_VENV, False)):
        worker = dedicated('redis', venv=venv, rust=rust)
        receiver = start_event_receiver(worker, node_id=unique(f'election-{label}'))
        try:
            time.sleep(0.5)
            control(worker, f'''
app.control.election({unique('election')!r}, 'task',
                     app.signature('rs_worker.marker', args=['/tmp/x', 'y']))
emit({{'ok': True}})
''')
            time.sleep(2)
        finally:
            events, _ = stop_event_receiver(receiver)
        types = [event.get('type') for event in events]
        outcomes[label] = ('worker-elect' in types, 'worker-elect-ack' in types)
    assert outcomes['rs'] == (True, True), outcomes
    assert outcomes['rs'] == outcomes['ref'], outcomes


def test_pool_commands_prefork(dedicated):
    worker = dedicated('redis', pool='prefork', concurrency=2)
    info = pool_info(worker)
    assert len(info['processes']) == 2, info

    control(worker, f"app.control.pool_grow(1, destination=[{worker.nodename!r}]); "
                    "emit({'ok': True})")
    assert wait_until(lambda: len(pool_info(worker)['processes']) >= 3), \
        pool_info(worker)

    control(worker, f"app.control.pool_shrink(1, destination=[{worker.nodename!r}]); "
                    "emit({'ok': True})")
    assert wait_until(lambda: len(pool_info(worker)['processes']) <= 2), \
        pool_info(worker)

    control(worker, f"app.control.autoscale(4, 1, destination=[{worker.nodename!r}]); "
                    "emit({'ok': True})")
    # The Rust pool records the limit and adds processes on demand.
    assert wait_until(lambda: pool_info(worker).get('implementation', '').endswith('RsTaskPool'))
    assert worker.inspect('stats').returncode == 0


def test_shutdown_command(dedicated):
    worker = dedicated('redis')
    # shutdown raises WorkerShutdown in the panel; the worker may exit before its reply reaches the client.
    worker.control('shutdown')
    deadline = time.monotonic() + TIMEOUT
    while time.monotonic() < deadline and worker.process.poll() is None:
        time.sleep(0.1)
    assert worker.process.poll() is not None


def test_destination_and_pattern(dedicated):
    first = dedicated('redis', nodename=f'{unique("ctl-a")}@localhost')
    second = dedicated('redis', nodename=f'{unique("ctl-b")}@localhost',
                       db=first.db)
    code = f'''
only = app.control.inspect(timeout=1.5, destination=[{first.nodename!r}]).ping() or {{}}
pattern = app.control.inspect(timeout=1.5, pattern='ctl-a*@*').ping() or {{}}
missing = app.control.inspect(timeout=0.5, destination=['nope@nowhere']).ping() or {{}}
emit({{'only': sorted(only), 'pattern': sorted(pattern), 'missing': sorted(missing)}})
'''
    replies = control(first, code)
    assert replies['only'] == [first.nodename], replies
    assert first.nodename in replies['pattern'], replies
    assert second.nodename not in replies['pattern'], replies
    assert replies['missing'] == [], replies



@pytest.mark.parametrize('direction', ['rs-to-ref', 'ref-to-rs'])
def test_mingle_syncs_revoked_between_ref_and_rs(tmp_path, direction):
    broker = 'redis'
    if not broker_available(broker):
        pytest.skip('Redis unreachable')
    rs_worker = ref_worker = None
    try:
        if direction == 'rs-to-ref':
            rs_worker = start(broker, directory=tmp_path / 'rs')
            task_id = unique('mingle-rs-ref')
            control(rs_worker, f"app.control.revoke({task_id!r}, "
                               f"destination=[{rs_worker.nodename!r}]); emit({{'ok': True}})")
            time.sleep(0.5)
            ref_worker = start(broker, venv=REF_VENV, rust=False,
                               directory=tmp_path / 'ref',
                               db=rs_worker.db)
            target, revoker = ref_worker, rs_worker
        else:
            ref_worker = start(broker, venv=REF_VENV, rust=False,
                               directory=tmp_path / 'ref')
            task_id = unique('mingle-ref-rs')
            control(ref_worker, f"app.control.revoke({task_id!r}, "
                                f"destination=[{ref_worker.nodename!r}]); emit({{'ok': True}})")
            time.sleep(0.5)
            rs_worker = start(broker, directory=tmp_path / 'rs',
                              db=ref_worker.db)
            target, revoker = rs_worker, ref_worker

        # Route only to the node that just mingled; it must learn the revocation from its peer.
        result = submit(target, 'rs_worker.add', (1, 2), task_id=task_id,
                        queue=target.queue)
        wait_state(result, 'REVOKED')
    finally:
        stop_all([worker for worker in (rs_worker, ref_worker) if worker])



def test_gossip_sees_reference_worker_join_and_leave(tmp_path):
    if not broker_available('redis'):
        pytest.skip('Redis unreachable')
    log = tmp_path / 'gossip.log'
    observer = start('redis', directory=tmp_path / 'observer',
                     env_extra={'CELERY_RS_TEST_GOSSIP_LOG': str(log)})
    guest = None
    try:
        guest = start('redis', venv=REF_VENV, rust=False,
                      directory=tmp_path / 'guest', db=observer.db)
        assert wait_until(
            lambda: log.exists() and f'node_join {guest.nodename}' in log.read_text(),
            timeout=TIMEOUT), log.read_text() if log.exists() else ''

        guest.stop()
        guest = None
        assert wait_until(
            lambda: any(f'{kind} ' in log.read_text()
                        for kind in ('node_leave', 'node_lost')),
            timeout=TIMEOUT), log.read_text()
        # Check that gossip remains usable.
        assert wait_ping(observer)
    finally:
        stop_all([worker for worker in (observer, guest) if worker])



def test_pidbox_responds_after_broker_reconnect(dedicated):
    worker = dedicated('redis')
    assert ping(worker)

    if worker.broker_name == 'redis':
        killed = kill_redis_consumer(worker.db)
    else:
        killed = rabbit_kill_consumer(worker.queue)
    assert killed, 'no consumer connection to close'

    assert wait_ping(worker, timeout=40), worker.log()[-4000:]



# Real kombu objects require amqp, kombu.connection and pidbox; broker clients and transports remain avoidable.
FORBIDDEN_MASTER_MODULES = {
    'redis',
    'kombu.transport.redis',
    'kombu.transport.pyamqp',
    'kombu.transport.virtual',
    'kombu.transport.virtual.base',
}


@pytest.mark.parametrize('pool', ['solo', 'prefork'])
def test_master_does_not_import_broker_modules(tmp_path, pool):
    if not broker_available('redis'):
        pytest.skip('Redis unreachable')
    state = tmp_path / 'modules.json'
    worker = start('redis', pool=pool, concurrency=2,
                   directory=tmp_path / 'worker',
                   env_extra={'CELERY_RS_TEST_MASTER_MODULES': str(state)})
    try:
        assert wait_until(lambda: state.exists(), timeout=TIMEOUT)
        modules = set(json.loads(state.read_text())['modules'])
        loaded = modules & FORBIDDEN_MASTER_MODULES
        assert not loaded, loaded
        assert 'redis' not in modules
    finally:
        worker.stop(signal.SIGKILL)
        worker.cleanup()
