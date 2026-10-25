"""Pool di processi Rust (spec 05b): zygote + figli Python.

Eseguito con ``.venv-rs`` contro Redis reale. Il pool viene pilotato
direttamente con ``Request`` del riferimento (messaggi costruiti con
``AMQP.as_task_v2``) e un backend Redis vero; gli stessi scenari girano anche
con ``celery.concurrency.prefork.TaskPool`` per confrontare risultati,
callback ed eccezioni con billiard.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from unittest.mock import Mock

import pytest
from kombu.serialization import dumps

from celery import Celery, signals
from celery.concurrency.prefork import TaskPool as PreforkTaskPool
from celery.concurrency.rs_prefork import RsTaskPool
from celery.exceptions import (SoftTimeLimitExceeded, Terminated, TimeLimitExceeded,
                               WorkerLostError)
from celery.worker.request import Request


def unwrap(exc):
    """Le eccezioni del pool arrivano avvolte in ``ExceptionWithTraceback``."""
    return getattr(exc, 'exc', exc)

ROOT = Path(__file__).resolve().parents[2]
REDIS_URL = os.environ.get('CELERY_RS_TEST_REDIS', 'redis://127.0.0.1:6390')
# Database dedicato: non interferire con gli altri test (0/1/8/9).
BACKEND_URL = REDIS_URL.rsplit('/', 1)[0] + '/11' if REDIS_URL.rsplit('/', 1)[-1].isdigit() \
    else REDIS_URL.rstrip('/') + '/11'
HOSTNAME = 'w1@pool-test'

app = Celery('rs_pool_test', broker='memory://', backend=BACKEND_URL)
app.conf.update(
    result_serializer='pickle',
    accept_content=['json', 'pickle'],
    task_serializer='json',
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
)


@app.task(shared=False)
def add(x, y):
    return x + y


@app.task(shared=False)
def sleepy(seconds):
    time.sleep(seconds)
    return 'done'


@app.task(shared=False)
def whoami():
    return os.getpid()


@app.task(shared=False)
def rss_kb():
    import resource
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


@app.task(shared=False)
def allocate(mb):
    return (os.getpid(), len(bytearray(mb * 1024 * 1024)))


@app.task(shared=False, acks_late=True, reject_on_worker_lost=True)
def lost_but_reject():
    time.sleep(5)
    return 'never'


# -- helper ---------------------------------------------------------------


def make_request(task, args=(), kwargs=None, **options):
    """``Request`` del riferimento da un messaggio ``as_task_v2``."""
    task_id = options.pop('task_id', None) or uuid.uuid4().hex
    time_limit = options.pop('time_limit', None)
    soft_time_limit = options.pop('soft_time_limit', None)
    message = app.amqp.as_task_v2(
        task_id, task.name, args=list(args), kwargs=kwargs or {},
        time_limit=time_limit, soft_time_limit=soft_time_limit, **options,
    )
    mock = Mock(name=f'Message-{task_id}')
    mock.headers = message.headers
    mock.content_type, mock.content_encoding, mock.body = dumps(
        message.body, serializer='json')
    mock.payload = message.body
    mock.delivery_info = {
        'exchange': '', 'routing_key': 'celery', 'redelivered': False,
    }
    mock.properties = message.properties
    ack = Mock(name='ack')
    reject = Mock(name='reject')
    mock.ack = ack
    mock.reject = reject
    mock.ack_log_error = Mock(name='ack_log_error')
    mock.reject_log_error = Mock(name='reject_log_error')
    request = Request(mock, app=app, on_ack=ack, on_reject=reject,
                      hostname=HOSTNAME)
    return request, ack, reject


class Driver:
    """Avvia il pool indicato e guida i callback come il loop del master."""

    def __init__(self, kind, concurrency=4, **options):
        self.kind = kind
        self.options = options
        if kind == 'rust':
            self.pool = RsTaskPool(
                limit=concurrency, app=app, initargs=(app, HOSTNAME), **options)
        elif kind == 'prefork':
            self.pool = PreforkTaskPool(
                limit=concurrency, app=app, initargs=(app, HOSTNAME),
                threads=True, **options)
        else:  # pragma: no cover
            raise ValueError(kind)
        self.jobs = []

    def __enter__(self):
        self.pool.start()
        return self

    def __exit__(self, *exc_info):
        self.stop()

    def submit(self, task, args=(), kwargs=None, **options):
        request, ack, reject = make_request(task, args, kwargs, **options)
        result = request.execute_using_pool(self.pool)
        self.jobs.append((request, result, ack, reject))
        return request, result, ack, reject

    def tick(self):
        if self.kind == 'rust':
            self.pool.wait(0.02)
        else:
            time.sleep(0.005)

    def wait(self, timeout=60):
        deadline = time.monotonic() + timeout
        while True:
            if self.kind == 'rust':
                self.pool.process_events()
            if all(result.ready() for _, result, _, _ in self.jobs):
                return
            if time.monotonic() > deadline:
                raise AssertionError(
                    'job non completati: {}'.format(
                        [result.ready() for _, result, _, _ in self.jobs]))
            self.tick()

    def wait_until(self, predicate, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.kind == 'rust':
                self.pool.process_events()
            if predicate():
                return True
            self.tick()
        return False

    def meta(self, request):
        return app.backend.get_task_meta(request.id)

    def value(self, request):
        meta = self.meta(request)
        assert meta['status'] == 'SUCCESS', meta
        return meta['result']

    def wait_status(self, request, timeout=15):
        """Attende che il callback del pool abbia scritto il risultato.

        Per i fallimenti decisi dal master (hard timeout, worker perso) il
        `_set` di billiard sveglia il risultato prima di eseguire
        l'error_callback, quindi il backend può essere ancora PENDING.
        """
        deadline = time.monotonic() + timeout
        status = self.meta(request)['status']
        while status == 'PENDING' and time.monotonic() < deadline:
            self.tick()
            status = self.meta(request)['status']
        return status

    def exception(self, request):
        status = self.wait_status(request)
        meta = self.meta(request)
        assert status == 'FAILURE', meta
        return meta['result']

    def state(self, request):
        return self.wait_status(request)

    def pids(self):
        return sorted(pid for pid in self.pool.info['processes'] if pid)

    def stop(self):
        # billiard non ferma il TimeoutHandler in `Pool.join()` (solo in
        # `terminate()`): con soft/hard time limit resterebbe un thread vivo
        # per ogni pool, e i fork successivi dello zygote sarebbero rischiosi.
        inner = getattr(self.pool, '_pool', None)
        timeout_handler = getattr(inner, '_timeout_handler', None)
        try:
            self.pool.stop()
        finally:
            if timeout_handler is not None:
                timeout_handler.terminate()
                timeout_handler.stop(5)
            # Pool billiard già chiuso e atteso: il finalizzatore `_terminate_pool`
            # rimasto registrato riscriverebbe sulla outqueue all'uscita
            # dell'interprete e può fermarsi sul lock della coda (blocco a fine test).
            finalizer = getattr(inner, '_terminate', None)
            if finalizer is not None:
                finalizer.cancel()
            self.pool = None


@pytest.mark.parametrize('kind', ['rust', 'prefork'])
def test_concurrent_requests_results_and_callbacks(kind):
    with Driver(kind, concurrency=4) as driver:
        started = time.monotonic()
        jobs = [driver.submit(add, (index, index + 1)) for index in range(8)]
        failures = [driver.submit(sleepy, (0.15,)) for _ in range(8)]
        driver.wait()
        elapsed = time.monotonic() - started

        assert [driver.value(request) for request, _, _, _ in jobs] == [
            index + index + 1 for index in range(8)]
        assert all(driver.value(request) == 'done'
                   for request, _, _, _ in failures)
        # Callback `on_accepted`: pid e time_accepted come da billiard.
        for request, result, ack, _ in jobs + failures:
            assert request.worker_pid is not None
            assert request.time_start is not None
            assert result.accepted()
            assert result.worker_pids() == [request.worker_pid]
        # Con 8 job da 0.15s e -c 4 il pool deve eseguirli in parallelo
        # (in serie sarebbero 1.2s).
        assert elapsed < 1.0


def test_results_match_prefork():
    observed = {}
    for kind in ('rust', 'prefork'):
        with Driver(kind, concurrency=2) as driver:
            jobs = [driver.submit(add, (1, 2)), driver.submit(sleepy, (0.05,))]
            driver.wait()
            observed[kind] = [
                (driver.state(request), driver.value(request))
                for request, _, _, _ in jobs
            ]
    assert observed['rust'] == observed['prefork']


@pytest.mark.parametrize('kind', ['rust', 'prefork'])
def test_soft_time_limit(kind):
    with Driver(kind, concurrency=2) as driver:
        before = driver.pids()
        request, result, _, _ = driver.submit(
            sleepy, (5,), soft_time_limit=0.2, time_limit=5)
        driver.wait(timeout=15)
        error = driver.exception(request)
        assert isinstance(error, SoftTimeLimitExceeded)
        assert driver.state(request) == 'FAILURE'
        assert result.ready()
        # Il soft timeout non deve uccidere il figlio.
        time.sleep(0.2)
        assert driver.pids() == before


@pytest.mark.parametrize('kind', ['rust', 'prefork'])
def test_hard_time_limit_replaces_child(kind):
    with Driver(kind, concurrency=2) as driver:
        before = driver.pids()
        request, result, _, _ = driver.submit(sleepy, (5,), time_limit=0.3)
        driver.wait(timeout=15)
        error = driver.exception(request)
        assert isinstance(error, TimeLimitExceeded)
        assert driver.state(request) == 'FAILURE'
        assert driver.wait_until(lambda: driver.pids() != before, 10)
        # Il pool resta utilizzabile dopo il rimpiazzo.
        request2, result2, _, _ = driver.submit(add, (2, 2))
        driver.wait()
        assert driver.value(request2) == 4


@pytest.mark.parametrize('kind', ['rust', 'prefork'])
def test_worker_killed_gives_worker_lost(kind):
    with Driver(kind, concurrency=2, lost_worker_timeout=0.1) as driver:
        before = driver.pids()
        request, result, _, _ = driver.submit(sleepy, (5,))
        assert driver.wait_until(lambda: request.worker_pid is not None)
        time.sleep(0.2)
        os.kill(request.worker_pid, signal.SIGKILL)
        driver.wait(timeout=15)
        error = driver.exception(request)
        assert isinstance(error, WorkerLostError)
        assert str(error) == (
            f'Worker exited prematurely: signal 9 (SIGKILL) '
            f'Job: {result._job}.')
        assert driver.wait_until(lambda: driver.pids() != before, 10)


def test_reject_on_worker_lost():
    rejected = {}
    for kind in ('rust', 'prefork'):
        with Driver(kind, concurrency=1, lost_worker_timeout=0.1) as driver:
            request, result, _, reject = driver.submit(lost_but_reject)
            assert driver.wait_until(lambda: request.worker_pid is not None)
            time.sleep(0.2)
            os.kill(request.worker_pid, signal.SIGKILL)
            driver.wait(timeout=15)
            rejected[kind] = [call.args[-1]
                              for call in reject.call_args_list]
            assert rejected[kind] == [True]
    assert rejected['rust'] == rejected['prefork']


@pytest.mark.parametrize('kind', ['rust', 'prefork'])
def test_maxtasksperchild(kind):
    with Driver(kind, concurrency=2, maxtasksperchild=2) as driver:
        jobs = [driver.submit(whoami) for _ in range(6)]
        driver.wait()
        pids = [driver.value(request) for request, _, _, _ in jobs]
        # Ogni figlio esegue al massimo 2 job e viene rimpiazzato.
        assert all(pids.count(pid) <= 2 for pid in set(pids))
        assert len(set(pids)) >= 3


@pytest.mark.parametrize('kind', ['rust', 'prefork'])
def test_max_memory_per_child(kind):
    with Driver(kind, concurrency=1) as driver:
        baseline_request, _, _, _ = driver.submit(rss_kb)
        driver.wait()
        baseline = driver.value(baseline_request)

    with Driver(kind, concurrency=1,
                max_memory_per_child=baseline + 15_000) as driver:
        jobs = [driver.submit(allocate, (40,)) for _ in range(3)]
        driver.wait(timeout=30)
        pids = [driver.value(request)[0] for request, _, _, _ in jobs]
        assert all(driver.value(request)[1] == 40 * 1024 * 1024
                   for request, _, _, _ in jobs)
        assert len(set(pids)) >= 2


@pytest.mark.parametrize('signum', [signal.SIGTERM, signal.SIGKILL])
def test_terminate_job(signum):
    observed = {}
    for kind in ('rust', 'prefork'):
        with Driver(kind, concurrency=1) as driver:
            request, result, _, _ = driver.submit(sleepy, (5,))
            assert driver.wait_until(lambda: request.worker_pid is not None)
            time.sleep(0.1)
            request.terminate(driver.pool, signal=signum)
            driver.wait(timeout=15)
            assert request._already_revoked
            with pytest.raises(Exception) as excinfo:
                result.get(timeout=5)
            error = unwrap(excinfo.value)
            assert isinstance(error, Terminated)
            observed[kind] = type(error)
    assert observed['rust'] == observed['prefork']


def test_grow_shrink_and_info():
    with Driver('prefork', concurrency=2) as reference:
        reference_info = reference.pool.info
    with Driver('rust', concurrency=2) as driver:
        assert set(driver.pool.info) == set(reference_info)
        assert driver.pool.info['max-concurrency'] == 2
        assert driver.pool.info['put-guarded-by-semaphore'] is True
        assert driver.pool.info['max-tasks-per-child'] == 'N/A'
        assert driver.pool.info['timeouts'] == (0, 0)
        assert driver.pool.num_processes == 2

        driver.pool.grow(2)
        assert driver.wait_until(
            lambda: len(driver.pids()) == 4 and driver.pool.num_processes == 4, 10)
        driver.pool.shrink(2)
        assert driver.wait_until(
            lambda: len(driver.pids()) == 2 and driver.pool.num_processes == 2, 10)

        busy = [driver.submit(sleepy, (0.5,)) for _ in range(2)]
        assert driver.wait_until(
            lambda: all(request.worker_pid for request, _, _, _ in busy))
        with pytest.raises(ValueError):
            driver.pool.shrink(1)


def test_info_writes_shape():
    with Driver('rust', concurrency=2) as driver:
        jobs = [driver.submit(add, (index, 0)) for index in range(3)]
        driver.wait()
        writes = driver.pool.info['writes']
        assert set(writes) == {'total', 'avg', 'all', 'raw', 'strategy', 'inqueues'}
        assert writes['total'] == 3
        assert writes['inqueues']['total'] == 2


def test_graceful_shutdown_finishes_running_job():
    driver = Driver('rust', concurrency=2)
    driver.pool.start()
    pids = [driver.pool._zygote_pid] + driver.pids()
    request, result, _, _ = driver.submit(sleepy, (0.4,))
    started = time.monotonic()
    driver.pool.stop()
    assert time.monotonic() - started >= 0.2
    assert result.ready()
    assert driver.meta(request)['result'] == 'done'
    assert _wait_gone(pids, timeout=5)


def test_terminate_pool_leaves_no_orphans():
    driver = Driver('rust', concurrency=3)
    driver.pool.start()
    pids = [driver.pool._zygote_pid] + driver.pids()
    driver.submit(sleepy, (30,))
    driver.pool.terminate()
    driver.pool = None
    assert _wait_gone(pids, timeout=10)


def _wait_gone(pids, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not any(os.path.exists(f'/proc/{pid}') for pid in pids):
            return True
        time.sleep(0.02)
    return not any(os.path.exists(f'/proc/{pid}') for pid in pids)


def _signal_recorder(path):
    def record(sender, pid=None, **kwargs):
        with open(path, 'a') as handle:
            handle.write(f'{os.getpid()}\n')

    return record


@pytest.mark.parametrize('kind', ['rust', 'prefork'])
def test_worker_process_signals(kind, tmp_path):
    path = tmp_path / 'signals.txt'
    os.environ['CELERY_RS_POOL_SIGNAL_FILE'] = str(path)
    init = _signal_recorder(str(path))
    shutdown = _signal_recorder(str(path))
    signals.worker_process_init.connect(init, weak=False)
    signals.worker_process_shutdown.connect(shutdown, weak=False)
    try:
        with Driver(kind, concurrency=2) as driver:
            children = driver.pids()
            request, _, _, _ = driver.submit(whoami)
            driver.wait()
            assert driver.value(request) in children
        lines = path.read_text().split()
        assert len(lines) == 4
        assert sorted(lines) == sorted([str(pid) for pid in children] * 2)
    finally:
        signals.worker_process_init.disconnect(init)
        signals.worker_process_shutdown.disconnect(shutdown)
        os.environ.pop('CELERY_RS_POOL_SIGNAL_FILE', None)


def test_pss_and_throughput_vs_prefork():
    """PSS (master+zygote+figli) e job/s, riportati e confrontati."""
    probe = Path(__file__).with_name('_pool_probe.py')
    results = {}
    for kind in ('rust', 'prefork'):
        completed = subprocess.run(
            [sys.executable, str(probe), kind],
            capture_output=True, text=True, timeout=300,
        )
        assert completed.returncode == 0, completed.stderr
        results[kind] = json.loads(completed.stdout.strip().splitlines()[-1])

    rust, prefork = results['rust'], results['prefork']
    print('\npool PSS/job/s: rust={} prefork={}'.format(rust, prefork))
    assert rust['jobs'] == prefork['jobs'] == 1000
    assert rust['jobs_per_s'] > 100
    assert prefork['jobs_per_s'] > 100
    # La memoria resta nello stesso ordine di grandezza (lo zygote è un
    # processo in più nel conteggio PSS: i numeri sono riportati, non un gate).
    assert rust['pss_idle_kb'] < prefork['pss_idle_kb'] * 2
    assert rust['pss_loaded_kb'] < prefork['pss_loaded_kb'] * 2
