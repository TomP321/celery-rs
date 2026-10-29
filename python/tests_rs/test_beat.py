"""Compare Rust and reference beat messages using isolated queues and a controlled TCP proxy."""
from __future__ import annotations

import json
import os
from urllib.parse import urlsplit
import re
import signal
import socket
import subprocess
import threading
import time
import uuid
from pathlib import Path

import pytest
import redis

TESTS_DIR = Path(__file__).resolve().parent
ROOT = TESTS_DIR.parents[1]
REF_VENV = ROOT / '.venv-ref'
RS_VENV = ROOT / '.venv-rs'
REF_PYTHON = REF_VENV / 'bin' / 'python'
RS_PYTHON = RS_VENV / 'bin' / 'python'

REDIS_HOST, REDIS_PORT = urlsplit(os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390')).hostname, urlsplit(os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390')).port
REDIS_DB = 6
REDIS_URL = f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}'
AMQP_URL = os.environ.get('CELERY_RS_AMQP_URL', 'amqp://guest:guest@127.0.0.1:5673//')

TIMEOUT = 30
CRON_TIMEOUT = 75


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


@pytest.fixture(scope='session', autouse=True)
def require_brokers():
    if not tcp_ok(REDIS_HOST, REDIS_PORT):
        pytest.skip('Redis unreachable')


def unique(prefix):
    return f'{prefix}-{uuid.uuid4().hex[:10]}'


def clean_env(**extra):
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('PYTHON') and not key.startswith('CELERY_')}
    env['PYTHONPATH'] = str(TESTS_DIR)
    env['PYTHONHASHSEED'] = '0'
    env.update({key: str(value) for key, value in extra.items()})
    return env


def redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


def wait_for(predicate, timeout=TIMEOUT, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return predicate()


def wait_log(path, pattern, timeout=TIMEOUT):
    regex = re.compile(pattern)

    def found():
        try:
            return regex.search(Path(path).read_text(errors='replace'))
        except OSError:
            return None

    return wait_for(found, timeout)


def wait_process_ready(path, pattern, process, timeout=TIMEOUT):
    match = wait_log(path, pattern, timeout)
    if not match:
        raise AssertionError(
            f'process not ready ({pattern}):\n'
            f'{Path(path).read_text(errors="replace")[-3000:]}')
    return match


class Beat:
    """Run celery beat in a subprocess."""

    def __init__(self, venv, directory, tag, env_extra, args=(),
                 loglevel='debug', schedule=None):
        self.tag = tag
        self.schedule = (Path(schedule) if schedule is not None
                         else Path(directory) / f'{tag}-schedule')
        self.log = Path(directory) / f'{tag}.log'
        env = clean_env(**env_extra)
        command = [
            str(Path(venv) / 'bin' / 'python'), '-m', 'celery',
            '-A', 'beat_app', 'beat',
            '-s', str(self.schedule), '-l', loglevel, *args,
        ]
        with open(self.log, 'w') as log:
            self.process = subprocess.Popen(
                command, cwd=TESTS_DIR, env=env, stdout=log,
                stderr=subprocess.STDOUT, start_new_session=True)

    def ready(self, timeout=TIMEOUT):
        wait_process_ready(self.log, 'beat: Starting...', self.process, timeout)
        return self

    def stop(self, timeout=TIMEOUT):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
        try:
            self.process.wait(timeout)
        except subprocess.TimeoutExpired:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait()
        return self.process.returncode

    def text(self):
        return Path(self.log).read_text(errors='replace')

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stop()


class Worker:
    """Run the selected venv's worker on a dedicated queue."""

    def __init__(self, venv, directory, tag, env_extra, queue):
        self.log = Path(directory) / f'{tag}.log'
        self.queue = queue
        env = clean_env(**env_extra)
        command = [
            str(Path(venv) / 'bin' / 'python'), '-m', 'celery',
            '-A', 'beat_app', 'worker', '-P', 'solo', '-c', '1',
            '-Q', queue, '-n', f'{tag}@localhost', '-l', 'info',
            '--without-gossip', '--without-mingle',
        ]
        with open(self.log, 'w') as log:
            self.process = subprocess.Popen(
                command, cwd=TESTS_DIR, env=env, stdout=log,
                stderr=subprocess.STDOUT, start_new_session=True)

    def ready(self, timeout=TIMEOUT):
        wait_process_ready(self.log, r' ready\.', self.process, timeout)
        return self

    def stop(self, timeout=TIMEOUT):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
        try:
            self.process.wait(timeout)
        except subprocess.TimeoutExpired:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stop()


class EmbeddedWorker(Worker):
    """Run a worker with embedded beat."""

    def __init__(self, venv, directory, tag, env_extra, queue):
        self.log = Path(directory) / f'{tag}.log'
        self.queue = queue
        env = clean_env(**env_extra)
        command = [
            str(Path(venv) / 'bin' / 'python'), '-m', 'celery',
            '-A', 'beat_app', 'worker', '-P', 'solo', '-c', '1',
            '-B', '-s', str(Path(directory) / f'{tag}-schedule'),
            '-Q', queue, '-n', f'{tag}@localhost', '-l', 'info',
            '--without-gossip', '--without-mingle',
        ]
        with open(self.log, 'w') as log:
            self.process = subprocess.Popen(
                command, cwd=TESTS_DIR, env=env, stdout=log,
                stderr=subprocess.STDOUT, start_new_session=True)



class TcpProxy:
    """Controllable TCP proxy for broker outages and restarts."""

    def __init__(self, host=REDIS_HOST, port=REDIS_PORT):
        self.target = (host, port)
        self.server = None
        self.port = None
        self._conns = set()
        self._lock = threading.Lock()
        self._thread = None
        self._stop = threading.Event()

    def prepare(self):
        """Reserve the port without accepting connections."""
        server = socket.socket()
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('127.0.0.1', 0))
        self.server = server
        self.port = server.getsockname()[1]
        return self

    def start(self):
        if self.server is None:
            self.prepare()
        self._stop.clear()
        self.server.listen(64)
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()
        return self

    def _accept(self):
        while not self._stop.is_set():
            try:
                client, _ = self.server.accept()
            except OSError:
                return
            try:
                upstream = socket.create_connection(self.target)
            except OSError:
                client.close()
                continue
            with self._lock:
                self._conns.update((client, upstream))
            for left, right in ((client, upstream), (upstream, client)):
                threading.Thread(target=self._pump, args=(left, right),
                                 daemon=True).start()

    def _pump(self, src, dst):
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                dst.sendall(data)
        except OSError:
            pass
        finally:
            for sock in (src, dst):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass
                with self._lock:
                    self._conns.discard(sock)

    def stop(self):
        """Close the listener and active connections to simulate broker loss."""
        self._stop.set()
        if self.server is not None:
            try:
                self.server.close()
            except OSError:
                pass
            self.server = None
        with self._lock:
            conns = list(self._conns)
            self._conns.clear()
        for sock in conns:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def restart(self):
        # Keep the port so beat reconnects to the same URL.
        server = socket.socket()
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('127.0.0.1', self.port))
        self.server = server
        return self.start()

    @property
    def url(self):
        return f'redis://127.0.0.1:{self.port}/{REDIS_DB}'



def read_events(sink):
    client = redis_client()
    return [json.loads(item) for item in client.lrange(sink, 0, -1)]


def clear_key(sink):
    redis_client().delete(sink)


def shape(events):
    """Normalize entries whose schedules may fire different numbers of times."""
    out = set()
    for event in events:
        headers = event.get('headers') or {}
        out.add((
            event['task'],
            # Exclude the final beat tag argument (ref/rs) from comparisons.
            tuple(event['args'][:-1]),
            tuple(sorted(event['kwargs'].items())),
            headers.get('celery_beat_task'),
            headers.get('x-extra'),
            bool(event.get('expires')),
        ))
    return out


def beat_env(queue, sink, mode='interval', broker=REDIS_URL, tag='', **extra):
    return dict(
        BEAT_TEST_BROKER=broker,
        BEAT_TEST_QUEUE=queue,
        BEAT_TEST_SINK_KEY=sink,
        BEAT_TEST_MODE=mode,
        BEAT_TEST_TAG=tag,
        **extra,
    )


def events_of(events, tag):
    """Select events published by beat *tag*, stored in the last argument."""
    return [event for event in events
            if event['args'] and event['args'][-1] == tag]


def kinds_of(events, tag):
    return {event['args'][0] for event in events_of(events, tag)}


@pytest.fixture
def directory(tmp_path):
    return tmp_path



def test_interval_and_relative(directory):
    queue = unique('beat-interval')
    sink = unique('sink')
    with Worker(REF_VENV, directory, 'ref-worker', beat_env(queue, sink),
                queue).ready():
        beats = {
            tag: Beat(venv, directory, f'{tag}-beat',
                      beat_env(queue, sink, tag=tag)).ready()
            for tag, venv in (('ref', REF_VENV), ('rs', RS_VENV))
        }
        try:
            wait_for(lambda: all(
                kinds_of(read_events(sink), tag) >= {'interval', 'relative'}
                for tag in ('ref', 'rs')), 20)
        finally:
            for beat in beats.values():
                beat.stop()
        events = read_events(sink)
        ref_events, rs_events = events_of(events, 'ref'), events_of(events, 'rs')
        assert ref_events, 'reference beat did not publish'
        assert shape(ref_events) == shape(rs_events), (
            shape(ref_events) ^ shape(rs_events))
        assert {event['task'] for event in rs_events} == {'beattest.record'}
        assert any(event['kwargs'].get('tag') == 'relative'
                   for event in rs_events)
        assert any(event['args'][0] == 'interval' for event in rs_events)
        clear_key(sink)


def test_options_are_forwarded(directory):
    queue = unique('beat-options')
    sink = unique('sink')
    with Worker(REF_VENV, directory, 'ref-worker', beat_env(queue, sink),
                queue).ready():
        beats = {
            tag: Beat(venv, directory, f'{tag}-beat',
                      beat_env(queue, sink, mode='options', tag=tag)).ready()
            for tag, venv in (('ref', REF_VENV), ('rs', RS_VENV))
        }
        try:
            wait_for(lambda: all(events_of(read_events(sink), tag)
                                 for tag in ('ref', 'rs')), 20)
        finally:
            for beat in beats.values():
                beat.stop()
        events = read_events(sink)
        ref_events, rs_events = events_of(events, 'ref'), events_of(events, 'rs')
        assert shape(ref_events) == shape(rs_events)
        option = rs_events[0]
        assert option['headers'].get('x-extra') == 'hi'
        assert option['headers'].get('celery_beat_task') is True
        assert option['expires'] is not None
        assert option['priority'] == 7
        assert option['task'] == 'beattest.record'
        clear_key(sink)


def test_unregistered_task(directory):
    queue = unique('beat-missing')
    sink = unique('sink')
    with Worker(REF_VENV, directory, 'ref-worker', beat_env(queue, sink),
                queue).ready():
        beats = {
            tag: Beat(venv, directory, f'{tag}-beat',
                      beat_env(queue, sink, mode='missing', tag=tag)).ready()
            for tag, venv in (('ref', REF_VENV), ('rs', RS_VENV))
        }
        try:
            wait_for(lambda: all(
                re.search(r'Sending due task unregistered \(beattest\.unregistered\)',
                          beat.text()) for beat in beats.values()), 20)
        finally:
            for beat in beats.values():
                beat.stop()
        for beat in beats.values():
            text = beat.text()
            assert 'Message Error' not in text
            assert 'Sending due task unregistered' in text
        assert events_of(read_events(sink), 'ref') == []
        clear_key(sink)


def test_crontab_fires_at_minute(directory):
    queue = unique('beat-cron')
    sink = unique('sink')
    with Worker(REF_VENV, directory, 'ref-worker', beat_env(queue, sink),
                queue).ready():
        beats = {
            tag: Beat(venv, directory, f'{tag}-beat',
                      beat_env(queue, sink, mode='crontab', tag=tag)).ready()
            for tag, venv in (('ref', REF_VENV), ('rs', RS_VENV))
        }
        try:
            def fired():
                events = read_events(sink)
                return all(any(event['args'][0] == 'cron'
                               for event in events_of(events, tag))
                           for tag in ('ref', 'rs'))

            wait_for(fired, CRON_TIMEOUT)
        finally:
            for beat in beats.values():
                beat.stop()
        events = read_events(sink)
        ref_events, rs_events = events_of(events, 'ref'), events_of(events, 'rs')
        assert any(event['args'][0] == 'cron' for event in ref_events)
        assert shape(ref_events) == shape(rs_events)
        clear_key(sink)


def test_broker_unreachable_at_startup_then_available(directory):
    proxy = TcpProxy().prepare()  # Reserved port with no listener.
    queue = unique('beat-proxy')
    sink = unique('sink')
    with Worker(REF_VENV, directory, 'ref-worker', beat_env(queue, sink),
                queue).ready():
        beats = {
            tag: Beat(venv, directory, f'{tag}-beat',
                      beat_env(queue, sink, broker=proxy.url, tag=tag)).ready()
            for tag, venv in (('ref', REF_VENV), ('rs', RS_VENV))
        }
        try:
            # Beat must retry rather than exit when the broker is unreachable.
            for beat in beats.values():
                wait_process_ready(
                    beat.log,
                    r'beat: Connection error: .*Trying again in 2(?:\.0)? seconds\.\.\.',
                    beat.process, 20)
                assert beat.process.poll() is None
            proxy.start()
            wait_for(lambda: len(events_of(read_events(sink), 'ref')) >= 2 and
                     len(events_of(read_events(sink), 'rs')) >= 2, 30)
        finally:
            for beat in beats.values():
                beat.stop()
            proxy.stop()
        events = read_events(sink)
        ref_events, rs_events = events_of(events, 'ref'), events_of(events, 'rs')
        assert ref_events
        assert shape(ref_events) == shape(rs_events)
        clear_key(sink)


def test_broker_lost_during_execution_then_recover(directory):
    proxy = TcpProxy().start()
    queue = unique('beat-lost')
    sink = unique('sink')
    with Worker(REF_VENV, directory, 'ref-worker', beat_env(queue, sink),
                queue).ready():
        beats = {
            tag: Beat(venv, directory, f'{tag}-beat',
                      beat_env(queue, sink, broker=proxy.url, tag=tag)).ready()
            for tag, venv in (('ref', REF_VENV), ('rs', RS_VENV))
        }
        try:
            wait_for(lambda: len(events_of(read_events(sink), 'ref')) >= 2 and
                     len(events_of(read_events(sink), 'rs')) >= 2, 20)
            proxy.stop()
            # Both must log publication errors and remain alive.
            for beat in beats.values():
                wait_log(beat.log, r'Message Error:', 30)
                assert beat.process.poll() is None
            before = {tag: len(events_of(read_events(sink), tag))
                      for tag in ('ref', 'rs')}
            proxy.restart()
            wait_for(lambda: all(
                len(events_of(read_events(sink), tag)) > before[tag]
                for tag in ('ref', 'rs')), 30)
        finally:
            for beat in beats.values():
                beat.stop()
            proxy.stop()
        events = read_events(sink)
        ref_events, rs_events = events_of(events, 'ref'), events_of(events, 'rs')
        assert shape(ref_events) == shape(rs_events)
        clear_key(sink)


def dump_schedule(python, path):
    code = '''
import json, shelve, sys
store = shelve.open(sys.argv[1])
entries = store.get('entries', {})
print(json.dumps({
    'keys': sorted(entries),
    'entries': {
        name: [entry.last_run_at.isoformat() if entry.last_run_at else None,
               entry.total_run_count, entry.task]
        for name, entry in entries.items()
    },
    'tz': store.get('tz'),
    'utc_enabled': store.get('utc_enabled'),
}, sort_keys=True))
store.close()
'''
    result = subprocess.run(
        [str(python), '-c', code, str(path)], cwd='/tmp',
        capture_output=True, text=True, check=True)
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_schedule_file_compatible_both_ways(directory):
    queue = unique('beat-file')
    env = beat_env(queue, 'unused')

    # Reference writes, Rust reads without resetting, then reference reads again.
    schedule = directory / 'ref-written-schedule'
    ref = Beat(REF_VENV, directory, 'ref-first', env, schedule=schedule)
    ref.ready()
    time.sleep(2.0)
    ref.stop()
    before = dump_schedule(REF_PYTHON, schedule)
    assert set(before['keys']) >= {'every-300ms', 'relative-500ms'}
    for name in ('every-300ms', 'relative-500ms'):
        assert before['entries'][name][1] > 0

    rs = Beat(RS_VENV, directory, 'rs-second', env, schedule=schedule)
    rs.ready()
    time.sleep(1.5)
    rs.stop()
    after = dump_schedule(REF_PYTHON, schedule)
    assert after['keys'] == before['keys']
    for name, entry in after['entries'].items():
        assert entry[1] >= before['entries'][name][1], (name, before, after)
        assert entry[0] >= before['entries'][name][0]
    assert after['tz'] == before['tz']
    assert after['utc_enabled'] == before['utc_enabled']

    # Rust writes a new file; reference reads it and continues counting.
    schedule2 = directory / 'rs-written-schedule'
    rs2 = Beat(RS_VENV, directory, 'rs-first', env, schedule=schedule2)
    rs2.ready()
    time.sleep(2.0)
    rs2.stop()
    written = dump_schedule(REF_PYTHON, schedule2)
    assert written['keys'] == before['keys']
    ref2 = Beat(REF_VENV, directory, 'ref-second', env, schedule=schedule2)
    ref2.ready()
    time.sleep(1.5)
    ref2.stop()
    read_back = dump_schedule(REF_PYTHON, schedule2)
    for name, entry in read_back['entries'].items():
        assert entry[1] >= written['entries'][name][1]


def test_custom_scheduler_with_own_producer(directory):
    queue = unique('beat-custom')
    sink = unique('sink')
    with Worker(REF_VENV, directory, 'ref-worker', beat_env(queue, sink),
                queue).ready():
        producers = {tag: directory / f'{tag}-producer.txt'
                     for tag in ('ref', 'rs')}
        beats = {
            tag: Beat(venv, directory, f'{tag}-beat',
                      beat_env(queue, sink, tag=tag,
                               BEAT_TEST_PRODUCER_FILE=str(producers[tag])),
                      ('-S', 'beat_app:RecordingScheduler')).ready()
            for tag, venv in (('ref', REF_VENV), ('rs', RS_VENV))
        }
        try:
            wait_for(lambda: all(path.exists() for path in producers.values()), 20)
            wait_for(lambda: all(
                kinds_of(read_events(sink), tag) >= {'interval', 'relative'}
                for tag in ('ref', 'rs')), 20)
        finally:
            for beat in beats.values():
                beat.stop()
        assert producers['ref'].read_text() == 'RecordingProducer'
        assert producers['rs'].read_text() == 'RecordingProducer', (
            'Rust beat must retain kombu when Producer is overridden')
        events = read_events(sink)
        assert shape(events_of(events, 'ref')) == shape(events_of(events, 'rs'))
        clear_key(sink)


def test_producer_env_disable(directory):
    queue = unique('beat-env')
    sink = unique('sink')
    with Worker(REF_VENV, directory, 'ref-worker', beat_env(queue, sink),
                queue).ready():
        mods = directory / 'rs-mods.json'
        ref = Beat(REF_VENV, directory, 'ref-beat',
                   beat_env(queue, sink, tag='ref')).ready()
        rs = Beat(RS_VENV, directory, 'rs-beat',
                  beat_env(queue, sink, tag='rs', CELERY_RS_PRODUCER='0',
                           BEAT_TEST_MODS_OUT=str(mods))).ready()
        try:
            wait_for(lambda: all(
                kinds_of(read_events(sink), tag) >= {'interval', 'relative'}
                for tag in ('ref', 'rs')), 20)
        finally:
            ref.stop()
            rs.stop()
        events = read_events(sink)
        assert shape(events_of(events, 'ref')) == shape(events_of(events, 'rs'))
        imported = json.loads(mods.read_text())
        assert imported['redis'] is True, (
            'CELERY_RS_PRODUCER=0 uses kombu transport')
        clear_key(sink)


@pytest.mark.parametrize('broker,transport', [
    (REDIS_URL, 'redis_transport'),
    (AMQP_URL, 'pyamqp_transport'),
])
def test_rust_producer_does_not_import_transport(directory, broker, transport):
    queue = unique('beat-imports')
    mods = directory / 'mods.json'
    rs = Beat(RS_VENV, directory, 'rs-beat',
              beat_env(queue, unique('sink'), broker=broker,
                       BEAT_TEST_MODS_OUT=str(mods))).ready()
    time.sleep(1.5)
    rs.stop()
    imported = json.loads(mods.read_text())
    assert imported['redis'] is False
    assert imported['redis_transport'] is False
    assert imported['pyamqp_transport'] is False, (
        f'Rust path must not import {transport}')


def test_embedded_beat(directory):
    """Compare embedded beat in Rust and reference workers."""
    queue = unique('beat-embedded')
    results = {}
    for tag, venv in (('ref', REF_VENV), ('rs', RS_VENV)):
        sink = unique('sink')
        worker = EmbeddedWorker(venv, directory, f'{tag}-embedded',
                                beat_env(queue, sink), queue)
        try:
            worker.ready(30)
            wait_for(lambda: {'interval', 'relative'} <= kinds_of(
                read_events(sink), ''), 30)
        finally:
            worker.stop()
        results[tag] = read_events(sink)
        clear_key(sink)
    assert results['ref']
    assert shape(results['ref']) == shape(results['rs'])
