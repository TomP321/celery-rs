"""CLI: output ed exit code identici al riferimento, avvio leggero, ``multi``.

Ogni caso esegue lo stesso comando con la CLI di ``.venv-ref`` (Celery
originale) e con quella di ``.venv-rs``, poi confronta stdout/stderr/exit code
dopo la normalizzazione di host, pid, uuid e tempi. Il worker è sempre quello
del riferimento: la variabile misurata è la CLI.

Copre: ``multi`` reale (start/stop/stopwait/restart/kill) e ``worker``/
``beat --detach`` con worker rs, ``events --dump`` con un task, ``purge`` con
risposta ``no``/``yes`` su stdin, ``inspect`` con 2 worker (ordine e ``limit``),
``control -d`` su più nodi, broker irraggiungibile e moduli importati da
``--help``/``status``/``call``.

Il file è eseguito da ``.venv-rs``; i processi figli usano il venv indicato.
"""
from __future__ import annotations

import json
import os
from urllib.parse import urlsplit
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).resolve().parent
ROOT = TESTS_DIR.parents[1]
REF_VENV = ROOT / '.venv-ref'
RS_VENV = ROOT / '.venv-rs'
REF_PYTHON = REF_VENV / 'bin' / 'python'
RS_PYTHON = RS_VENV / 'bin' / 'python'

REDIS_HOST, REDIS_PORT = urlsplit(os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390')).hostname, urlsplit(os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390')).port
#: Database dedicati (gli altri test usano 7-15): broker 3, backend 4.
BROKER = f'redis://{REDIS_HOST}:{REDIS_PORT}/3'
BACKEND = f'redis://{REDIS_HOST}:{REDIS_PORT}/4'
UNREACHABLE = f'redis://{REDIS_HOST}:6399/0'

HOST = socket.gethostname()
UUID_RE = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')
TIME_RE = re.compile(r'\b\d{1,2}:\d{2}:\d{2}(?:\.\d+)?\b')


def unique(prefix):
    return f'{prefix}-{uuid.uuid4().hex[:10]}'


def venv_python(venv):
    return str((Path(venv).resolve() / 'bin' / 'python'))


def clean_env(**extra):
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('PYTHON') and not key.startswith('CELERY_')}
    env['PYTHONPATH'] = str(TESTS_DIR)
    env['PYTHONHASHSEED'] = '0'
    env.update({key: str(value) for key, value in extra.items()})
    return env


def app_env(broker=BROKER, backend=BACKEND, queue=None):
    return clean_env(CELERY_RS_CLI_BROKER=broker,
                     CELERY_RS_CLI_BACKEND=backend,
                     CELERY_RS_CLI_QUEUE=queue or unique('cli'))


def run_cli(venv, args, env, stdin=None, timeout=120, cwd=None):
    return subprocess.run(
        [venv_python(venv), '-P', '-m', 'celery', *args],
        cwd=cwd or TESTS_DIR, env=env, input=stdin,
        capture_output=True, text=True, timeout=timeout)


def normalize(text, venvs=()):
    text = ANSI_RE.sub('', text)
    text = UUID_RE.sub('<id>', text)
    text = text.replace(HOST, '<host>')
    for venv in venvs:
        text = text.replace(str(Path(venv).resolve()), '<venv>')
    # pid dei worker (almeno 4 cifre) e tempi
    text = re.sub(r'\b\d{4,7}\b', '<pid>', text)
    text = TIME_RE.sub('<time>', text)
    return text


def normalize_lines(text, venvs=()):
    lines = [line.rstrip() for line in normalize(text, venvs).splitlines()]
    return [line for line in lines if line.strip()]


def wait_until(predicate, timeout=60, interval=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return predicate()


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
def require_redis():
    if not tcp_ok(REDIS_HOST, REDIS_PORT):
        pytest.skip('Redis non raggiungibile')


class Worker:
    def __init__(self, venv, nodename, queue, directory=None):
        self.venv = venv
        self.nodename = nodename
        self.queue = queue
        self.directory = Path(directory or tempfile.mkdtemp(prefix='cli-worker-'))
        self.directory.mkdir(parents=True, exist_ok=True)
        self.log = self.directory / f'{nodename.split("@")[0]}.log'
        self.env = app_env(queue=queue)
        self.process = None

    def start(self, *extra, events=False):
        command = [
            venv_python(self.venv), '-P', '-m', 'celery', '-A', 'cli_app',
            'worker', '-P', 'solo', '-c', '1', '-Q', self.queue,
            '-n', self.nodename, '-l', 'warning',
            '--without-gossip', '--without-mingle',
            *extra,
        ]
        if events:
            command.append('-E')
        with open(self.log, 'w') as handle:
            self.process = subprocess.Popen(
                command, cwd=TESTS_DIR, env=self.env, stdout=handle,
                stderr=subprocess.STDOUT, start_new_session=True)
        self.ready()
        return self

    def ready(self, timeout=60):
        def ping():
            if self.process.poll() is not None:
                raise RuntimeError(f'worker terminato:\n{self.log_text()[-4000:]}')
            result = run_cli(REF_VENV, [
                '-A', 'cli_app', '-b', self.env['CELERY_RS_CLI_BROKER'],
                'inspect', 'ping', '-d', self.nodename, '-t', '1'], self.env)
            return 'pong' in result.stdout
        if not wait_until(ping, timeout=timeout, interval=0.2):
            self.stop()
            raise RuntimeError(f'worker non pronto:\n{self.log_text()[-4000:]}')
        return self

    def log_text(self):
        try:
            return self.log.read_text(errors='replace')
        except OSError:
            return ''

    def stop(self, sig=signal.SIGTERM, timeout=30):
        if self.process is None or self.process.poll() is not None:
            return
        try:
            os.killpg(self.process.pid, sig)
        except ProcessLookupError:
            return
        try:
            self.process.wait(timeout)
        except subprocess.TimeoutExpired:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait(10)


@pytest.fixture(scope='module')
def queue():
    return unique('cli-tests')


@pytest.fixture(scope='module')
def env(queue):
    return app_env(queue=queue)


@pytest.fixture(scope='module')
def worker(queue, env):
    started = Worker(REF_VENV, f'cli-ref@{HOST}', queue).start(events=True)
    yield started
    started.stop()


@pytest.fixture(scope='module')
def two_workers(queue, env):
    first = Worker(REF_VENV, f'cli-w1@{HOST}', queue).start()
    second = Worker(REF_VENV, f'cli-w2@{HOST}', queue).start()
    yield first, second
    for item in (first, second):
        item.stop()


# ---------------------------------------------------------------------------
# multi / worker --detach / beat --detach
# ---------------------------------------------------------------------------

def _multi_args(names, directory, extra=()):
    return [
        'multi', *names,
        '-A', 'cli_app', '-c', '1',
        f'--pidfile={directory}/%n.pid',
        f'--logfile={directory}/%n.log',
        '--without-gossip', '--without-mingle',
        *extra,
    ]


def _multi_sequence(venv, broker):
    """start → show → restart → kill → stopwait con *venv*."""
    directory = Path(tempfile.mkdtemp(prefix='cli-multi-'))
    env = app_env(broker=broker, queue=unique('cli-multi'))
    steps = []

    def step(args, timeout=120, stdin=None):
        result = run_cli(venv, args, env, timeout=timeout, stdin=stdin)
        def clean(text):
            text = text.replace(str(directory), '<dir>')
            text = normalize(text, venvs=(REF_VENV, RS_VENV))
            text = re.sub(r'(Waiting for \d+ nodes? -> ).*', r'\1<state>', text)
            return [line.rstrip() for line in text.splitlines() if line.strip()]
        steps.append((result.returncode, clean(result.stdout),
                      clean(result.stderr)))
        return result

    def ping(nodename):
        def check():
            result = run_cli(REF_VENV, [
                '-A', 'cli_app', '-b', broker, 'inspect', 'ping',
                '-d', f'{nodename}@{HOST}', '-t', '1'], env)
            return 'pong' in result.stdout
        return wait_until(check, timeout=30, interval=0.2)

    try:
        step(_multi_args(('start', 'w1', 'w2'), directory))
        assert ping('w1') and ping('w2')
        step(_multi_args(('show', 'w1', 'w2'), directory))
        step(_multi_args(('restart', 'w1', 'w2'), directory))
        assert ping('w1') and ping('w2')
        step(_multi_args(('kill', 'w1'), directory))
        step(_multi_args(('names', '3'), directory))
        step(_multi_args(('expand', '%n-%h', 'w1', 'w2'), directory))
    finally:
        run_cli(venv, _multi_args(('stopwait', 'w1', 'w2'), directory), env)
        shutil.rmtree(directory, ignore_errors=True)
    return steps


def test_multi_real_matches_reference():
    """``multi start/restart/kill/show/names/expand`` con il worker del venv."""
    steps_ref = _multi_sequence(REF_VENV, f'redis://{REDIS_HOST}:{REDIS_PORT}/5')
    steps_rs = _multi_sequence(RS_VENV, f'redis://{REDIS_HOST}:{REDIS_PORT}/6')
    assert len(steps_ref) == len(steps_rs) == 6
    # `Cluster.shutdown_nodes` del riferimento itera un `set` di `Node` (hash per id):
    # l'ordine dei nodi in stop/restart dipende dagli indirizzi di memoria, non è semantica.
    def unordered(step):
        rc, stdout, stderr = step
        return rc, sorted(stdout), sorted(stderr)
    for index, (ref, rust) in enumerate(zip(steps_ref, steps_rs)):
        assert unordered(rust) == unordered(ref), f'passo {index} diverso:\nref={ref}\nrs={rust}'


def test_worker_detach_and_beat_detach():
    """``worker --detach`` e ``beat --detach`` avviano i processi rs."""
    for venv in (REF_VENV, RS_VENV):
        directory = Path(tempfile.mkdtemp(prefix='cli-detach-'))
        env = app_env(queue=unique('cli-detach'))
        try:
            worker = run_cli(venv, [
                '-A', 'cli_app', 'worker', '--detach', '-c', '1',
                '-Q', env['CELERY_RS_CLI_QUEUE'], '-n', f'detach@{HOST}',
                f'--pidfile={directory}/worker.pid',
                f'--logfile={directory}/worker.log',
                '--without-gossip', '--without-mingle'], env)
            assert worker.returncode == 0, worker.stderr
            pidfile = directory / 'worker.pid'
            assert wait_until(pidfile.exists, timeout=30), 'pidfile worker mancante'
            pid = int(pidfile.read_text().strip())
            assert wait_until(lambda: Path(f'/proc/{pid}').exists(), timeout=15)
            run_cli(REF_VENV, [
                '-A', 'cli_app', '-b', env['CELERY_RS_CLI_BROKER'],
                'control', 'shutdown', '-d', f'detach@{HOST}', '-t', '2'], env)
            assert wait_until(lambda: not Path(f'/proc/{pid}').exists(),
                              timeout=30), 'worker detach non terminato'

            beat = run_cli(venv, [
                '-A', 'cli_app', 'beat', '--detach',
                f'--pidfile={directory}/beat.pid',
                f'--logfile={directory}/beat.log',
                f'--schedule={directory}/beat.schedule'], env)
            assert beat.returncode == 0, beat.stderr
            pidfile = directory / 'beat.pid'
            assert wait_until(pidfile.exists, timeout=30), 'pidfile beat mancante'
            pid = int(pidfile.read_text().strip())
            assert wait_until(lambda: Path(f'/proc/{pid}').exists(), timeout=15)
            os.kill(pid, signal.SIGTERM)
            assert wait_until(lambda: not Path(f'/proc/{pid}').exists(),
                              timeout=30), 'beat detach non terminato'
        finally:
            shutil.rmtree(directory, ignore_errors=True)


# ---------------------------------------------------------------------------
# events --dump
# ---------------------------------------------------------------------------

def _dump_events(venv, env, worker_env, seconds=3.0):
    process = subprocess.Popen(
        [venv_python(venv), '-P', '-m', 'celery', '-A', 'cli_app',
         'events', '--dump'],
        cwd=TESTS_DIR, env=worker_env, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True, start_new_session=True)
    try:
        time.sleep(1.0)
        run_cli(REF_VENV, [
            '-A', 'cli_app', '-b', worker_env['CELERY_RS_CLI_BROKER'],
            'call', 'cli.event', '-a', '["x"]'], worker_env)
        time.sleep(seconds)
    finally:
        try:
            os.killpg(process.pid, signal.SIGINT)
        except ProcessLookupError:
            pass
        try:
            output = process.communicate(timeout=15)[0]
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            output = process.communicate()[0]
    return output


def _event_types(output):
    types = set()
    for line in output.splitlines():
        for kind in ('task received', 'task started', 'task succeeded',
                     'task failed', 'heartbeat', 'started', 'shutdown'):
            if f'] {kind}' in line:
                types.add(kind)
    return types


def test_events_dump_same_event_types(worker):
    reference = _dump_events(REF_VENV, worker.env, worker.env)
    rust = _dump_events(RS_VENV, worker.env, worker.env)
    assert 'task succeeded' in _event_types(reference), reference
    assert _event_types(rust) == _event_types(reference), rust


# ---------------------------------------------------------------------------
# purge con prompt
# ---------------------------------------------------------------------------

def test_purge_prompt_no_and_yes(queue):
    """Prompt identico; ``no`` aborta, ``yes`` svuota e conta i messaggi."""
    results = {}
    queues = {'ref': unique('purge-ref'), 'rs': unique('purge-rs')}
    venvs = {'ref': REF_VENV, 'rs': RS_VENV}
    for label in ('ref', 'rs'):
        target = queues[label]
        env = app_env(queue=target)
        broker = env['CELERY_RS_CLI_BROKER']
        for _ in range(2):
            published = run_cli(REF_VENV, [
                '-A', 'cli_app', '-b', broker, 'call', 'cli.add',
                '-a', '[1, 2]', '--queue', target], env)
            assert published.returncode == 0, published.stderr

        denied = run_cli(venvs[label], [
            '-A', 'cli_app', '-b', broker, 'purge', '-Q', target],
            env, stdin='no\n')
        accepted = run_cli(venvs[label], [
            '-A', 'cli_app', '-b', broker, 'purge', '-Q', target],
            env, stdin='yes\n')
        results[label] = (
            denied.returncode,
            normalize(denied.stdout + denied.stderr).replace(target, '<queue>'),
            accepted.returncode,
            normalize(accepted.stdout + accepted.stderr).replace(target, '<queue>'),
        )
    assert results['rs'] == results['ref']
    assert 'Aborted!' in results['rs'][1]
    assert 'Purged 2 messages from 1 known task queue.' in results['rs'][3]


# ---------------------------------------------------------------------------
# inspect con 2 worker / control -d
# ---------------------------------------------------------------------------

def test_inspect_two_workers_order_and_limit(two_workers):
    first, second = two_workers
    nodes = sorted([first.nodename, second.nodename])
    destinations = f'{first.nodename},{second.nodename}'
    outputs = {}
    for label, venv in (('ref', REF_VENV), ('rs', RS_VENV)):
        result = run_cli(venv, [
            '-A', 'cli_app', '-b', first.env['CELERY_RS_CLI_BROKER'],
            'inspect', 'ping', '-d', destinations, '-t', '2'], second.env)
        assert result.returncode == 0, result.stderr
        blocks = sorted(
            line for line in normalize_lines(result.stdout, venvs=(REF_VENV, RS_VENV))
            if line.startswith('->'))
        single = run_cli(venv, [
            '-A', 'cli_app', '-b', first.env['CELERY_RS_CLI_BROKER'],
            'inspect', 'ping', '-d', first.nodename, '-t', '2'], second.env)
        outputs[label] = (blocks, normalize_lines(
            single.stdout, venvs=(REF_VENV, RS_VENV)))
    assert outputs['rs'] == outputs['ref']
    assert len(outputs['rs'][0]) == 2
    assert len([line for line in outputs['rs'][1] if line.startswith('->')]) == 1

    probe = (
        'import json\n'
        'from celery import Celery\n'
        "app = Celery('cli_probe', broker=__import__('os').environ['CELERY_RS_CLI_BROKER'])\n"
        f'reply = app.control.inspect(timeout=2, limit=1).ping(destination={nodes!r})\n'
        'print(json.dumps({"count": len(reply or {}), "nodes": sorted(reply or {})}))\n'
    )
    limits = {}
    for label, venv in (('ref', REF_VENV), ('rs', RS_VENV)):
        result = subprocess.run(
            [venv_python(venv), '-P', '-c', probe],
            cwd=TESTS_DIR, env=second.env, capture_output=True, text=True,
            timeout=60)
        assert result.returncode == 0, result.stderr
        limits[label] = json.loads(result.stdout.strip().splitlines()[-1])
    # `limit=1` ferma la raccolta dopo un messaggio; il riferimento può
    # consegnarne due nello stesso `drain_events` (il limite conta i giri).
    assert limits['rs']['count'] == 1
    assert 1 <= limits['ref']['count'] <= 2
    assert set(limits['rs']['nodes']) <= set(nodes)


def test_control_destination_multiple_nodes(two_workers):
    first, second = two_workers
    destinations = f'{first.nodename},{second.nodename}'
    outputs = {}
    for label, venv in (('ref', REF_VENV), ('rs', RS_VENV)):
        result = run_cli(venv, [
            '-A', 'cli_app', '-b', first.env['CELERY_RS_CLI_BROKER'],
            'control', 'rate_limit', 'cli.add', '100/s',
            '-d', destinations, '-t', '2'], second.env)
        outputs[label] = (
            result.returncode,
            sorted(line for line in normalize_lines(
                result.stdout, venvs=(REF_VENV, RS_VENV))
                if line.startswith('->')))
    assert outputs['rs'] == outputs['ref']
    assert len(outputs['rs'][1]) == 2


# ---------------------------------------------------------------------------
# broker irraggiungibile
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('args', [
    ['status', '-t', '0.2'],
    ['inspect', 'ping', '-t', '0.2'],
    ['purge', '-f', '-Q', 'cli-unreachable'],
])
def test_unreachable_broker_same_error(args):
    outputs = {}
    for label, venv in (('ref', REF_VENV), ('rs', RS_VENV)):
        env = app_env(broker=UNREACHABLE)
        result = run_cli(venv, ['-A', 'cli_app', '-b', UNREACHABLE, *args],
                         env, timeout=60)
        lines = [line for line in normalize(
            result.stderr, venvs=(REF_VENV, RS_VENV)).splitlines() if line.strip()]
        outputs[label] = (result.returncode, lines[-1] if lines else '')
    assert outputs['rs'] == outputs['ref']


# ---------------------------------------------------------------------------
# import presenti in sys.modules
# ---------------------------------------------------------------------------

IMPORT_PROBE = """
import json, runpy, sys
sys.argv = ['celery'] + sys.argv[1:]
try:
    runpy.run_module('celery', run_name='__main__')
except SystemExit:
    pass
interesting = ('_celery_rs', 'kombu.transport.redis', 'kombu.transport.pyamqp',
               'redis', 'pyamqp')
print(json.dumps({name: name in sys.modules for name in interesting}))
"""


def _imports(venv, args, env):
    result = subprocess.run(
        [venv_python(venv), '-P', '-c', IMPORT_PROBE, *args],
        cwd=TESTS_DIR, env=env, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_imports_help_status_call(worker):
    env = worker.env
    cases = {
        'help': ['--help'],
        'status': ['-A', 'cli_app', '-b', env['CELERY_RS_CLI_BROKER'],
                   'status', '-t', '0.2'],
        'call': ['-A', 'cli_app', '-b', env['CELERY_RS_CLI_BROKER'],
                 'call', 'cli.add', '-a', '[1, 2]'],
    }
    for name, args in cases.items():
        ref = _imports(REF_VENV, args, env)
        rs = _imports(RS_VENV, args, env)
        # Nessun trasporto kombu/redis-py in più del riferimento.
        for module in ('kombu.transport.redis', 'redis'):
            assert not rs[module] or ref[module], f'{name}: {module} in rs'
        if name == 'help':
            assert not ref['_celery_rs'] and not rs['_celery_rs'], (
                '--help non deve caricare _celery_rs')
        else:
            assert rs['_celery_rs'], f'{name} deve usare il core Rust'
        assert not rs['kombu.transport.redis'], (
            f'{name}: il trasporto kombu non deve essere caricato')
        assert not rs['redis'], f'{name}: redis-py non deve essere caricato'


# ---------------------------------------------------------------------------
# control shutdown (ultimo: spegne i worker dedicati)
# ---------------------------------------------------------------------------

def test_control_shutdown_last(queue):
    first = Worker(REF_VENV, f'stop-w1@{HOST}', unique('cli-stop')).start()
    second = Worker(REF_VENV, f'stop-w2@{HOST}', unique('cli-stop')).start()
    try:
        outputs = {}
        for label, venv, target in (
                ('ref', REF_VENV, second.nodename),
                ('rs', RS_VENV, first.nodename)):
            result = run_cli(venv, [
                '-A', 'cli_app', '-b', first.env['CELERY_RS_CLI_BROKER'],
                'control', 'shutdown', '-d', target, '-t', '2'], first.env)
            text = normalize(result.stdout, venvs=(REF_VENV, RS_VENV))
            text = re.sub(r'stop-w\d+', '<node>', text)
            outputs[label] = (
                result.returncode,
                sorted(line for line in text.splitlines()
                       if line.strip().startswith('->')),
            )
        assert outputs['rs'] == outputs['ref']
        assert first.process.wait(timeout=30) is not None
        assert second.process.wait(timeout=30) is not None
    finally:
        first.stop(signal.SIGKILL)
        second.stop(signal.SIGKILL)


def test_lazy_names_are_the_real_objects():
    """I nomi caricati su richiesta restano gli oggetti veri, estendibili."""
    probe = (
        'import sys\n'
        'from celery.bin import celery as c\n'
        'from celery.backends import redis as r\n'
        "assert 'redis' not in sys.modules\n"
        'class Conn(r.SentinelManagedSSLConnection):\n'
        '    extra = 1\n'
        'import redis, celery.bin.worker as w\n'
        'assert issubclass(Conn, redis.SSLConnection) and type(Conn) is type\n'
        'assert c.worker is w.worker and c.list_.name == "list"\n'
        "assert {'worker', 'beat', 'list_'} <= set(dir(c))\n"
        'from click_didyoumean import DYMGroup\n'
        'assert isinstance(c.celery, DYMGroup)\n'
    )
    result = subprocess.run([venv_python(RS_VENV), '-P', '-c', probe], cwd=TESTS_DIR,
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
