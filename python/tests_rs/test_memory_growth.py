"""La diagnostica resta opzionale e separa memoria Python e nativa."""
import importlib.util
from pathlib import Path
import pickle
import subprocess
import sys

import pytest


def test_soak_interpreter_follows_impl(monkeypatch, tmp_path):
    path = Path(__file__).resolve().parents[2] / 'bench/soak.py'
    spec = importlib.util.spec_from_file_location('soak', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    from types import SimpleNamespace
    commands = []

    def spawn(command, **kwargs):
        commands.append(command)
        raise RuntimeError('arresto prima di creare processi')

    monkeypatch.setattr(module, 'OUT', tmp_path)
    monkeypatch.setattr(module.subprocess, 'Popen', spawn)
    monkeypatch.setattr(module.subprocess, 'call', lambda command: commands.append(command) or 0)
    monkeypatch.setenv('CELERY_RS_REDIS_URL', 'redis://localhost:6390')
    monkeypatch.setenv('CELERY_RS_AMQP_URL', 'amqp://guest:guest@localhost:5673//')
    monkeypatch.setenv('CELERY_RS_LANE', '0')
    for impl in ('ref', 'rs'):
        executable = str(path.parents[1] / f'.venv-{impl}/bin/python')
        assert module.interpreter(impl) == executable
        args = SimpleNamespace(impl=impl, broker='redis', minutes=1, sample_seconds=30)
        monkeypatch.delenv('CELERY_RS_BROKER_LOCKED', raising=False)
        assert module.run(args) == 0
        assert commands[-1][1] == executable
        monkeypatch.setenv('CELERY_RS_BROKER_LOCKED', '1')
        assert module.run(args) == 1
        assert commands[-1][0] == executable


def test_memory_probe_records_components(tmp_path):
    code = '''
import json, os, sys
from pathlib import Path
from celery.concurrency._rs_memory_probe import MemoryProbe
probe = MemoryProbe(sys.argv[1])
probe.sample(1)
records = [json.loads(line) for line in next(Path(sys.argv[1]).glob('*.jsonl')).read_text().splitlines()]
assert [record['completed'] for record in records] == [0, 1]
record = records[-1]
assert record['completed'] == 1
assert record['memory_kib']['Rss'] > 0
assert len(record['python_bytes']) == 2
assert record['uss_kib'] > 0
assert record['mappings_kib']['heap']['Private_Dirty'] > 0
assert next(Path(sys.argv[1]).glob('*.snapshot')).stat().st_size > 0
assert next(Path(sys.argv[1]).glob('*.malloc.xml')).read_text().startswith('<malloc')
'''
    result = subprocess.run([sys.executable, '-c', code, str(tmp_path)],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


def test_completed_traceback_releases_frames_without_gc():
    import gc
    import weakref
    from billiard.einfo import ExceptionInfo
    from celery.concurrency._rs_child import _clear_result_traceback

    class Payload:
        pass

    def failed_trace():
        payload = Payload()
        reference = weakref.ref(payload)
        try:
            raise ValueError('failure')
        except ValueError:
            info = ExceptionInfo()
        return reference, info

    enabled = gc.isenabled()
    gc.disable()
    try:
        reference, info = failed_trace()
        assert reference() is not None
        serialized = pickle.dumps(info)
        _clear_result_traceback((True, (1, info, 0.01)))
        assert reference() is None
        assert pickle.dumps(info) == serialized
        reference, info = failed_trace()
        _clear_result_traceback((False, info))
        assert reference() is None
    finally:
        if enabled:
            gc.enable()


@pytest.mark.parametrize('kind', ['failure', 'retry', 'reject', 'ignore'])
def test_real_task_failure_drops_exception_info_without_collection(kind):
    import gc
    import weakref
    from celery import Celery
    from celery.app.trace import build_tracer, fast_trace_task
    from celery.exceptions import Ignore, Reject, Retry
    from celery.concurrency._rs_child import _clear_result_traceback

    app = Celery('trace-memory', broker='memory://', backend='cache+memory://')

    @app.task(shared=False, ignore_result=True)
    def fail():
        errors = {'failure': ValueError, 'retry': Retry, 'reject': Reject, 'ignore': Ignore}
        raise errors[kind]('failure')

    fail.__trace__ = build_tracer(fail.name, fail, app=app)
    enabled = gc.isenabled()
    gc.disable()
    try:
        for index in range(20):
            result = fast_trace_task(fail.name, str(index), {}, ((), {}, {}),
                                     None, None, _loc=({fail.name: fail}, None, 'probe'))
            reference = weakref.ref(result[1])
            _clear_result_traceback((True, result))
            del result
            assert reference() is None
    finally:
        if enabled:
            gc.enable()
        app.close()
