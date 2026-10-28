"""GC prima del fork e rilascio dei buffer nel loop reale del figlio."""
import json
import os
import subprocess
import sys

import pytest


def isolated(source, **environment):
    process = subprocess.run([sys.executable, '-c', source],
                             env=dict(os.environ, CELERY_RS_IMMORTALIZE='0', **environment),
                             capture_output=True, text=True, timeout=45)
    assert process.returncode == 0, process.stdout + process.stderr
    return json.loads(process.stdout.splitlines()[-1])


@pytest.mark.parametrize('enabled', [True, False])
def test_collect_before_freeze_preserves_gc_and_child_signals(enabled):
    data = isolated('''
import gc, json, os, time, weakref
from celery import Celery, signals
from celery.concurrency.rs_prefork import RsTaskPool
app = Celery('fork-memory', broker='memory://')
app.finalize()
gc.set_threshold(123, 7, 5)
gc.disable()
finalized = []
class Cycle:
    def __del__(self): finalized.append(os.getpid())
value = Cycle()
value.self = value
ref = weakref.ref(value)
del value
assert ref() is not None
if os.environ['GC_ENABLED'] == '1': gc.enable()
init_pid = None
@signals.worker_process_init.connect(weak=False)
def initialized(**kwargs):
    global init_pid
    init_pid = os.getpid()
def state():
    return os.getpid(), init_pid, gc.isenabled(), gc.get_threshold(), gc.get_freeze_count()
pool = RsTaskPool(1, app=app, initargs=(app, 'memory@test'))
pool.start()
try:
    assert ref() is None
    assert finalized == [os.getpid()]
    assert gc.isenabled() == (os.environ['GC_ENABLED'] == '1')
    assert gc.get_threshold() == (123, 7, 5)
    assert gc.get_freeze_count() > 0
    result = pool.apply_async(state)
    deadline = time.monotonic() + 15
    while not result.ready():
        assert time.monotonic() < deadline
        pool.wait(.01)
    print(json.dumps([os.getpid(), result.get()]))
finally:
    pool.stop()
''', GC_ENABLED=str(int(enabled)))
    master, (child, init_pid, gc_enabled, thresholds, frozen) = data
    assert child != master
    assert init_pid == child
    assert gc_enabled is enabled
    assert thresholds == [123, 7, 5]
    assert frozen > 0


def test_trim_each_task_keeps_user_cycles_until_their_gc(tmp_path):
    data = isolated('''
import gc, json, os, time, weakref
import _celery_rs
from celery import Celery
from celery.concurrency.rs_prefork import RsTaskPool
app = Celery('trim-each-task', broker='memory://')
app.finalize()
original = _celery_rs._release_task_memory
def release(force=False):
    with open(os.environ['TRIM_CALLS'], 'a') as out:
        out.write(json.dumps([os.getpid(), force]) + '\\n')
    original(force=force)
_celery_rs._release_task_memory = release
pending = None
def create_cycle():
    global pending
    gc.disable()
    class Cycle: pass
    value = Cycle()
    value.self = value
    pending = weakref.ref(value)
    return os.getpid()
def check_cycle():
    return pending() is not None, gc.isenabled()
def execute(pool, function):
    result = pool.apply_async(function)
    deadline = time.monotonic() + 15
    while not result.ready():
        assert time.monotonic() < deadline
        pool.wait(.01)
    return result.get()
pool = RsTaskPool(1, app=app, initargs=(app, 'trim@test'))
pool.start()
try:
    child = execute(pool, create_cycle)
    first = execute(pool, check_cycle)
    second = execute(pool, check_cycle)
    deadline = time.monotonic() + 5
    while True:
        calls = [json.loads(line) for line in open(os.environ['TRIM_CALLS'])]
        calls = [force for pid, force in calls if pid == child]
        if len(calls) >= 4: break
        assert time.monotonic() < deadline
        time.sleep(.01)
    print(json.dumps([first, second, calls]))
finally:
    pool.stop()
''', TRIM_CALLS=str(tmp_path / 'trim.jsonl'))
    first, second, calls = data
    assert first == second == [True, False]
    assert calls == [True] * 4
