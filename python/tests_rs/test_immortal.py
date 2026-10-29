"""Isolate irreversible immortalization in dedicated processes."""
import os
import subprocess
import sys

import pytest


def run(code, **env):
    subprocess.run([sys.executable, '-c', code], check=True,
                   env={**os.environ, **env}, timeout=60)


def test_objects_and_exclusions():
    run('''
import gc, sys, sysconfig
from _celery_rs import _immortalize, Waker, CronParseError
supported = (sys.implementation.name == 'cpython' and
             (3, 12) <= sys.version_info[:2] <= (3, 14) and
             sys.maxsize > 2**32 and sys.byteorder == 'little' and
             not sysconfig.get_config_var('Py_GIL_DISABLED'))
destroyed = []
class Finalized:
    def __del__(self): destroyed.append(True)
obj = [''.join(['a', 'dynamic', 'string'])]
finalized = Finalized()
native_error = CronParseError('x')
error_refs = sys.getrefcount(native_error)
native = Waker()
import _celery_rs
del _celery_rs.Waker
a, b = sys.getrefcount(finalized), sys.getrefcount(native)
# Untracked tuples must also expose their referents.
text = ''.join(['another', 'dynamic', 'string'])
tuple_obj = (text,)
gc.collect()
assert not gc.is_tracked(tuple_obj)
count = _immortalize([obj, finalized, native, native_error, tuple_obj])
assert sys.getrefcount(finalized) == a
assert sys.getrefcount(native) == b
assert sys.getrefcount(native_error) == error_refs
if supported:
    assert count >= 4
    assert sys.getrefcount(obj) >= 2**31
    assert sys.getrefcount(obj[0]) >= 2**31
    assert sys.getrefcount(text) >= 2**31
    assert _immortalize([obj, tuple_obj]) == 0
else:
    assert count == 0
del finalized
gc.collect()
assert destroyed == [True]
fresh = []
assert sys.getrefcount(fresh) < 100
cycle = []
cycle.append(cycle)
if supported:
    assert _immortalize([cycle]) == 1
    assert _immortalize([cycle]) == 0
''', CELERY_RS_IMMORTALIZE='1')


def test_disabled():
    run('''
import sys
from _celery_rs import _immortalize
obj = []
before = sys.getrefcount(obj)
assert _immortalize([obj]) == 0
assert sys.getrefcount(obj) == before
''', CELERY_RS_IMMORTALIZE='0')


@pytest.mark.skipif(sys.platform != 'linux' or sys.version_info < (3, 12),
                    reason='Linux PSS and PEP 683')
def test_fork_private_pages():
    run('''
import os, sys
from _celery_rs import _immortalize
objects = [[str(i)] for i in range(40000)]
def private():
    with open('/proc/self/smaps_rollup') as f:
        return sum(int(line.split()[1]) for line in f
                   if line.startswith(('Private_Clean:', 'Private_Dirty:')))
def measure():
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:
        before = private()
        for obj in objects:
            for text in obj:
                pass
        delta = private() - before
        os.write(w, str(delta).encode())
        os._exit(0)
    os.close(w)
    value = int(os.read(r, 100))
    os.close(r)
    os.waitpid(pid, 0)
    return value
normal = measure()
count = _immortalize(objects)
if count:
    shared = measure()
    assert normal > 2000, normal
    assert shared < normal * 0.6, (normal, shared)
''', CELERY_RS_IMMORTALIZE='1')


@pytest.mark.parametrize('unsupported', ['version', 'free_threaded', 'trace_refs'])
def test_unsupported_layout(unsupported):
    run('''
import sys, sysconfig
from _celery_rs import _immortalize
case = %r
if case == 'version':
    sys.version_info = (3, 11, 0)
else:
    original = sysconfig.get_config_var
    name = 'Py_GIL_DISABLED' if case == 'free_threaded' else 'Py_TRACE_REFS'
    sysconfig.get_config_var = lambda key: 1 if key == name else original(key)
obj = []
before = sys.getrefcount(obj)
assert _immortalize([obj]) == 0
assert sys.getrefcount(obj) == before
''' % unsupported, CELERY_RS_IMMORTALIZE='1')
