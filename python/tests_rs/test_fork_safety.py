"""Regressioni: oggetti raccolti dopo fork e registri senza riferimenti forti."""
import gc
import os
import subprocess
import sys
import weakref


def test_after_fork_registry_releases_objects(monkeypatch):
    from celery.utils import after_fork
    monkeypatch.setattr(after_fork, '_after_forkers', weakref.WeakValueDictionary())
    register_after_fork = after_fork.register_after_fork
    _run_after_forkers = after_fork._run_after_forkers

    class Owner:
        pass

    seen = []
    owner = Owner()
    reference = weakref.ref(owner)
    register_after_fork(owner, lambda obj: seen.append(id(obj)))
    _run_after_forkers()
    assert id(owner) in seen
    del owner
    gc.collect()
    assert reference() is None


def test_native_resources_collected_in_forked_child():
    code = r'''
import faulthandler, gc, os
import _celery_rs as rs
faulthandler.enable()
url = os.environ['CELERY_RS_REDIS_URL'] + '/14'
from urllib.parse import urlsplit
port = urlsplit(url).port
client = rs.RedisClient(port=port, db=14)
client.get('09b-fork-missing')
pubsub = client.pubsub()
pubsub.subscribe(['09b-fork-' + str(os.getpid())])
connection = rs.Connection(url)
waker = rs.Waker()
# Un ciclo ritarda la distruzione fino al GC, come nella suite completa.
cycle = [client, pubsub, connection, waker]
cycle.append(cycle)
del client, pubsub, connection, waker
pid = os.fork()
if pid == 0:
    del cycle
    for _ in range(5):
        gc.collect()
    fresh = rs.RedisClient(port=port, db=14)
    fresh.get('09b-fork-missing')
    os._exit(0)
_, status = os.waitpid(pid, 0)
assert os.waitstatus_to_exitcode(status) == 0, status
cycle[0].get('09b-fork-missing')
del cycle
gc.collect()
'''
    result = subprocess.run([sys.executable, '-c', code], capture_output=True,
                            text=True, timeout=30)
    assert result.returncode == 0, result.stderr
