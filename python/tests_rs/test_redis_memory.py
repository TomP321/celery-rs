"""Regressione: le pagine libere dei buffer RESP grandi tornano al sistema."""
import ctypes
import subprocess
import sys

import pytest


@pytest.mark.skipif(sys.platform != 'linux' or not hasattr(ctypes.CDLL(None), 'malloc_trim'),
                    reason='politica delle arene glibc su Linux')
def test_large_result_pipeline_does_not_retain_scratch_pages():
    code = r'''
import json, os, uuid
from urllib.parse import urlsplit
import _celery_rs as rs
address = urlsplit(os.environ['CELERY_RS_REDIS_URL'])
client = rs.RedisClient(host=address.hostname, port=address.port, db=14)
key = '09b-large-' + uuid.uuid4().hex
payload = b'x' * (1024 * 1024)
def pss():
    with open('/proc/self/smaps_rollup') as source:
        return next(int(line.split()[1]) for line in source if line.startswith('Pss:'))
client.setex(key, 60, b'warmup')
baseline = pss()
try:
    for _ in range(100):
        pipe = client.pipeline(True)
        pipe.setex(key, 60, payload)
        pipe.publish(key, payload)
        assert pipe.execute() == [True, 0]
    final = pss()
    print(json.dumps({'baseline_kib': baseline, 'final_kib': final}), flush=True)
    # Prima del fix restavano residenti 4–13 MiB di buffer già liberati.
    assert final - baseline < 3 * 1024, (baseline, final)
finally:
    client.delete([key])
'''
    result = subprocess.run([sys.executable, '-c', code], capture_output=True,
                            text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.skipif(sys.platform != 'linux' or not hasattr(ctypes.CDLL(None), 'malloc_trim'),
                    reason='politica delle arene glibc su Linux')
def test_large_result_releases_python_encoding_pages():
    code = r'''
import gc, json, os, uuid
from celery import Celery
app = Celery('memory', broker='memory://', backend=os.environ['CELERY_RS_REDIS_URL'] + '/14')
backend = app.backend
key = '09b-memory-' + uuid.uuid4().hex
payload = 'x' * (1024 * 1024)
def pss():
    with open('/proc/self/smaps_rollup') as source:
        return next(int(line.split()[1]) for line in source if line.startswith('Pss:'))
backend.get(backend.get_key_for_task(key))
baseline = pss()
try:
    for _ in range(100):
        backend.store_result(key, payload, 'SUCCESS')
        backend.forget(key)
    gc.collect()
    final = pss()
    print(json.dumps({'baseline_kib': baseline, 'final_kib': final}), flush=True)
    assert final - baseline < 4 * 1024, (baseline, final)
finally:
    backend.forget(key)
'''
    result = subprocess.run([sys.executable, '-c', code], capture_output=True,
                            text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
