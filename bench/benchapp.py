"""App Celery usata dal benchmark. Lo stesso file gira con Celery di riferimento e con celery-rs."""
import os

import redis
from celery import Celery

BROKER = os.environ.get('BENCH_BROKER', 'redis://127.0.0.1:6390/0')
BACKEND = os.environ.get('BENCH_BACKEND', 'redis://127.0.0.1:6390/1')
COUNTER = 'bench:done'

app = Celery('bench', broker=BROKER, backend=BACKEND)
app.conf.update(
    worker_hijack_root_logger=False,
    broker_connection_retry_on_startup=True,
)

_counter = None


def counter():
    global _counter
    if _counter is None:
        _counter = redis.Redis(host='127.0.0.1', port=6390, db=2)
    return _counter


@app.task(ignore_result=True)
def noop():
    counter().incr(COUNTER)


@app.task
def add(x, y):
    return x + y
