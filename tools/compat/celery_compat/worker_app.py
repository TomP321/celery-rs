"""App Celery usata da `compat wire-worker`: gira nel venv del worker sotto esame (ref o rs).

Broker e backend arrivano da COMPAT_BROKER / COMPAT_BACKEND. I task sono deterministici:
stessi argomenti → stessi risultati, eccezioni e retry in entrambi i venv.
"""
import os
import time

from celery import Celery

app = Celery('compat_worker', broker=os.environ['COMPAT_BROKER'], backend=os.environ['COMPAT_BACKEND'])
app.conf.update(
    worker_hijack_root_logger=False,
    result_extended=False,
    task_track_started=True,
)


@app.task(name='compat.add')
def add(x, y):
    return x + y


@app.task(name='compat.fail')
def fail(message):
    raise ValueError(message)


@app.task(name='compat.retry_once', bind=True, max_retries=3)
def retry_once(self, value):
    if self.request.retries == 0:
        raise self.retry(countdown=0.2)
    return value


@app.task(name='compat.sleep')
def sleep(seconds):
    time.sleep(seconds)
    return seconds


@app.task(name='compat.late', acks_late=True)
def late(value):
    return value


@app.task(name='compat.tsum')
def tsum(values):
    return sum(values)
