"""Provide the shared reference and Rust app for the 09b soak."""
import os

from celery import Celery

app = Celery('soak', broker=os.environ['SOAK_BROKER'], backend=os.environ['SOAK_BACKEND'])
app.conf.update(
    task_default_queue=os.environ['SOAK_QUEUE'],
    result_expires=120,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=None,
    beat_schedule={
        f'entry-{i}': {'task': 'soak.pure', 'schedule': 1.0 if i < 100 else 10.0 + i % 60,
                       'args': (i,)} for i in range(1000)
    },
)


@app.task(name='soak.pure', ignore_result=True)
def pure(value=0):
    return value + 1


@app.task(name='soak.add')
def add(x, y=1):
    return x + y


@app.task(name='soak.fail')
def fail():
    raise ValueError('soak failure')


@app.task(name='soak.retry', bind=True, max_retries=1)
def retry(self):
    if not self.request.retries:
        raise self.retry(countdown=1)
    return 'retried'


@app.task(name='soak.large')
def large():
    return 'x' * (1024 * 1024)


@app.task(name='soak.total')
def total(values):
    return sum(values)
