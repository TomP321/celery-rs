"""App usata da ``bench/bench_result.py`` (worker di riferimento)."""
import os

from celery import Celery

BROKER = os.environ.get('BENCH_BROKER', os.environ.get('CELERY_RS_AMQP_URL', 'amqp://guest:guest@127.0.0.1:5673//'))
BACKEND = os.environ.get('BENCH_RESULT_BACKEND', 'rpc://')
QUEUE = os.environ.get('BENCH_QUEUE', 'bench-result')

app = Celery('bench_result', broker=BROKER, backend=BACKEND)
app.conf.update(
    task_default_queue=QUEUE,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    worker_hijack_root_logger=False,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
)


@app.task(name='bench_result.add')
def add(x, y):
    return x + y


@app.task(name='bench_result.noop')
def noop():
    return 1
