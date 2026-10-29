"""Provide the CLI benchmark app without importing redis-py.

Broker, backend and queue come from the environment. A reference worker
keeps the CLI as the only measured variable."""
import os

from celery import Celery

BROKER = os.environ.get('BENCH_CLI_BROKER', os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/0')
BACKEND = os.environ.get('BENCH_CLI_BACKEND', os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/1')
QUEUE = os.environ.get('BENCH_CLI_QUEUE', 'bench-cli')

app = Celery('bench_cli', broker=BROKER, backend=BACKEND)
app.conf.update(
    task_default_queue=QUEUE,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    result_expires=300,
    worker_hijack_root_logger=False,
    broker_connection_retry_on_startup=True,
    broker_transport_options={},
)


@app.task(name='bench_cli.add')
def add(x, y):
    return x + y
