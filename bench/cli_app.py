"""App Celery per ``bench/bench_cli.py`` (nessun import di redis-py).

Broker, backend e coda arrivano dall'ambiente: il worker è sempre quello del
riferimento, così il benchmark misura solo la CLI.
"""
import os

from celery import Celery

BROKER = os.environ.get('BENCH_CLI_BROKER', 'redis://127.0.0.1:6390/0')
BACKEND = os.environ.get('BENCH_CLI_BACKEND', 'redis://127.0.0.1:6390/1')
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
