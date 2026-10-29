"""Shared CLI test app with broker, backend and queue configured through the environment."""
import os

from celery import Celery

BROKER = os.environ.get(
    'CELERY_RS_CLI_BROKER', os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/10')
BACKEND = os.environ.get(
    'CELERY_RS_CLI_BACKEND', os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/11')
QUEUE = os.environ.get('CELERY_RS_CLI_QUEUE', 'cli-tests')


def _bool(name, default):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in ('0', 'false', 'no', 'off')


app = Celery('cli_app', broker=BROKER, backend=BACKEND)
app.conf.update(
    task_default_queue=QUEUE,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    task_track_started=True,
    result_expires=120,
    worker_hijack_root_logger=False,
    broker_connection_retry_on_startup=True,
    worker_soft_shutdown_timeout=0,
    broker_transport_options={},
)


@app.task(name='cli.add')
def add(x, y):
    return x + y


@app.task(name='cli.fail')
def fail(message):
    raise ValueError(message)


@app.task(name='cli.event')
def event(value):
    return value
