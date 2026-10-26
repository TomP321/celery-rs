"""App Celery usata dai test ``rpc://`` con il worker del riferimento.

Il modulo viene importato dal worker ``.venv-ref`` (Celery originale) e dal
client ``.venv-rs``; i nomi dei task sono quindi l'unico contratto fra i due.
"""
import os
import time

from celery import Celery

BROKER = os.environ.get('CELERY_RS_AMQP_URL', os.environ.get('CELERY_RS_TEST_AMQP', 'amqp://guest:guest@127.0.0.1:5673//'))
QUEUE = os.environ.get('CELERY_RS_TEST_RPC_QUEUE', 'celery-rs-rpc-tests')

app = Celery('rpc_worker_app', broker=BROKER, backend='rpc://')
app.conf.update(
    task_default_queue=QUEUE,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    task_track_started=True,
    worker_hijack_root_logger=False,
    broker_connection_retry_on_startup=True,
)


@app.task(name='rs_rpc.add')
def add(x, y):
    return x + y


@app.task(name='rs_rpc.fail')
def fail(message):
    raise ValueError(message)


@app.task(name='rs_rpc.sleep_add')
def sleep_add(seconds, x, y):
    time.sleep(seconds)
    return x + y


@app.task(name='rs_rpc.progress')
def progress(steps, x, y):
    for step in range(steps):
        progress.update_state(
            state='PROGRESS', meta={'step': step, 'total': steps})
        time.sleep(0.15)
    return x + y


@app.task(name='rs_rpc.wait_file')
def wait_file(path, x, y):
    """Attende un file: il risultato non arriva prima che il client lo crei."""
    deadline = time.monotonic() + 60
    while not os.path.exists(path) and time.monotonic() < deadline:
        time.sleep(0.05)
    return x + y
