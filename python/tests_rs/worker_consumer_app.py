"""App usata dai test del consumer del worker (``test_worker_consumer.py``).

Lo stesso modulo viene importato dal worker ``.venv-ref`` (Celery originale) e
da ``.venv-rs`` (celery-rs): i nomi dei task e le opzioni con cui sono
registrati sono l'unico contratto fra i due processi.

Tutto è configurabile da ambiente perché i test avviano worker con broker,
coda, backend e opzioni diversi.
"""
import os
import time

from celery import Celery

BROKER = os.environ['CELERY_RS_TEST_WORKER_BROKER']
BACKEND = os.environ.get(
    'CELERY_RS_TEST_WORKER_BACKEND', 'redis://127.0.0.1:6390/5')
QUEUE = os.environ.get('CELERY_RS_TEST_WORKER_QUEUE', 'rs-worker-tests')


def _bool(name, default):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in ('0', 'false', 'no', 'off')


app = Celery('worker_consumer_app', broker=BROKER, backend=BACKEND)
app.conf.update(
    task_default_queue=QUEUE,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    task_track_started=True,
    result_expires=120,
    worker_hijack_root_logger=False,
    broker_connection_retry_on_startup=True,
    worker_prefetch_multiplier=int(
        os.environ.get('CELERY_RS_TEST_PREFETCH_MULTIPLIER', '1')),
    task_queue_max_priority=10,
    task_acks_on_failure_or_timeout=_bool(
        'CELERY_RS_TEST_ACKS_ON_FAILURE', True),
    worker_soft_shutdown_timeout=float(
        os.environ.get('CELERY_RS_TEST_SOFT_SHUTDOWN', '0')),
    broker_transport_options={},
)


@app.task(name='rs_worker.add')
def add(x, y):
    return x + y


@app.task(name='rs_worker.big')
def big(payload, **kwargs):
    """Args/kwargs grandi: il body deve sopravvivere al trasporto intatto."""
    return {
        'len': len(payload),
        'keys': sorted(kwargs),
        'checksum': sum(payload) if isinstance(payload, list) else 0,
    }


@app.task(name='rs_worker.marker')
def marker(path, label='marker', delay=0.0):
    """Scrive *label* in *path*: verifica l'ordine/l'esecuzione dei task."""
    with open(path, 'a', encoding='utf-8') as handle:
        handle.write(f'{label}\n')
    if delay:
        time.sleep(delay)
    return label


@app.task(name='rs_worker.sleep_add')
def sleep_add(seconds, x, y):
    time.sleep(seconds)
    return x + y


@app.task(name='rs_worker.fail')
def fail(message):
    raise ValueError(message)


@app.task(name='rs_worker.retry', bind=True, max_retries=3,
          default_retry_delay=0.2)
def retry_task(self, x):
    if self.request.retries < 2:
        raise self.retry(countdown=0.2)
    return x


@app.task(name='rs_worker.rate', rate_limit='2/s')
def rate(x):
    return x


@app.task(name='rs_worker.acks_late_fail_once', bind=True, acks_late=True,
          reject_on_worker_lost=True, max_retries=0)
def acks_late_fail_once(self, path, x):
    """Fallisce al primo tentativo; con acks_late il broker lo riconsegna."""
    if os.path.exists(path):
        return x
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write('first\n')
    raise RuntimeError('primo tentativo')


@app.task(name='rs_worker.wait_file', acks_late=True)
def wait_file(path, seconds=60):
    deadline = time.monotonic() + seconds
    while not os.path.exists(path) and time.monotonic() < deadline:
        time.sleep(0.02)
    return os.path.exists(path)


@app.task(name='rs_worker.acks_late_slow', acks_late=True,
          reject_on_worker_lost=True)
def acks_late_slow(seconds, x):
    time.sleep(seconds)
    return x


@app.task(name='rs_worker.reject_once', bind=True, acks_late=True)
def reject_once(self, path, x):
    """Rimette in coda il messaggio al primo tentativo (``Reject``)."""
    from celery.exceptions import Reject
    if os.path.exists(path):
        return x
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write('first\n')
    raise Reject('riprova', requeue=True)


@app.task(name='rs_worker.marker_slow', acks_late=True,
          reject_on_worker_lost=True)
def marker_slow(path, seconds, x):
    """Segnala l'inizio (file) e dorme: usata nei test di shutdown."""
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write('started\n')
    time.sleep(seconds)
    return x
