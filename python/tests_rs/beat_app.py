"""App condivisa dai test di beat (``.venv-rs`` e ``.venv-ref``).

Lo stesso modulo viene importato dal beat e dal worker del riferimento e di
celery-rs: nomi dei task, schedule e opzioni sono l'unico contratto fra i
processi. Tutto è configurabile da ambiente.
"""
import atexit
import json
import os

from celery import Celery
from celery.schedules import crontab, schedule
from kombu import Producer as KombuProducer

from celery.beat import PersistentScheduler

BROKER = os.environ.get(
    'BEAT_TEST_BROKER', 'redis://127.0.0.1:6390/11')
QUEUE = os.environ.get('BEAT_TEST_QUEUE', 'beat-tests')
SINK_KEY = os.environ.get('BEAT_TEST_SINK_KEY', 'beat-tests:sink')
MODE = os.environ.get('BEAT_TEST_MODE', 'interval')
TAG = os.environ.get('BEAT_TEST_TAG', '')

app = Celery('beat_app', broker=BROKER)
app.conf.update(
    task_default_queue=QUEUE,
    task_ignore_result=True,
    task_serializer='json',
    accept_content=['json'],
    worker_hijack_root_logger=False,
    worker_concurrency=1,
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=int(
        os.environ.get('BEAT_TEST_MAX_RETRIES', '100')),
    beat_max_loop_interval=0.1,
    beat_sync_every=60,
)


def _sink():
    import redis
    return redis.Redis.from_url(BROKER)


@app.task(name='beattest.record', ignore_result=True)
def record(*args, **kwargs):
    request = record.request
    expires = request.expires
    if expires is not None and hasattr(expires, 'isoformat'):
        expires = expires.isoformat()
    data = {
        'task': request.task,
        'args': list(args),
        'kwargs': kwargs,
        'priority': (request.delivery_info or {}).get('priority'),
        'expires': None if expires is None else str(expires),
        'headers': request.headers or {},
    }
    _sink().rpush(SINK_KEY, json.dumps(data, sort_keys=True, default=str))


def _make_schedule():
    if MODE == 'interval':
        return {
            'every-300ms': {
                'task': 'beattest.record',
                'schedule': 0.3,
                'args': ('interval', TAG),
            },
            'relative-500ms': {
                'task': 'beattest.record',
                'schedule': schedule(0.5, relative=True),
                'args': ('relative', TAG),
                'kwargs': {'tag': 'relative'},
            },
        }
    if MODE == 'options':
        return {
            'with-options': {
                'task': 'beattest.record',
                'schedule': 0.25,
                'args': ('options', TAG),
                'options': {
                    'queue': QUEUE,
                    'priority': 7,
                    'expires': 30,
                    'headers': {'x-extra': 'hi'},
                },
            },
        }
    if MODE == 'missing':
        return {
            'unregistered': {
                'task': 'beattest.unregistered',
                'schedule': 0.3,
                'args': ('missing', TAG),
            },
        }
    if MODE == 'crontab':
        return {
            'cron-every-minute': {
                'task': 'beattest.record',
                'schedule': crontab(minute='*'),
                'args': ('cron', TAG),
            },
        }
    raise RuntimeError(f'BEAT_TEST_MODE sconosciuto: {MODE!r}')


app.conf.beat_schedule = _make_schedule()


class RecordingProducer(KombuProducer):
    """Producer personalizzato: deve restare sul percorso kombu."""


class RecordingScheduler(PersistentScheduler):
    """Scheduler personalizzato con ``Producer`` ridefinito."""

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('Producer', RecordingProducer)
        super().__init__(*args, **kwargs)

    def apply_entry(self, entry, producer=None):
        path = os.environ.get('BEAT_TEST_PRODUCER_FILE')
        if path and not os.path.exists(path):
            producer = self.producer
            with open(path, 'w', encoding='utf-8') as handle:
                handle.write(type(producer).__name__)
        return super().apply_entry(entry, producer=producer)


_MODS_OUT = os.environ.get('BEAT_TEST_MODS_OUT')
if _MODS_OUT:
    @atexit.register
    def _write_modules():
        import sys
        modules = {
            'redis': 'redis' in sys.modules,
            'redis_transport': 'kombu.transport.redis' in sys.modules,
            'pyamqp_transport': 'kombu.transport.pyamqp' in sys.modules,
        }
        with open(_MODS_OUT, 'w', encoding='utf-8') as handle:
            json.dump(modules, handle)
