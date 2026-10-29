"""Provide the wire-beat app in the selected venv.

Short intervals produce repeated messages with deterministic shape, not counts.
Crontab behaviour is covered by differential tests to avoid minute boundaries."""
import os
from datetime import timedelta

from celery import Celery
from celery.schedules import schedule

app = Celery('compat_beat', broker=os.environ['COMPAT_BROKER'])
app.conf.update(
    beat_schedule={
        'add-every-300ms': {
            'task': 'compat.add',
            'schedule': 0.3,
            'args': (2, 3),
        },
        'kwargs-options': {
            'task': 'compat.add',
            'schedule': timedelta(milliseconds=400),
            'kwargs': {'x': 1, 'y': 2},
            'options': {'queue': 'compat_beat_q', 'priority': 5, 'expires': 30,
                        'headers': {'x-compat': 'beat'}},
        },
        'relative': {
            'task': 'compat.tsum',
            'schedule': schedule(timedelta(milliseconds=500), relative=True),
            'args': ([1, 2, 3],),
        },
    },
    beat_max_loop_interval=0.1,
)
