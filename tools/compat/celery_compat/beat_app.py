"""App Celery usata da `compat wire-beat`: gira nel venv del beat sotto esame (ref o rs).

Solo schedule a intervallo breve: in pochi secondi ogni voce scatta più volte, in modo
deterministico nella forma (non nel numero esatto) dei messaggi. I crontab sono verificati dai
test differenziali, non qui: scatterebbero solo a cavallo di un minuto.
"""
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
