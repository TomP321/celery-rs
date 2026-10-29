"""Import redis-py before fork to model a typical application."""
import os
from urllib.parse import urlsplit

import redis

from benchapp import app, COUNTER

_counter = None


@app.task(ignore_result=True)
def noop_toplevel():
    global _counter
    if _counter is None:
        url = urlsplit(os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390'))
        _counter = redis.Redis(host=url.hostname, port=url.port, db=2)
    _counter.incr(COUNTER)
