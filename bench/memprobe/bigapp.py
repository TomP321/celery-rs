import os
from celery import Celery
app = Celery('bigapp', broker=os.environ['BROKER'])

@app.task
def big(n):
    data = [str(i) * 8 for i in range(n)]  # Small objects exercise Python and glibc heaps.
    return len(data)

_cache = []


@app.task
def fragment(n):
    data = [str(i) * 8 for i in range(n)]
    _cache.extend(data[::200])  # Scattered survivors model a cache.
    return len(data)
