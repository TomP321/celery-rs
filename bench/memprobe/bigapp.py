import os
from celery import Celery
app = Celery('bigapp', broker=os.environ['BROKER'])

@app.task
def big(n):
    data = [str(i) * 8 for i in range(n)]  # molti oggetti piccoli: heap Python/glibc
    return len(data)

_cache = []


@app.task
def fragment(n):
    data = [str(i) * 8 for i in range(n)]
    _cache.extend(data[::200])  # pochi sopravvissuti sparsi, come una cache
    return len(data)
