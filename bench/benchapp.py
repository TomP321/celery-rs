"""App Celery usata dal benchmark. Lo stesso file gira con Celery di riferimento e con celery-rs."""
import os

from celery import Celery

BROKER = os.environ.get('BENCH_BROKER', 'redis://127.0.0.1:6390/0')
BACKEND = os.environ.get('BENCH_BACKEND', 'redis://127.0.0.1:6390/1')
COUNTER = os.environ.get('BENCH_COUNTER', 'bench:done')

app = Celery('bench', broker=BROKER, backend=BACKEND)
app.conf.update(
    worker_hijack_root_logger=False,
    task_default_queue=os.environ.get('BENCH_QUEUE', 'celery'),
    broker_connection_retry_on_startup=True,
)

_counter = None


def counter():
    global _counter
    if _counter is None:
        # Import pigro: il contatore serve ai task, non al master del worker.
        # Con `import redis` in testa il master caricava redis-py (~19 MB di
        # PSS) e la misura della memoria del worker non era più quella del
        # solo worker (spec 08a: il master non deve importare redis-py).
        import redis
        _counter = redis.Redis(host='127.0.0.1', port=6390, db=2)
    return _counter


@app.task(ignore_result=True)
def noop():
    counter().incr(COUNTER)


@app.task
def add(x, y):
    return x + y


@app.task(ignore_result=True)
def noop_pure():
    """Task del bench che non importa nulla (misura solo il costo nostro).

    Il completamento è contato dal padre sulla dimensione del file: nessun
    client redis-py nel figlio (spec 08b, variante ``--task pure``).
    """
    path = os.environ.get('BENCH_PURE_DONE')
    if path:
        fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        try:
            os.write(fd, b'.')
        finally:
            os.close(fd)
