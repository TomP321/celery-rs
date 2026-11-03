import asyncio
import os
import time
from celery import Celery
from celery.signals import worker_process_init
from pydantic import BaseModel

app = Celery('fa', broker=os.environ['E2E_BROKER'], backend=os.environ['E2E_RESULT'])
app.conf.update(task_default_queue=os.environ['E2E_QUEUE'], task_track_started=True,
                broker_connection_retry_on_startup=True, result_extended=True)


class Order(BaseModel):
    sku: str
    qty: int


@worker_process_init.connect
def mark_child(**kwargs):
    with open(os.path.join(os.environ['E2E_RUN'], f'child-{os.getpid()}'), 'w'):
        pass


@app.task
def add(x, y):
    return x + y

@app.task
def total(values):
    return sum(values)

@app.task
def process_order(order):
    return Order(**order).model_dump() | {'ok': True}

@app.task
def boom():
    raise ValueError('kaboom')

@app.task
def slow(seconds):
    time.sleep(seconds)
    return seconds

async def _fetch(n):
    await asyncio.sleep(0.01)
    return n * 2

@app.task
def async_inside(n):
    return asyncio.run(_fetch(n))

@app.task
def blob(size):
    return 'x' * size

@app.task
def pid():
    return os.getpid()
