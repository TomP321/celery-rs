import asyncio
import os
from contextlib import asynccontextmanager
from celery import chain, chord, group
from celery.result import AsyncResult
from fastapi import BackgroundTasks, FastAPI
from tasks import Order, add, app as celery_app, async_inside, blob, boom, process_order, slow, total

# Publish before uvicorn/gunicorn forks: the producer must survive the fork.
add.delay(1, 1)


@asynccontextmanager
async def lifespan(_):
    add.delay(2, 2)
    yield

api = FastAPI(lifespan=lifespan)


@api.post('/add-async')
async def add_async(x: int, y: int):
    return {'id': add.delay(x, y).id}

@api.post('/add-sync')
def add_sync(x: int, y: int):
    return {'id': add.apply_async((x, y)).id}

@api.post('/add-wait')
async def add_wait(x: int, y: int):
    res = add.delay(x, y)
    return {'result': await asyncio.to_thread(res.get, timeout=30)}

@api.post('/add-bg')
async def add_bg(x: int, y: int, background: BackgroundTasks):
    background.add_task(add.delay, x, y)
    return {'queued': True}

@api.post('/order')
async def order(o: Order):
    return {'id': process_order.delay(o.model_dump()).id}

@api.get('/result/{tid}')
async def result(tid: str):
    res = AsyncResult(tid, app=celery_app)
    return {'state': res.state, 'result': res.result if res.successful() else None,
            'error': type(res.result).__name__ if res.failed() else None}

@api.post('/boom')
async def do_boom():
    return {'id': boom.delay().id}

def _workflow():
    # Build and wait in one thread: a group waited on from another thread times out in Celery too.
    return {
        'chain': chain(add.s(1, 2), add.s(3), add.s(4)).delay().get(timeout=30),
        'group': group(add.s(i, i) for i in range(5)).delay().get(timeout=30),
        'chord': chord((add.s(i, 1) for i in range(5)), total.s()).delay().get(timeout=30),
    }

@api.post('/workflow')
async def workflow():
    return await asyncio.to_thread(_workflow)

@api.post('/async-inside')
async def do_async_inside(n: int):
    res = async_inside.delay(n)
    return {'result': await asyncio.to_thread(res.get, timeout=30)}

@api.post('/blob')
async def do_blob(size: int):
    res = blob.delay(size)
    return {'len': len(await asyncio.to_thread(res.get, timeout=60))}

@api.post('/revoke')
async def revoke():
    res = slow.apply_async((30,), countdown=20)
    res.revoke()
    return {'id': res.id}

@api.get('/ping')
async def ping():
    replies = await asyncio.to_thread(celery_app.control.inspect(timeout=2).ping)
    return {'workers': len(replies or {})}

@api.get('/stats')
async def stats():
    insp = celery_app.control.inspect(timeout=2)
    registered = await asyncio.to_thread(insp.registered)
    return {'has_add': any('tasks.add' in v for v in (registered or {}).values())}

@api.get('/pid')
async def pid():
    return {'pid': os.getpid()}
