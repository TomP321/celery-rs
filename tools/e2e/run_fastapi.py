import os, sys, time, glob, json
from common import *
venv, name = sys.argv[1], sys.argv[2]
here = os.path.dirname(os.path.abspath(__file__))
vb = f'{venv}/bin'; project = f'{here}/projects/fastapi_app'
os.environ['PYTHONPATH'] = project
sys.path.insert(0, project)
r = Run(name, project, dict(os.environ))
import httpx, concurrent.futures as cf
from celery.result import AsyncResult
import tasks as T
port = os.environ['E2E_PORT']; base = f'http://127.0.0.1:{port}'
try:
    w = r.spawn('worker', f'{vb}/celery', '-A', 'tasks', 'worker', '--concurrency', '3', '--pool', 'prefork', '--max-memory-per-child', '200000', '-l', 'INFO', '-n', 'main@%h')
    r.check('worker starts', lambda: r.wait_log(w, r' ready\.'))
    r.check('worker_process_init once per child', lambda: wait_until(lambda: len(glob.glob(os.environ['E2E_RUN'] + '/child-*')) == 3, 10))
    def scenarios(label, server):
        c = httpx.Client(base_url=base, timeout=60)
        def wait_up():
            for _ in range(100):
                try:
                    return c.get('/pid').status_code == 200
                except httpx.TransportError:
                    time.sleep(0.2)
        r.check(f'{label}: starts after publish before fork', wait_up, True)
        result = lambda tid: AsyncResult(tid, app=T.app).get(timeout=30)
        r.check(f'{label}: async endpoint delay', lambda: result(c.post('/add-async', params={'x': 2, 'y': 3}).json()['id']), 5)
        r.check(f'{label}: sync endpoint apply_async', lambda: result(c.post('/add-sync', params={'x': 2, 'y': 3}).json()['id']), 5)
        r.check(f'{label}: await to_thread(get)', lambda: c.post('/add-wait', params={'x': 40, 'y': 2}).json(), {'result': 42})
        r.check(f'{label}: BackgroundTasks', lambda: c.post('/add-bg', params={'x': 1, 'y': 1}).json(), {'queued': True})
        r.check(f'{label}: pydantic payload', lambda: result(c.post('/order', json={'sku': 'a', 'qty': 2}).json()['id']), {'sku': 'a', 'qty': 2, 'ok': True})
        def poll():
            tid = c.post('/add-async', params={'x': 1, 'y': 1}).json()['id']
            return wait_until(lambda: c.get(f'/result/{tid}').json()['state'] == 'SUCCESS', 15) and c.get(f'/result/{tid}').json()
        r.check(f'{label}: poll /result', poll, {'state': 'SUCCESS', 'result': 2, 'error': None})
        def fail():
            tid = c.post('/boom').json()['id']
            wait_until(lambda: c.get(f'/result/{tid}').json()['state'] == 'FAILURE', 15)
            return c.get(f'/result/{tid}').json()
        r.check(f'{label}: task failure state', fail, {'state': 'FAILURE', 'result': None, 'error': 'ValueError'})
        r.check(f'{label}: chain/group/chord', lambda: c.post('/workflow').json(), {'chain': 10, 'group': [0, 2, 4, 6, 8], 'chord': 15})
        r.check(f'{label}: asyncio.run inside task', lambda: c.post('/async-inside', params={'n': 21}).json(), {'result': 42})
        r.check(f'{label}: 1 MiB result', lambda: c.post('/blob', params={'size': 1 << 20}).json(), {'len': 1 << 20})
        def revoked():
            tid = c.post('/revoke').json()['id']
            return AsyncResult(tid, app=T.app).id == tid
        r.check(f'{label}: revoke', revoked, True)
        r.check(f'{label}: inspect ping from endpoint', lambda: c.get('/ping').json()['workers'] >= 1)
        r.check(f'{label}: inspect registered from endpoint', lambda: c.get('/stats').json(), {'has_add': True})
        def concurrent():
            def one(i):
                path = '/add-async' if i % 2 else '/add-sync'
                return result(httpx.post(base + path, params={'x': i, 'y': 1}, timeout=60).json()['id'])
            with cf.ThreadPoolExecutor(32) as ex:
                return sorted(ex.map(one, range(500)))
        r.check(f'{label}: 500 concurrent requests', concurrent, [i + 1 for i in range(500)])
        def many_wait():
            def one(i):
                return httpx.post(base + '/add-wait', params={'x': i, 'y': 0}, timeout=60).json()['result']
            with cf.ThreadPoolExecutor(32) as ex:
                return sorted(ex.map(one, range(100)))
        r.check(f'{label}: 100 concurrent awaited results', many_wait, list(range(100)))
        r.check(f'{label}: clean stop', lambda: r.stop(server), lambda rc: rc in (0, -15))

    env = {'PYTHONPATH': project}
    scenarios('uvicorn', r.spawn('uvicorn', f'{vb}/uvicorn', 'main:api', '--port', port, '--log-level', 'warning', env=env))
    time.sleep(1)
    scenarios('uvicorn --workers 2', r.spawn('uvicorn2', f'{vb}/uvicorn', 'main:api', '--port', port, '--workers', '2', '--log-level', 'warning', env=env))
    time.sleep(1)
    scenarios('gunicorn --preload', r.spawn('gunicorn', f'{vb}/gunicorn', 'main:api', '--preload', '-w', '2', '-k', 'uvicorn.workers.UvicornWorker', '-b', f'127.0.0.1:{port}', env=env))
    r.check('worker clean stop', lambda: r.stop(w), 0)
    r.check('no fatal lines in logs', lambda: r.scan_logs('NOTHING'), [])

finally:
    r.cleanup()
    r.finish()
