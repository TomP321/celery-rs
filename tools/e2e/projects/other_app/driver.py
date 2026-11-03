import json, os, sys, time
sys.path.insert(0, os.path.dirname(__file__))
from celery import chain, chord, group
import otasks as T
out = {}
def check(name, fn):
    try:
        out[name] = fn()
    except BaseException as e:
        out[name] = f'ERR {type(e).__name__}: {str(e)[:120]}'
G = dict(timeout=20)
check('add', lambda: T.add.delay(2, 3).get(**G))
def boom():
    try:
        T.boom.delay().get(**G)
    except ValueError as e:
        return str(e)
check('failure', boom)
check('retry', lambda: T.retry_once.delay().get(**G))
check('chain', lambda: chain(T.add.s(1, 2), T.add.s(3)).delay().get(**G))
check('group', lambda: group(T.add.s(i, i) for i in range(5)).delay().get(**G))
if os.environ.get('E2E_CHORD', '1') == '1':
    check('chord', lambda: chord((T.add.s(i, 1) for i in range(4)), T.total.s()).delay().get(**G))
check('countdown', lambda: T.add.apply_async((1, 1), countdown=1).get(**G))
def many():
    r = [T.add.delay(i, 1) for i in range(200)]
    return sum(x.get(timeout=40) for x in r) == sum(range(1, 201))
check('200 tasks', many)
if os.environ.get('E2E_PARALLEL'):
    n = int(os.environ['E2E_PARALLEL'])
    def par():
        t = time.time(); [x.get(timeout=30) for x in [T.nap.delay(0.5) for _ in range(n)]]
        return time.time() - t < 2.5
    check(f'{n} naps overlap', par)
print('JSON' + json.dumps(out))
