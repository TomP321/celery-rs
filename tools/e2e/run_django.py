import os, sys, time, glob, json
from common import *
venv, name = sys.argv[1], sys.argv[2]
here = os.path.dirname(os.path.abspath(__file__))
celery_bin = f'{venv}/bin/celery'; py = f'{venv}/bin/python'; project = f'{here}/projects/django_app'
os.environ.update(DJANGO_SETTINGS_MODULE='proj.settings', PYTHONPATH=project)
sys.path.insert(0, project)
import psycopg
with psycopg.connect('host=127.0.0.1 port=55432 user=postgres password=e2e dbname=postgres', autocommit=True) as c:
    c.execute(f"DROP DATABASE IF EXISTS {os.environ['E2E_DB']} WITH (FORCE)")
    c.execute(f"CREATE DATABASE {os.environ['E2E_DB']}")
r = Run(name, project, dict(os.environ))
try:
    r.check('manage.py check', lambda: r.cmd(py, 'manage.py', 'check')[0], 0)
    r.check('manage.py migrate', lambda: r.cmd(py, 'manage.py', 'migrate', '--noinput')[0], 0)
    r.check('manage.py test (eager, on_commit)', lambda: (lambda o: o[0] == 0 or o[1][-800:])(r.cmd(py, 'manage.py', 'test', 'shop', '--noinput', timeout=120)), True)

    import django; django.setup()
    from django.db import transaction, connection
    from celery import chain, chord, group, current_app
    from celery.result import AsyncResult
    from shop import tasks as T
    from shop.models import Item
    from django.core.cache import cache
    from django_celery_results.models import TaskResult, GroupResult
    from django_celery_beat.models import PeriodicTask, IntervalSchedule, CrontabSchedule, PeriodicTasks

    q = os.environ['E2E_QUEUE']; recycle_q = q + '.recycle'
    w = r.spawn('worker', celery_bin, '-A', 'proj', 'worker', '--concurrency', '3', '--pool', 'prefork', '--max-memory-per-child', '200000', '-l', 'INFO', '-Q', q, '-n', 'main@%h')
    w2 = r.spawn('recycle', celery_bin, '-A', 'proj', 'worker', '--concurrency', '1', '--pool', 'prefork', '--max-memory-per-child', '1', '-l', 'INFO', '-Q', recycle_q, '-n', 'recycle@%h')
    r.check('worker starts', lambda: r.wait_log(w, r' ready\.'))
    r.check('recycle worker starts', lambda: r.wait_log(w2, r' ready\.'))

    G = dict(timeout=30)
    r.check('add', lambda: T.add.delay(2, 3).get(**G), 5)
    r.check('ORM write', lambda: Item.objects.filter(pk=T.create_item.delay('orm').get(**G)).exists())
    r.check('ORM read', lambda: T.count_items.delay('orm').get(**G), 1)
    def ids():
        pairs = group(T.db_identity.s() for _ in range(30)).delay().get(**G)
        return {tuple(p) for p in pairs}
    pairs = r.check('per-child DB connections reach all 3 children', ids, lambda s: len({p for p, _ in s}) == 3)
    def distinct():
        by_backend = {}
        for pid, backend in pairs:
            by_backend.setdefault(backend, set()).add(pid)
        return max(len(v) for v in by_backend.values())
    r.check('no DB connection shared between children', distinct, 1)
    r.check('worker_process_init once per child', lambda: len(glob.glob(os.environ['E2E_RUN'] + '/child-*')) >= 3)
    def retry():
        key = f'flaky-{time.time()}'; cache.set(key, 0, 60)
        return T.flaky.delay(key).get(**G)
    r.check('retry with cache', retry, 3)
    def fails():
        res = T.boom.delay()
        try:
            res.get(**G)
        except ValueError as e:
            return str(e), res.state, 'kaboom' in (res.traceback or '')
    r.check('failure propagates', fails, ('kaboom', 'FAILURE', True))
    r.check('IntegrityError inside atomic', lambda: T.broken_tx.delay(f'k{time.time()}').get(**G), 1)
    def query_err():
        try:
            T.broken_query.delay().get(**G)
        except Exception as e:
            return type(e).__name__
    r.check('SQL error type', query_err, 'ProgrammingError')
    r.check('DB usable after SQL errors', lambda: group(T.count_items.s('orm') for _ in range(9)).delay().get(**G), [1] * 9)
    r.check('soft time limit', lambda: T.soft.delay().get(**G), 'soft-limit-handled')
    def hard():
        try:
            T.hard.delay().get(**G)
        except Exception as e:
            return type(e).__name__
    r.check('hard time limit', hard, 'TimeLimitExceeded')
    r.check('after hard limit: ORM on replacement child', lambda: group(T.count_items.s('orm') for _ in range(9)).delay().get(**G), [1] * 9)
    r.check('django-redis cache', lambda: T.cached.delay('ck', {'a': 1}).get(**G), {'a': 1})
    r.check('settings, timezone, gettext', lambda: T.django_env.delay().get(**G), {'marker': 'from-django-settings', 'tz': True, 'word': 'Sì'})
    r.check('send_mail', lambda: T.send_one.delay().get(**G), True)
    r.check('chain', lambda: chain(T.add.s(1, 2), T.add.s(3)).delay().get(**G), 6)
    r.check('group', lambda: group(T.add.s(i, i) for i in range(10)).delay().get(**G), [i * 2 for i in range(10)])
    r.check('chord', lambda: chord((T.add.s(i, 1) for i in range(5)), T.total.s()).delay().get(**G), 15)
    def on_commit():
        with transaction.atomic():
            obj = Item.objects.create(name='sig:one')
            time.sleep(0.5)
        return wait_until(lambda: Item.objects.filter(name='done:sig:one').count(), 15)
    r.check('delay_on_commit from post_save', on_commit, 1)
    def rollback():
        try:
            with transaction.atomic():
                Item.objects.create(name='sig:rb')
                raise KeyError
        except KeyError:
            pass
        time.sleep(1.5)
        return Item.objects.filter(name='done:sig:rb').count()
    r.check('delay_on_commit not sent after rollback', rollback, 0)
    if os.environ['E2E_RESULT'] == 'django-db':
        def tr():
            res = T.add.delay(20, 22); res.get(**G)
            return list(TaskResult.objects.filter(task_id=res.id).values_list('status', 'task_name', 'result'))
        r.check('TaskResult row', tr, [('SUCCESS', 'shop.tasks.add', '42')])
        def gr():
            g = group(T.add.s(1, i) for i in range(3)).apply_async(); g.get(**G); g.save()
            return GroupResult.objects.filter(group_id=g.id).count()
        r.check('GroupResult row', gr, 1)
    def recycle():
        out = [T.db_identity.apply_async((0,), queue=recycle_q).get(**G) for _ in range(6)]
        return len({p for p, _ in out}), len({b for _, b in out})
    r.check('--max-memory-per-child recycles, DB reconnects', recycle, lambda v: v[0] >= 3 and v[1] >= 3)

    # gunicorn --preload with threads; wsgi.py publishes in the master before fork
    g = r.spawn('gunicorn', f'{venv}/bin/gunicorn', 'proj.wsgi', '--preload', '-w', '2', '-k', 'gthread', '--threads', '4', '-b', f"127.0.0.1:{os.environ['E2E_PORT']}")
    r.check('gunicorn --preload starts after publish in master', lambda: r.wait_log(g, r'Booting worker.*\n(.*\n)*.*Booting worker'))
    import httpx, concurrent.futures as cf
    base = f"http://127.0.0.1:{os.environ['E2E_PORT']}"
    time.sleep(1)
    def web_one(i):
        tid = httpx.get(f'{base}/enqueue', params={'x': i, 'y': 1}, timeout=30).json()['id']
        return AsyncResult(tid).get(timeout=30)
    with cf.ThreadPoolExecutor(16) as ex:
        web = r.check('view -> delay, 200 concurrent requests', lambda: sorted(ex.map(web_one, range(200))), [i + 1 for i in range(200)])
    r.check('view: transaction + delay_on_commit', lambda: (httpx.get(f'{base}/item', params={'name': 'sig:web'}, timeout=30).status_code, wait_until(lambda: Item.objects.filter(name='done:sig:web').count(), 15)), (200, 1))
    r.check('gunicorn clean stop', lambda: r.stop(g), 0)

    # shell, CLI
    r.check('manage.py shell -> delay', lambda: r.cmd(py, 'manage.py', 'shell', '-c', 'from shop.tasks import add; print("R", add.delay(4, 4).get(timeout=30))')[1].split('R ')[-1].strip(), '8')
    C = (celery_bin, '-A', 'proj')
    r.check('celery inspect ping', lambda: (lambda o: (o[0], 'pong' in o[1]))(r.cmd(*C, 'inspect', 'ping', '-d', f'main@{os.uname().nodename}')), (0, True))
    r.check('celery inspect registered', lambda: 'shop.tasks.create_item' in r.cmd(*C, 'inspect', 'registered')[1])
    r.check('celery call', lambda: r.cmd(*C, 'call', 'shop.tasks.add', '--args=[2,3]', f'--queue={q}')[0], 0)
    r.check('celery report', lambda: 'software' in r.cmd(*C, 'report')[1])

    # django-celery-beat DatabaseScheduler
    every, _ = IntervalSchedule.objects.get_or_create(every=1, period=IntervalSchedule.SECONDS)
    CrontabSchedule.objects.get_or_create(minute='*', hour='*', day_of_week='*', day_of_month='*', month_of_year='*', timezone='Europe/Rome')
    pt = PeriodicTask.objects.create(name='tick', task='shop.tasks.tick', interval=every, queue=q)
    b = r.spawn('beat', celery_bin, '-A', 'proj', 'beat', '-l', 'INFO', '--pidfile=', '-s', os.path.join(os.environ['E2E_RUN'], 'beat-schedule'))
    r.check('DatabaseScheduler ticks', lambda: wait_until(lambda: Item.objects.filter(name='tick').count() >= 3, 25), True)
    def disable():
        p = PeriodicTask.objects.get(pk=pt.pk); p.enabled = False; p.save()
        time.sleep(9)
        before = Item.objects.filter(name='tick').count(); time.sleep(4)
        return Item.objects.filter(name='tick').count() - before
    r.check('disabled in DB: beat stops sending', disable, 0)
    r.check('beat clean stop', lambda: r.stop(b), 0)
    r.check('total_run_count saved on sync', lambda: PeriodicTask.objects.get(pk=pt.pk).total_run_count > 0)
    r.check('worker clean stop', lambda: r.stop(w), 0)
    r.check('recycle worker clean stop', lambda: r.stop(w2), 0)
    r.check('no fatal lines in logs', lambda: r.scan_logs('Task handler raised error: .*(TimeLimitExceeded|WorkerLostError)|Hard time limit|CRITICAL/MainProcess.*Unrecoverable NOTHING'), [])
finally:
    r.cleanup()
    r.finish()
