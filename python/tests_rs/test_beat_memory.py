"""Beat public object compatibility and memory usage."""
import json
import os
from pathlib import Path
import subprocess
import textwrap

import pytest

ROOT = Path(__file__).resolve().parents[2]


def run(venv, code, *args):
    env = {k: v for k, v in os.environ.items() if not k.startswith('PYTHON')}
    result = subprocess.run([str(ROOT / venv / 'bin/python'), '-c', textwrap.dedent(code),
                             *map(str, args)], env=env, cwd=ROOT / 'bench',
                            capture_output=True, text=True, timeout=120, check=True)
    return json.loads(result.stdout.splitlines()[-1])


PUBLIC_OBJECTS = '''
import json, pickle
from datetime import datetime, timezone
from celery import Celery
from celery.beat import ScheduleEntry
from celery.schedules import crontab
app = Celery('08f-public', broker='memory://')
now = datetime(2026, 10, 26, tzinfo=timezone.utc)
app.now = lambda: now
cron = crontab('*/5', '2-6', 'mon-fri', app=app)
entry = ScheduleEntry('entry', 'test.task', now, 42, cron,
                      (1,), {'x': 2}, {'queue': 'q'}, app=app)
copy = pickle.loads(pickle.dumps(entry))
next_entry = next(entry)
fields = ('minute', 'hour', 'day_of_week', 'day_of_month', 'month_of_year')
result = {'repr': repr(entry), 'equal': entry == copy,
          'next_count': next_entry.total_run_count,
          'fields': {k: sorted(vars(cron)[k]) for k in fields},
          'set_types': [type(getattr(cron, k)).__name__ for k in fields],
          'entry_keys': sorted(vars(entry)),
          'pickle_repr': repr(copy)}
cron.minute.add(3)
cron.minute = {10}
result['changed'] = sorted(cron.minute)
result['next'] = str(cron.remaining_estimate(now))
result['identity'] = cron.minute is vars(cron)['minute']
result['fraction'] = sorted(crontab(minute=[1.5]).minute)
print(json.dumps(result))
'''


def test_public_objects_match_reference():
    assert run('.venv-rs', PUBLIC_OBJECTS) == run('.venv-ref', PUBLIC_OBJECTS)


def test_compact_cron_exposure_and_mutation():
    from celery.schedules import crontab
    from celery import Celery
    from datetime import datetime, timezone
    app = Celery('08f-cron')
    app.now = lambda: datetime(2026, 10, 26, tzinfo=timezone.utc)
    cron = crontab('*', app=app)
    cron.remaining_estimate(app.now())
    import _celery_rs
    assert isinstance(cron._rs_cached, _celery_rs.CronSpec)
    # The public view must be the real, writable dictionary.
    data = vars(cron)
    data['minute'] = {7}
    cron._rs_spec()
    cron.minute.add(8)
    assert cron._rs_spec().fields()[0] == [7, 8]
    del cron.minute
    with pytest.raises(AttributeError):
        getattr(cron, 'minute')
    supplied = {3}
    other = crontab(supplied)
    assert other.minute is supplied
    supplied.add(4)
    assert other._rs_spec().fields()[0] == [3, 4]


def test_persistent_sync_reuses_entries(tmp_path):
    from celery import Celery
    from celery.beat import PersistentScheduler
    from celery.schedules import crontab
    app = Celery('08f-store', broker='memory://')
    app.conf.update(result_expires=None, beat_schedule={
        'test': {'task': 'test', 'schedule': crontab('*/5')}})
    scheduler = PersistentScheduler(app, schedule_filename=str(tmp_path / 'schedule'))
    try:
        entries = scheduler.schedule
        cron = entries['test'].schedule
        for _ in range(10):
            scheduler.sync()
            assert scheduler.schedule is entries
            assert scheduler.schedule['test'].schedule is cron
        entries['test'].total_run_count = 123
        scheduler.sync()
    finally:
        scheduler.close()
    # The reference must retain values persisted in the same file.
    code = '''
    import shelve, json, sys
    with shelve.open(sys.argv[1]) as store:
        print(json.dumps(store['entries']['test'].total_run_count))
    '''
    assert run('.venv-ref', code, tmp_path / 'schedule') == 123


def test_pss_stable_over_accelerated_hour(tmp_path):
    code = '''
    import json, os, sys, uuid
    from datetime import datetime, timedelta, timezone
    import redis
    from celery import Celery
    from celery.beat import PersistentScheduler
    queue = '08f-memory-' + uuid.uuid4().hex
    url = os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/5'
    client = redis.Redis.from_url(url)
    app = Celery('08f-stability', broker=url)
    clock = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
    app.now = lambda: clock[0]
    app.conf.update(result_expires=None, task_ignore_result=True,
                    task_default_queue=queue, task_default_exchange=queue,
                    task_default_routing_key=queue, beat_sync_every=100,
                    beat_schedule={str(i): {'task': 'noop', 'schedule': 1.0}
                                   for i in range(3)})
    scheduler = PersistentScheduler(app, schedule_filename=sys.argv[1])
    def pss():
        with open('/proc/self/smaps_rollup') as handle:
            return next(int(line.split()[1]) for line in handle if line.startswith('Pss:'))
    samples = []
    try:
        for second in range(3600):
            clock[0] += timedelta(seconds=1)
            for _ in range(3):
                scheduler.tick()
            client.delete(queue)
            if second in (1799, 2699, 3599):
                scheduler.sync()
                samples.append(pss())
        counts = [entry.total_run_count for entry in scheduler.schedule.values()]
    finally:
        scheduler.close()
        client.delete(queue, '_kombu.binding.' + queue)
    print(json.dumps({'pss_kb': samples, 'counts': counts}))
    '''
    data = run('.venv-rs', code, tmp_path / 'schedule')
    assert min(data['counts']) >= 3500, data
    assert max(data['pss_kb']) - min(data['pss_kb']) < 2048, data


def test_pidbox_names_resolve_to_real_objects():
    code = """
    import json, sys
    import celery.beat as beat
    lazy = 'kombu.pidbox' not in sys.modules
    from kombu import pidbox
    from celery._beat_node import _BeatNode
    print(json.dumps([lazy, beat.pidbox is pidbox,
                      beat._BeatNode is _BeatNode,
                      issubclass(_BeatNode, pidbox.Node),
                      {'_BeatNode', 'pidbox'} <= set(dir(beat))]))
    """
    assert run('.venv-rs', code) == [True] * 5
