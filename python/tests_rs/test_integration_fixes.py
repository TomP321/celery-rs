"""Regressioni 09a confrontate con il pacchetto di riferimento."""
import json
import os
from pathlib import Path
import subprocess
import uuid

import pytest

ROOT = Path(__file__).resolve().parents[2]
REDIS = os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390')
AMQP = os.environ.get('CELERY_RS_AMQP_URL', 'amqp://guest:guest@127.0.0.1:5673//')


def compare(script, broker=REDIS):
    outputs = []
    for venv in ('.venv-ref', '.venv-rs'):
        env = dict(os.environ, FIX_BROKER=broker, FIX_BACKEND=REDIS + '/11',
                   FIX_ID='rs-09a-' + uuid.uuid4().hex)
        result = subprocess.run([str(ROOT / venv / 'bin/python'), '-I', '-c', script],
                                env=env, capture_output=True, text=True, timeout=45, check=False)
        assert result.returncode == 0, result.stderr
        outputs.append(json.loads(result.stdout))
    assert outputs[0] == outputs[1]
    return outputs[1]


@pytest.mark.parametrize('broker', [REDIS, AMQP], ids=['redis', 'amqp'])
def test_result_revoke_without_reply_accepts_none_timeout(broker):
    assert compare('''
import json, os
from celery import Celery
app = Celery('fix', broker=os.environ['FIX_BROKER'], backend='cache+memory://')
result = app.AsyncResult(os.environ['FIX_ID'])
result.revoke()
result.revoke_by_stamped_headers({'monitoring_id': ['example']})
print(json.dumps(True))
''', broker)


def test_redis_failure_and_chord_ttl():
    assert compare('''
import json, os
from types import SimpleNamespace
from celery import Celery, states
from celery.result import GroupResult
app = Celery('fix', broker='memory://', backend=os.environ['FIX_BACKEND'])
app.conf.update(result_expires=60, result_failure_expires=600, result_chord_expires=600)
b = app.backend
prefix = os.environ['FIX_ID']
success, failure, gid = [prefix + suffix for suffix in ('success', 'failure', 'group')]
header = GroupResult(gid, [GroupResult(prefix + 'nested', [app.AsyncResult(prefix + 'part')])])
keys = [b.get_key_for_task(i) for i in (success, failure)]
keys += [b.get_key_for_group(gid, suffix) for suffix in ('', '.j', '.s')]
try:
    assert b.client.ttl(prefix + 'missing') == -2
    b.client.set(prefix + 'persistent', 'value')
    assert b.client.ttl(prefix + 'persistent') == -1
    b.store_result(success, 42, states.SUCCESS)
    b.store_result(failure, KeyError('x'), states.FAILURE)
    assert 0 < b.client.ttl(keys[0]) <= 60
    assert 60 < b.client.ttl(keys[1]) <= 600
    b.apply_chord((gid, header.results), None)
    b.set_chord_size(gid, 2)
    b.on_chord_part_return(SimpleNamespace(id=prefix, group=gid, group_index=0, chord=None), states.SUCCESS, 1)
    assert all(60 < b.client.ttl(key) <= 600 for key in keys[2:])
    assert GroupResult.restore(gid, backend=b) == header
    print(json.dumps(True))
finally:
    b.client.delete(*keys, prefix + 'persistent')
''')


def test_pubsub_reentrant_cancel_releases_server_channels():
    assert compare('''
import json, os, threading
import redis
from celery import Celery
app = Celery('fix', broker='memory://', backend=os.environ['FIX_BACKEND'])
c = app.backend.result_consumer
initial = os.environ['FIX_ID']
c.start(initial)
pubsub = c._pubsub
pending = [initial + str(i) for i in range(6)]
for task_id in pending:
    c.consume_from(task_id)
active, nested, errors = [], [], []
def wrap(command):
    def call(*args):
        if active:
            nested.append(args)
        active.append(command)
        try:
            result = command(*args)
            if pending:
                c.cancel_for(pending.pop())
            return result
        finally:
            active.pop()
    return call
pubsub.subscribe = wrap(pubsub.subscribe)
pubsub.unsubscribe = wrap(pubsub.unsubscribe)
def run():
    try:
        c.consume_from(initial + 'outer')
        c.cancel_for(initial + 'outer')
    except Exception as exc:
        errors.append(str(exc))
t = threading.Thread(target=run, daemon=True)
t.start()
t.join(10)
assert not t.is_alive() and not errors and not nested and not pending
assert c.subscribed_to == {c._get_key_for_task(initial)}
del pubsub.subscribe, pubsub.unsubscribe
c.cancel_for(initial)
c.stop()
admin = redis.Redis.from_url(os.environ['FIX_BACKEND'])
assert admin.pubsub_channels('*' + initial + '*') == []
print(json.dumps(True))
''')


def test_amqp_publish_ignores_unknown_properties():
    assert compare('''
import json, os, time
from celery import Celery
from celery.utils.dispatch import Signal
from kombu import Connection, Queue
app = Celery('fix', broker=os.environ['FIX_BROKER'])
queue = Queue(os.environ['FIX_ID'], routing_key=os.environ['FIX_ID'])
with Connection(os.environ['FIX_BROKER']) as conn:
    bound = queue(conn)
    bound.declare()
    try:
        with app.producer_pool.acquire(block=True) as producer:
            producer.publish({'value': 42}, exchange=queue.exchange, routing_key=queue.name,
                             serializer='json', signal=Signal(), arbitrary=object())
        deadline = time.monotonic() + 5
        message = bound.get(no_ack=True)
        while message is None and time.monotonic() < deadline:
            time.sleep(0.01)
            message = bound.get(no_ack=True)
        assert message is not None
        print(json.dumps(message.payload))
    finally:
        bound.delete()
''', AMQP) == {'value': 42}
