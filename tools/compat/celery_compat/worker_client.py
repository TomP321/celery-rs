"""Client di `compat wire-worker`: gira **sempre** nel venv del riferimento.

Così ogni differenza nel traffico catturato viene dal worker sotto esame. Ogni scenario
pubblica i suoi task e attende che il worker li abbia completati; esce con 0 se gli esiti sono
quelli attesi.

    python worker_client.py --list
    python worker_client.py --scenario NAME   (COMPAT_BROKER / COMPAT_BACKEND nell'ambiente)
"""
import argparse
import os
import sys
import time

from celery import chord, group, states

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from worker_app import add, app, fail, late, retry_once, sleep, tsum  # noqa: E402

TIMEOUT = 20
SCENARIOS = {}


def scenario(fn):
    SCENARIOS[fn.__name__] = fn
    return fn


def wait_state(result, state, timeout=TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result.state == state:
            return
        time.sleep(0.05)
    raise AssertionError(f'{result.id}: stato {result.state}, atteso {state}')


@scenario
def success():
    for i in range(3):
        assert add.apply_async((i, 10), task_id=f'success-{i}').get(timeout=TIMEOUT) == i + 10


@scenario
def failure():
    r = fail.apply_async(('boom',), task_id='failure-0')
    wait_state(r, states.FAILURE)
    assert isinstance(r.result, ValueError) and str(r.result) == 'boom'


@scenario
def retry():
    assert retry_once.apply_async((7,), task_id='retry-0').get(timeout=TIMEOUT) == 7


@scenario
def countdown():
    assert add.apply_async((1, 1), countdown=1, task_id='countdown-0').get(timeout=TIMEOUT) == 2


@scenario
def acks_late():
    assert late.apply_async((5,), task_id='late-0').get(timeout=TIMEOUT) == 5


@scenario
def expired():
    r = add.apply_async((1, 2), expires=-1, task_id='expired-0')
    wait_state(r, states.REVOKED)


@scenario
def revoked():
    app.control.revoke('revoked-0')
    time.sleep(0.5)
    r = add.apply_async((1, 2), task_id='revoked-0')
    wait_state(r, states.REVOKED)


@scenario
def canvas():
    g = group(add.s(i, i).set(task_id=f'group-{i}') for i in range(3))
    assert g.apply_async(group_id='group-g').get(timeout=TIMEOUT) == [0, 2, 4]
    c = chord((add.s(i, 1).set(task_id=f'chord-{i}') for i in range(3)),
              tsum.s().set(task_id='chord-body'))
    assert c.apply_async(group_id='chord-g').get(timeout=TIMEOUT) == 6


@scenario
def inspect():
    replies = app.control.inspect(timeout=1).ping() or {}
    assert len(replies) == 1, replies
    stats = app.control.inspect(timeout=1).registered() or {}
    assert stats, stats


@scenario
def long_task():
    # il driver manda SIGTERM al worker dopo il client: il task deve essere già finito
    assert sleep.apply_async((1,), task_id='sleep-0').get(timeout=TIMEOUT) == 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--list', action='store_true')
    ap.add_argument('--scenario')
    args = ap.parse_args()
    if args.list:
        print('\n'.join(SCENARIOS))
        return
    SCENARIOS[args.scenario]()


if __name__ == '__main__':
    main()
