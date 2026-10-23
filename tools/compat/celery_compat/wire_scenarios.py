"""Scenari client per `celery-compat wire`. Gira dentro il venv da analizzare (ref o rs).

Uso: <venv>/bin/python wire_scenarios.py --broker URL [--backend URL] --scenario NOME
     <venv>/bin/python wire_scenarios.py --list

Ogni scenario esegue solo operazioni lato client (nessun worker): il traffico generato sul broker
è catturato dal processo che lancia lo script. Valori deterministici dove possibile (task_id, eta)
così la normalizzazione deve intervenire il meno possibile.
"""
import argparse
import datetime as dt
import sys

from celery import Celery, chain, chord, group, signature

FIXED_ETA = dt.datetime(2030, 1, 2, 3, 4, 5, 678901, tzinfo=dt.timezone.utc)
SCENARIOS = {}


def scenario(fn):
    SCENARIOS[fn.__name__] = fn
    return fn


def make_app(broker, backend):
    app = Celery('wire', broker=broker, backend=backend, set_as_current=True)
    app.conf.broker_connection_retry_on_startup = True

    @app.task(name='wire.add')
    def add(x, y):
        return x + y

    @app.task(name='wire.tsum')
    def tsum(nums):
        return sum(nums)

    @app.task(name='wire.noresult', ignore_result=True)
    def noresult():
        return None

    return app


@scenario
def delay_basic(app):
    app.tasks['wire.add'].apply_async((2, 3), task_id='00000000-0000-0000-0000-000000000001')


@scenario
def delay_autoid(app):
    app.tasks['wire.add'].delay(1, 2)


@scenario
def apply_async_options(app):
    add = app.tasks['wire.add']
    add.apply_async((1,), {'y': 2}, task_id='00000000-0000-0000-0000-000000000002',
                    eta=FIXED_ETA, expires=FIXED_ETA + dt.timedelta(hours=1), priority=5,
                    time_limit=30, soft_time_limit=20, headers={'x-custom': [1, 'a']},
                    shadow='shadowed', retries=2)


@scenario
def apply_async_countdown_expires(app):
    app.tasks['wire.add'].apply_async((1, 1), countdown=10, expires=60)


@scenario
def apply_async_routing(app):
    add = app.tasks['wire.add']
    add.apply_async((1, 1), queue='other', task_id='00000000-0000-0000-0000-000000000003')
    add.apply_async((1, 1), exchange='ex', routing_key='rk', task_id='00000000-0000-0000-0000-000000000004')


@scenario
def apply_async_serializers(app):
    add = app.tasks['wire.add']
    for i, ser in enumerate(('json', 'pickle', 'msgpack', 'yaml')):
        add.apply_async((i, 'é☃𝄞'), serializer=ser, task_id=f'00000000-0000-0000-0001-00000000000{i}')


@scenario
def apply_async_compression(app):
    add = app.tasks['wire.add']
    for i, comp in enumerate(('zlib', 'bzip2', 'lzma')):
        add.apply_async(('x' * 200, 'y'), compression=comp,
                        task_id=f'00000000-0000-0000-0002-00000000000{i}')


@scenario
def apply_async_payload_types(app):
    app.tasks['wire.add'].apply_async(
        ([1, 2.5, -0.0, 10 ** 30, None, True, 'ü\n\t"q"'],
         {'d': FIXED_ETA, 'date': FIXED_ETA.date(), 'b': b'\x00\xffbin', 'u': 'x'}),
        task_id='00000000-0000-0000-0000-000000000005')


@scenario
def apply_async_links(app):
    add = app.tasks['wire.add']
    add.apply_async((1, 1), link=add.s(10), link_error=signature('wire.noresult', immutable=True),
                    task_id='00000000-0000-0000-0000-000000000006')


@scenario
def send_task_by_name(app):
    app.send_task('wire.remote', args=(1,), kwargs={'k': 'v'},
                  task_id='00000000-0000-0000-0000-000000000007')


@scenario
def ignore_result_task(app):
    app.tasks['wire.noresult'].apply_async(task_id='00000000-0000-0000-0000-000000000008')


@scenario
def canvas_chain(app):
    add = app.tasks['wire.add']
    chain(add.s(1, 1), add.s(2), add.s(3)).apply_async()


@scenario
def canvas_group(app):
    add = app.tasks['wire.add']
    group(add.s(i, i) for i in range(3)).apply_async()


@scenario
def canvas_chord(app):
    add, tsum = app.tasks['wire.add'], app.tasks['wire.tsum']
    chord((add.s(i, i) for i in range(3)), tsum.s()).apply_async()


@scenario
def canvas_starmap_chunks(app):
    add = app.tasks['wire.add']
    add.starmap([(1, 1), (2, 2)]).apply_async()
    add.chunks([(i, i) for i in range(4)], 2).apply_async()


@scenario
def sent_event(app):
    app.conf.task_send_sent_event = True
    app.tasks['wire.add'].apply_async((1, 1), task_id='00000000-0000-0000-0000-000000000009')


@scenario
def control_revoke(app):
    app.control.revoke('00000000-0000-0000-0000-00000000000a', terminate=True, signal='SIGKILL')


@scenario
def control_ping(app):
    app.control.ping(timeout=0.2)


@scenario
def control_inspect_active(app):
    app.control.inspect(timeout=0.2).active()


@scenario
def control_rate_limit(app):
    app.control.rate_limit('wire.add', '10/m')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--broker')
    ap.add_argument('--backend')
    ap.add_argument('--scenario')
    ap.add_argument('--list', action='store_true')
    args = ap.parse_args()
    if args.list:
        print('\n'.join(SCENARIOS))
        return
    app = make_app(args.broker, args.backend)
    try:
        SCENARIOS[args.scenario](app)
    finally:
        app.close()
    sys.stdout.flush()


if __name__ == '__main__':
    main()
