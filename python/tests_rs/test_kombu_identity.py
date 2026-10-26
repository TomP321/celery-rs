"""Identità degli oggetti kombu: celery-rs espone gli stessi oggetti del riferimento.

Scritto dall'orchestratore (decisione 08b): dove il riferimento usa o espone un oggetto di
kombu, celery-rs deve usare l'oggetto vero (o una sua sottoclasse), anche se costa memoria.
Non modificare questo file per farlo passare.

1. Nessun nome camuffato: ogni funzione/classe raggiungibile dai moduli `celery.*` che dichiara
   `__module__` di kombu deve essere davvero l'oggetto definito in quel modulo di kombu, e ogni
   oggetto che dichiara `__module__` di celery deve essere definito in quel file.
2. Oggetti a runtime: per una serie di espressioni tipiche (code, exchange, connessioni,
   producer, mailbox…) le classi di kombu nella MRO del riferimento devono comparire anche nella
   MRO dell'oggetto di celery-rs (stessa classe, verificata con `isinstance` sulla classe vera).
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
REF_PY = ROOT / '.venv-ref' / 'bin' / 'python'
RS_PY = ROOT / '.venv-rs' / 'bin' / 'python'

MASQUERADE_PROBE = r'''
import importlib, inspect, json, pkgutil, sys, types
import celery
problems = []
seen = set()
names = sorted(m.name for m in pkgutil.walk_packages(celery.__path__, 'celery.'))
SKIP = ('celery.contrib.testing', 'celery.contrib.pytest', 'celery.contrib.sphinx',
        'celery.backends.', 'celery.concurrency.eventlet', 'celery.concurrency.gevent',
        'celery.bin.amqp', 'celery.contrib.django', 'celery.fixups.django')
for name in names:
    if name.startswith(SKIP):
        continue
    try:
        mod = importlib.import_module(name)
    except Exception:
        continue
    for attr, obj in list(vars(mod).items()):
        if not isinstance(obj, (type, types.FunctionType)) or id(obj) in seen:
            continue
        seen.add(id(obj))
        declared = getattr(obj, '__module__', None) or ''
        qual = getattr(obj, '__qualname__', '') or ''
        if '<locals>' in qual or not declared:
            continue
        if declared.split('.')[0] not in ('celery', 'kombu', 'amqp', 'billiard', 'click', 'vine'):
            continue
        # l'oggetto deve essere quello che si trova al nome dichiarato (`__module__` +
        # `__qualname__`): un nome riscritto a mano punta a un altro oggetto o a niente
        try:
            real = importlib.import_module(declared)
            for part in qual.split('.'):
                real = getattr(real, part)
        except Exception as exc:
            if declared.startswith('celery'):
                # nomi rimossi dal namespace dopo la definizione (`del`) sono legittimi
                continue
            problems.append(f'{name}.{attr}: dichiara {declared}.{qual}, che non esiste ({exc!r})')
            continue
        if real is not obj and inspect.unwrap(real) is not inspect.unwrap(obj):
            problems.append(f'{name}.{attr}: dichiara {declared}.{qual} ma non è quell\'oggetto')
print(json.dumps(problems))
'''


def test_no_masqueraded_names():
    out = subprocess.run([str(RS_PY), '-c', MASQUERADE_PROBE], capture_output=True, text=True,
                         timeout=300, cwd='/')
    assert out.returncode == 0, out.stderr[-3000:]
    problems = json.loads(out.stdout.strip().splitlines()[-1])
    assert problems == [], '\n'.join(problems)


EXPRESSIONS = {
    'queue': "app.amqp.queues['celery']",
    'queue.exchange': "app.amqp.queues['celery'].exchange",
    'queue_from_conf': "app2.amqp.queues['extra']",
    'route_queue': "app.amqp.router.route({}, 'proj.add')['queue']",
    'default_exchange': "app.amqp.default_exchange",
    'Queues.new_missing': "app.amqp.queues.new_missing('fresh')",
    'connection': "app.connection()",
    'connection_for_write': "app.connection_for_write()",
    'connection_for_read': "app.connection_for_read()",
    'producer.connection': "producer.connection",
    'producer': "producer",
    'mailbox': "app.control.mailbox",
    'mailbox.connection': "app.control.mailbox(app.connection())",
    'events.dispatcher': "app.events.Dispatcher(app.connection(), enabled=False)",
    'backend.result_consumer': "getattr(app.backend, 'result_consumer', None)",
}

RUNTIME_PROBE = r'''
import json, sys
from celery import Celery
broker = sys.argv[1]
app = Celery('proj', broker=broker, backend='redis://127.0.0.1:6390/9')
from kombu import Exchange, Queue
app2 = Celery('proj2', broker=broker)
app2.conf.task_queues = [Queue('extra', Exchange('extra', type='direct'), routing_key='extra')]
out = {}
with app.producer_or_acquire() as producer:
    for key, expr in json.loads(sys.argv[2]).items():
        try:
            obj = eval(expr)
        except Exception as exc:
            out[key] = {'error': type(exc).__name__}
            continue
        mro = [f'{c.__module__}.{c.__qualname__}' for c in type(obj).__mro__]
        out[key] = {'mro': [m for m in mro if m.split('.')[0] in ('kombu', 'amqp', 'redis')]}
print(json.dumps(out))
'''

CHECK_PROBE = r'''
import importlib, json, sys
from celery import Celery
broker = sys.argv[1]
app = Celery('proj', broker=broker, backend='redis://127.0.0.1:6390/9')
from kombu import Exchange, Queue
app2 = Celery('proj2', broker=broker)
app2.conf.task_queues = [Queue('extra', Exchange('extra', type='direct'), routing_key='extra')]
expected = json.loads(sys.argv[3])
problems = []
with app.producer_or_acquire() as producer:
    for key, expr in json.loads(sys.argv[2]).items():
        want = expected[key]
        try:
            obj = eval(expr)
        except Exception as exc:
            if 'error' not in want:
                problems.append(f'{key}: {type(exc).__name__} (il riferimento non solleva)')
            continue
        for path in want.get('mro', []):
            mod, _, qual = path.rpartition('.')
            # classi annidate: risolvi per parti dal modulo più lungo importabile
            parts = path.split('.')
            for i in range(len(parts) - 1, 0, -1):
                try:
                    cls = importlib.import_module('.'.join(parts[:i]))
                    for p in parts[i:]:
                        cls = getattr(cls, p)
                    break
                except (ImportError, AttributeError):
                    cls = None
            if cls is None or not isinstance(obj, cls):
                problems.append(f'{key}: {type(obj).__module__}.{type(obj).__qualname__} '
                                f'non è istanza di {path} (come nel riferimento)')
print(json.dumps(problems))
'''


@pytest.mark.parametrize('broker', ['redis://127.0.0.1:6390/7', 'amqp://guest:guest@127.0.0.1:5673//'])
def test_runtime_objects_are_kombu_objects(broker):
    exprs = json.dumps(EXPRESSIONS)
    ref = subprocess.run([str(REF_PY), '-c', RUNTIME_PROBE, broker, exprs], capture_output=True,
                         text=True, timeout=120, cwd='/')
    assert ref.returncode == 0, ref.stderr[-3000:]
    expected = ref.stdout.strip().splitlines()[-1]
    rs = subprocess.run([str(RS_PY), '-c', CHECK_PROBE, broker, exprs, expected],
                        capture_output=True, text=True, timeout=120, cwd='/')
    assert rs.returncode == 0, rs.stderr[-3000:]
    problems = json.loads(rs.stdout.strip().splitlines()[-1])
    assert problems == [], '\n'.join(problems)


if __name__ == '__main__':
    sys.exit(pytest.main([__file__, '-q']))
