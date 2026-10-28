"""Inventario diagnostico: usare solo per la sonda, mai per misure di RAM."""
import gc
import json
import os
import sys
import types
from pathlib import Path

from memapp import app
from celery import signals
from celery.concurrency import _rs_zygote


def dump(role):
    directory = Path(os.environ['CELERY_RS_INVENTORY'])
    directory.mkdir(parents=True, exist_ok=True)
    # Costo superficiale più codice/costanti: non attribuisce l'intero grafo
    # condiviso a ciascun modulo e non pretende di misurare le arene per modulo.
    modules = []
    for name, module in sorted(sys.modules.copy().items()):
        namespace = getattr(module, '__dict__', None)
        if not namespace:
            continue
        seen = set()
        def size(obj):
            if id(obj) in seen:
                return 0
            seen.add(id(obj))
            total = sys.getsizeof(obj)
            if isinstance(obj, types.FunctionType):
                total += size(obj.__code__)
            elif isinstance(obj, types.CodeType):
                total += size(obj.co_consts) + size(obj.co_code)
            elif isinstance(obj, tuple):
                total += sum(size(item) for item in obj)
            return total
        estimated = sys.getsizeof(namespace) + sum(size(v) for v in namespace.values())
        modules.append(dict(name=name, file=getattr(module, '__file__', None),
                            estimated_bytes=estimated))
    prefix = directory / f'{role}-{os.getpid()}'
    prefix.with_suffix('.json').write_text(json.dumps(dict(
        role=role, pid=os.getpid(), modules=modules, freeze=gc.get_freeze_count()), indent=2))
    prefix.with_suffix('.smaps').write_text(Path('/proc/self/smaps').read_text())
    # Le statistiche pymalloc includono byte liberi e pool parziali ereditati.
    fd = os.open(str(prefix) + '.pymalloc', os.O_CREAT | os.O_WRONLY, 0o600)
    saved = os.dup(2)
    try:
        os.dup2(fd, 2)
        sys._debugmallocstats()
    finally:
        os.dup2(saved, 2)
        os.close(saved)
        os.close(fd)


_original = _rs_zygote._immortalize_heap
_master = os.getpid()

def inventory_heap():
    _original()
    dump('master-before-fork' if os.getpid() == _master else 'zygote')


_rs_zygote._immortalize_heap = inventory_heap

@signals.worker_process_init.connect(weak=False)
def child_inventory(**kwargs):
    dump('child')

@signals.worker_ready.connect(weak=False)
def master_inventory(**kwargs):
    dump('master-ready')
