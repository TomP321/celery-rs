"""Collect explicit 10c diagnostics with --include, outside normal execution.

RAM_BUDGET_REPORT selects the output directory. PYTHONTRACEMALLOC=1 adds
per-file heap attribution, which cannot be compared with unprofiled PSS.
Also report mallinfo2 and free pymalloc arenas."""
import ctypes
import json
import os
from pathlib import Path
import sys
import tempfile
import tracemalloc
import types

from celery import signals


def report(role, stage):
    directory = Path(os.environ['RAM_BUDGET_REPORT'])
    directory.mkdir(parents=True, exist_ok=True)
    data = {'role': role, 'stage': stage, 'pid': os.getpid(),
            'modules': sorted(sys.modules), 'threads': len(os.listdir('/proc/self/task'))}
    mappings = []
    current = None
    for line in Path('/proc/self/smaps').read_text().splitlines():
        fields = line.split()
        if '-' in fields[0]:
            current = {'mapping': line}
            mappings.append(current)
        elif fields[0].rstrip(':') in ('Size', 'Pss', 'Rss', 'Private_Clean',
                                      'Private_Dirty', 'Shared_Clean', 'Shared_Dirty'):
            current[fields[0].rstrip(':')] = int(fields[1])
    data['mappings_kib'] = mappings
    class Mallinfo(ctypes.Structure):
        _fields_ = [(name, ctypes.c_size_t) for name in (
            'arena', 'ordblks', 'smblks', 'hblks', 'hblkhd', 'usmblks',
            'fsmblks', 'uordblks', 'fordblks', 'keepcost')]
    libc = ctypes.CDLL(None)
    libc.mallinfo2.restype = Mallinfo
    info = libc.mallinfo2()
    data['glibc_bytes'] = {name: getattr(info, name) for name, _ in info._fields_}
    try:
        from _celery_rs import _allocator_stats
    except ImportError:
        data['rust_bytes'] = None
    else:
        data['rust_bytes'] = _allocator_stats()
    if tracemalloc.is_tracing():
        data['python_bytes_by_file'] = {
            str(row.traceback[0].filename): row.size
            for row in tracemalloc.take_snapshot().statistics('filename')}
        data['tracer_bytes'] = tracemalloc.get_tracemalloc_memory()
    sizes = {}
    seen = set()
    def visit(value, totals):
        if id(value) in seen:
            return
        seen.add(id(value))
        if isinstance(value, types.FunctionType):
            visit(value.__code__, totals)
        elif isinstance(value, types.CodeType):
            totals['code'] += sys.getsizeof(value)
            for constant in value.co_consts:
                if isinstance(constant, types.CodeType):
                    visit(constant, totals)
            if value.co_consts and isinstance(value.co_consts[0], str):
                totals['docstrings'] += sys.getsizeof(value.co_consts[0])
        elif isinstance(value, type):
            for member in vars(value).values():
                if isinstance(member, (staticmethod, classmethod)):
                    member = member.__func__
                visit(member, totals)
    for name, module in tuple(sys.modules.items()):
        if module is None:
            continue
        totals = {'code': 0, 'docstrings': 0}
        for value in tuple(vars(module).values()):
            visit(value, totals)
        sizes[name] = totals
    data['code_and_docstrings_bytes'] = sizes
    # CPython writes to the stderr fd rather than sys.stderr.
    with tempfile.TemporaryFile() as output:
        saved = os.dup(2)
        try:
            os.dup2(output.fileno(), 2)
            sys._debugmallocstats()
        finally:
            os.dup2(saved, 2)
            os.close(saved)
        output.seek(0)
        data['pymalloc'] = output.read().decode()
    (directory / f'{role}-{os.getpid()}-{stage}.json').write_text(json.dumps(data, indent=1))


@signals.worker_ready.connect(weak=False)
def master_ready(**kwargs):
    report('master', 'idle')


@signals.worker_process_init.connect(weak=False)
def child_ready(**kwargs):
    report('child', 'idle')


@signals.beat_init.connect(weak=False)
def beat_ready(**kwargs):
    report('beat', 'idle')


_completed = 0


@signals.task_postrun.connect(weak=False)
def child_loaded(**kwargs):
    global _completed
    _completed += 1
    if _completed == 100:
        report('child', 'loaded')


_received = 0


@signals.task_received.connect(weak=False)
def master_loaded(**kwargs):
    global _received
    _received += 1
    if _received == 100:
        report('master', 'loaded')


# The first command follows zygote import, freeze and trim.
from celery.concurrency import _rs_zygote
_recv = _rs_zygote._recv_control
_reported = False


def _recv_control(control):
    global _reported
    if not _reported:
        _reported = True
        report('zygote', 'idle')
    return _recv(control)


_rs_zygote._recv_control = _recv_control
