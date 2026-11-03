"""Attribute Django child memory without tracing allocations.

Run with the Python 3.11 e2e environment under tools/dev/with-brokers,
after tools/e2e/bench.sh has migrated e2e_bench.
Modes isolate connection setup, query compilation, and full traced tasks.
Soft-dirty pages are an upper bound on refcount CoW, not exclusive attribution.
"""
import argparse
import collections
import ctypes
import gc
import json
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(__file__))
from sd import anon_ranges, classify, softdirty


class Mallinfo(ctypes.Structure):
    _fields_ = [(name, ctypes.c_size_t) for name in (
        'arena', 'ordblks', 'smblks', 'hblks', 'hblkhd',
        'usmblks', 'fsmblks', 'uordblks', 'fordblks', 'keepcost')]


_libc = ctypes.CDLL(None)
_libc.mallinfo2.restype = Mallinfo


def shared_pages(pid, ranges):
    pages = set()
    with open(f'/proc/{pid}/pagemap', 'rb') as source:
        for start, end, _ in ranges:
            source.seek((start >> 12) * 8)
            data = source.read(((end - start) >> 12) * 8)
            for index, (entry,) in enumerate(struct.iter_unpack('<Q', data)):
                if entry >> 63 & 1 and not entry >> 56 & 1:
                    pages.add((start >> 12) + index)
    return pages


def memory():
    with open('/proc/self/smaps_rollup') as source:
        return {line.split(':')[0]: int(line.split()[1]) for line in source
                if line.startswith(('Pss:', 'Private_Dirty:', 'Private_Clean:'))}


def mappings():
    totals = collections.Counter()
    name = 'anonymous'
    with open('/proc/self/smaps') as source:
        for line in source:
            fields = line.split()
            if fields and '-' in fields[0]:
                name = os.path.basename(fields[5]) if len(fields) > 5 else 'anonymous'
            elif fields and fields[0] == 'Pss:':
                totals[name] += int(fields[1])
    return dict(totals.most_common(12))


def state():
    from django.db import connection
    from shop.models import Item
    allocator = _libc.mallinfo2()
    return dict(memory=memory(), mappings=mappings(), gc=gc.get_count(),
                allocated_blocks=sys.getallocatedblocks(),
                glibc_bytes={name: getattr(allocator, name) for name, _ in Mallinfo._fields_},
                meta_keys=sorted(Item._meta.__dict__),
                wrapper_keys=sorted(connection._connections['default'].__dict__),
                ops_keys=sorted(connection.ops.__dict__),
                features_keys=sorted(connection.features.__dict__))


def task(mode, index):
    from django.db import connection
    from shop.models import Item
    if mode == 'raw':
        global _raw_connection
        if '_raw_connection' not in globals():
            import psycopg
            _raw_connection = psycopg.connect(
                host='127.0.0.1', port=55432, user='postgres',
                password='e2e', dbname=os.environ['E2E_DB'])
    elif mode == 'connect':
        connection.ensure_connection()
    elif mode == 'compile':
        Item.objects.filter(name__startswith='x').query.sql_with_params()
    else:
        from celery.app.trace import trace_task
        from proj.celery import app
        name, args = ('shop.tasks.add', (1, 1)) if mode == 'add' else ('shop.tasks.count_items', ('x',))
        result = trace_task(app.tasks[name], str(index), args, {},
                            request={'id': str(index), 'task': name, 'ignore_result': True})
        if result.info is not None:
            raise RuntimeError(str(result))


def reclaim():
    from _celery_rs import _release_task_memory
    before = memory()
    collected = gc.collect()
    after_gc = memory()
    _release_task_memory(force=True)
    return dict(before=before, after_gc=after_gc, after_trim=memory(), collected=collected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('add', 'raw', 'connect', 'compile', 'orm'))
    options = parser.parse_args()
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    sys.path.insert(0, root + '/tools/e2e/projects/django_app')
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proj.settings')
    os.environ.setdefault('E2E_DB', 'e2e_bench')
    redis = os.environ['CELERY_RS_REDIS_URL']
    for key, value in dict(E2E_CACHE=redis + '/5', E2E_BROKER='memory://',
                           E2E_RESULT='cache+memory://', E2E_QUEUE='orm-probe', E2E_RUN='/tmp').items():
        os.environ[key] = value
    from proj.celery import app
    from celery.concurrency.rs_prefork import RsTaskPool
    app.loader.import_default_modules()
    app.finalize()
    for fixup in app._fixups:
        if hasattr(fixup, 'worker_fixup'):
            fixup.worker_fixup.install()
    pool = RsTaskPool(limit=1, app=app, initargs=(app, 'orm@probe'))
    pool.start()

    def call(fun, args=()):
        result = pool.apply_async(fun, args)
        while not result.ready():
            pool.wait(.01)
        return result.get()

    try:
        pid = call(os.getpid)
        inherited = anon_ranges(pid)
        report = dict(mode=options.mode, pid=pid, initial=call(state))
        shared = shared_pages(pid, inherited)
        shared_after_one = shared
        with open(f'/proc/{pid}/clear_refs', 'w') as target:
            target.write('4')
        for i in range(2000):
            call(task, (options.mode, i))
            if i in (0, 9, 1999):
                report[str(i + 1)] = call(state)
                pages = softdirty(pid, inherited)
                report[str(i + 1)]['dirty_in_initial_ranges_kib'] = len(pages) * 4
                report[str(i + 1)]['inherited_cow_kib'] = len(
                    (shared & pages) - shared_pages(pid, inherited)) * 4
                report[str(i + 1)]['cow_since_task_one_kib'] = len(
                    (shared_after_one & pages) - shared_pages(pid, inherited)) * 4
                if i == 0:
                    shared_after_one = shared_pages(pid, inherited)
                    with open(f'/proc/{pid}/clear_refs', 'w') as target:
                        target.write('4')
        report['reclaim'] = call(reclaim)
        report['page_types_1_to_2000'] = call(classify, (pages,))
        print(json.dumps(report))
    finally:
        pool.stop()


if __name__ == '__main__':
    main()
