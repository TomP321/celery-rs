"""Pagine soft-dirty tra il task 10 e il task 2000 nel pool reale.

Eseguire sotto tools/dev/with-brokers. La sonda non usa tracemalloc: altera
le allocazioni del percorso misurato. I tipi vengono contati solo dopo la misura.
"""
import argparse
import collections
import gc
import json
import os
import struct
import sys


def anon_ranges(pid):
    out = []
    with open(f'/proc/{pid}/maps') as source:
        for line in source:
            fields = line.split()
            name = fields[5] if len(fields) > 5 else ''
            if name in ('', '[heap]') and 'w' in fields[1]:
                a, b = (int(x, 16) for x in fields[0].split('-'))
                out.append((a, b, name or 'anon'))
    return out


def softdirty(pid, ranges):
    pages = set()
    with open(f'/proc/{pid}/pagemap', 'rb') as source:
        for a, b, _ in ranges:
            source.seek((a >> 12) * 8)
            data = source.read(((b - a) >> 12) * 8)
            for i, (entry,) in enumerate(struct.iter_unpack('<Q', data)):
                if entry >> 63 & 1 and entry >> 55 & 1:
                    pages.add((a >> 12) + i)
    return pages


def snapshot():
    return os.getpid(), private()


def private():
    with open('/proc/self/smaps_rollup') as source:
        return sum(int(line.split()[1]) for line in source
                   if line.startswith(('Private_Clean:', 'Private_Dirty:')))


def traced_add(i, payload):
    from celery.app.trace import trace_task
    from memapp import app
    task = app.tasks['memapp.add']
    args = (payload, '') if payload else (i, i)
    result = trace_task(task, str(i), args, {},
                        request={'id': str(i), 'task': task.name,
                                 'ignore_result': True})
    return result.info is None


def classify(pages):
    gc.unfreeze()
    known = collections.defaultdict(collections.Counter)
    seen = set()
    def visit(obj):
        address = id(obj)
        if address not in seen:
            seen.add(address)
            if address >> 12 in pages:
                known[address >> 12][type(obj).__name__] += 1
    for obj in gc.get_objects():
        visit(obj)
        for ref in gc.get_referents(obj):
            visit(ref)
    dominant = collections.Counter(counter.most_common(1)[0][0]
                                    for counter in known.values())
    return {'dominant_types': dominant.most_common(15),
            'unknown_pages': len(pages - known.keys())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--payload-bytes', type=int, default=0)
    options = parser.parse_args()
    os.environ.setdefault('BROKER', 'memory://')
    from memapp import app
    from celery.concurrency.rs_prefork import RsTaskPool
    app.loader.import_default_modules()
    app.finalize()
    pool = RsTaskPool(limit=1, app=app, initargs=(app, 'sd@probe'), threads=True)
    pool.start()
    def call(fun, args=()):
        result = pool.apply_async(fun, args)
        while not result.ready():
            pool.wait(0.01)
        return result.get()
    try:
        payload = 'x' * options.payload_bytes
        for i in range(10):
            assert call(traced_add, (i, payload))
        child, before = call(snapshot)
        parent_before = private()
        inherited = anon_ranges(child)
        for pid in (child, os.getpid()):
            with open(f'/proc/{pid}/clear_refs', 'w') as target:
                target.write('4')
        for i in range(10, 2000):
            assert call(traced_add, (i, payload))
        _, after = call(snapshot)
        parent_after = private()
        pages = softdirty(child, anon_ranges(child))
        old_pages = {pg for pg in pages
                     if any(a >> 12 <= pg < b >> 12 for a, b, _ in inherited)}
        master_pages = softdirty(os.getpid(), anon_ranges(os.getpid()))
        # La classificazione alloca, quindi deve seguire tutte le letture.
        report = call(classify, (pages,))
        print(json.dumps(dict(child_dirty_pages=len(pages),
                              inherited_dirty_pages=len(old_pages),
                              master_dirty_pages=len(master_pages),
                              child_private_growth_kib=after-before,
                              master_private_growth_kib=parent_after-parent_before,
                              **report)))
    finally:
        pool.stop()


if __name__ == '__main__':
    main()
