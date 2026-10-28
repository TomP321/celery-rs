"""Pagine scritte dall'initializer, senza tracemalloc (solo diagnosi)."""
import collections
import gc
import json
import os
import sys
import types
from pathlib import Path
from memapp import app
from sd import anon_ranges, private, softdirty
from celery.concurrency import prefork

original = prefork.process_initializer


def initialize(*args, **kwargs):
    pid = os.getpid()
    ranges = anon_ranges(pid)
    before = private()
    Path('/proc/self/clear_refs').write_text('4')
    original(*args, **kwargs)
    pages = softdirty(pid, ranges)
    after = private()
    gc.unfreeze()
    modules = {id(vars(m)): n for n, m in sys.modules.copy().items() if isinstance(m, types.ModuleType)}
    counts = collections.defaultdict(collections.Counter)
    seen = set()
    def visit(obj):
        address = id(obj)
        if address in seen or address >> 12 not in pages:
            return
        seen.add(address)
        kind = type(obj).__name__
        if isinstance(obj, types.FunctionType):
            kind += ':' + str(obj.__module__)
        elif isinstance(obj, types.CodeType):
            kind += ':' + obj.co_filename
        elif isinstance(obj, type):
            kind += ':' + obj.__module__
        elif isinstance(obj, dict) and address in modules:
            kind += ':' + modules[address]
        counts[address >> 12][kind] += 1
    for obj in gc.get_objects():
        visit(obj)
        for ref in gc.get_referents(obj):
            visit(ref)
    dominant = collections.Counter(c.most_common(1)[0][0] for c in counts.values())
    Path(os.environ['CELERY_RS_INIT_PROBE']).mkdir(parents=True,exist_ok=True)
    (Path(os.environ['CELERY_RS_INIT_PROBE'])/f'{pid}.json').write_text(json.dumps(dict(
        before=before, after=after, written=len(pages), dominant=dominant.most_common(),
        unknown=len(pages-counts.keys())),indent=2))
    gc.freeze()


prefork.process_initializer = initialize
