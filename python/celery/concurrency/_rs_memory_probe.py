"""Diagnostica esplicita dei figli; nessun import nel percorso normale."""
import ctypes
import gc
import json
import os
from pathlib import Path
import sys
import time
import tracemalloc


class MemoryProbe:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.pid = os.getpid()
        self.next_sample = 0
        self.started = time.monotonic()
        self.modules = set(sys.modules)
        tracemalloc.start(10)
        self.previous = tracemalloc.take_snapshot()
        self.libc = ctypes.CDLL(None)
        self.libc.fopen.argtypes = (ctypes.c_char_p, ctypes.c_char_p)
        self.libc.fopen.restype = ctypes.c_void_p
        self.libc.fclose.argtypes = (ctypes.c_void_p,)
        self.libc.malloc_info.argtypes = (ctypes.c_int, ctypes.c_void_p)
        self.sample(0)

    @staticmethod
    def mappings():
        totals = {}
        category = 'anonymous'
        for line in Path('/proc/self/smaps').read_text().splitlines():
            parts = line.split()
            if parts and '-' in parts[0]:
                name = parts[5] if len(parts) > 5 else ''
                category = ('heap' if name == '[heap]' else
                            'stack' if name.startswith('[stack') else
                            'libraries' if name.startswith('/') else 'anonymous')
            elif len(parts) == 3 and parts[0] in (
                    'Rss:', 'Pss:', 'Private_Dirty:', 'Private_Clean:',
                    'Shared_Dirty:', 'Shared_Clean:'):
                group = totals.setdefault(category, {})
                key = parts[0].rstrip(':')
                group[key] = group.get(key, 0) + int(parts[1])
        return totals

    def sample(self, completed):
        now = time.monotonic()
        if completed not in (0, 1, 10, 100, 1000, 2000) and now < self.next_sample:
            return
        self.next_sample = now + 30
        memory = {}
        for line in Path('/proc/self/smaps_rollup').read_text().splitlines():
            parts = line.split()
            if len(parts) == 3 and parts[2] == 'kB':
                memory[parts[0].rstrip(':')] = int(parts[1])
        snapshot = tracemalloc.take_snapshot()
        diff = snapshot.compare_to(self.previous, 'traceback')[:20]
        self.previous = snapshot
        snapshot.dump(str(self.directory / f'{self.pid}.snapshot'))
        try:
            from _celery_rs import _allocator_stats
            rust = _allocator_stats()
        except ImportError:
            rust = None
        record = dict(seconds=now - self.started, completed=completed, memory_kib=memory,
                      mappings_kib=self.mappings(),
                      new_modules=sorted(set(sys.modules) - self.modules),
                      uss_kib=memory.get('Private_Clean', 0) + memory.get('Private_Dirty', 0),
                      python_bytes=tracemalloc.get_traced_memory(), rust_bytes=rust,
                      tracemalloc_metadata_bytes=tracemalloc.get_tracemalloc_memory(),
                      gc=gc.get_count(), diff=[dict(bytes=d.size_diff, count=d.count_diff,
                          traceback=[f'{frame.filename}:{frame.lineno}' for frame in d.traceback])
                          for d in diff])
        self.modules = set(sys.modules)
        if os.environ.get('CELERY_RS_MEMORY_PROBE_RECLAIM') == '1':
            # Esperimento distruttivo delle sole statistiche: mai attivo in produzione.
            def current():
                return Path('/proc/self/smaps_rollup').read_text()
            record['before_gc'] = current()
            record['collected'] = gc.collect()
            record['after_gc'] = current()
            record['python_after_gc'] = tracemalloc.get_traced_memory()
            self.libc.malloc_trim(0)
            record['after_trim'] = current()
        with (self.directory / f'{self.pid}.jsonl').open('a') as output:
            output.write(json.dumps(record) + '\n')
        path = self.directory / f'{self.pid}-{completed}.malloc.xml'
        stream = self.libc.fopen(os.fsencode(path), b'w')
        if stream:
            try:
                self.libc.malloc_info(0, stream)
            finally:
                self.libc.fclose(stream)
