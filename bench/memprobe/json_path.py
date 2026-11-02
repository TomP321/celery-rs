"""Measure native JSON paths; --extension-dir selects a saved extension build."""
import argparse
import gc
import json
import statistics
import sys
import timeit

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--extension-dir')
parser.add_argument('--number', type=int, default=20_000)
parser.add_argument('--repeat', type=int, default=7)
options = parser.parse_args()
if options.extension_dir:
    sys.path.insert(0, options.extension_dir)

import _celery_rs as rs


def default(value):
    raise TypeError(type(value))


cases = {
    'task': [
        ['hello', 42, True, None, 3.5],
        {'key': 'value', 'unicode': 'héllo'},
        {'callbacks': None, 'errbacks': None, 'chain': None, 'chord': None},
    ],
    'dict': {str(i): [i, i + 0.5, False, 'value'] for i in range(32)},
    'ints': list(range(100)),
    'bigints': [2**100, -(2**200)],
}
results = {}
for name, value in cases.items():
    operation = lambda: rs.json_dumps(value, default)
    operation()
    gc.disable()
    try:
        samples = timeit.repeat(operation, number=options.number, repeat=options.repeat)
    finally:
        gc.enable()
    results[name] = {'median_ns': statistics.median(samples) * 1e9 / options.number, 'seconds': samples}
for name, value in [('loads', cases['dict']), ('loads_big', cases['bigints'])]:
    data = rs.json_dumps(value, default)
    operation = lambda: rs.json_loads(data)
    operation()
    gc.disable()
    try:
        samples = timeit.repeat(operation, number=options.number, repeat=options.repeat)
    finally:
        gc.enable()
    results[name] = {'median_ns': statistics.median(samples) * 1e9 / options.number, 'seconds': samples}
print(json.dumps(results, indent=2))
