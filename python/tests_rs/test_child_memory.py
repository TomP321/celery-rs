"""Child memory after load and prefork initialization without CFFI parsing."""
import json
import os
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_native_pdeathsig_matches_reference_without_initialization_effects():
    code = '''
import ctypes, gc, json, logging, signal, sys
from celery import platforms, signals
libc = ctypes.CDLL(None)
value = ctypes.c_int()
before = (gc.isenabled(), gc.get_threshold(), dict(logging.Logger.manager.loggerDict),
          signal.getsignal(signal.SIGUSR1))
events = []
for name in ('worker_process_init', 'task_prerun', 'task_postrun', 'task_success'):
    getattr(signals, name).connect(lambda **kwargs: events.append(kwargs), weak=False)
modules = set(sys.modules)
result = platforms._set_pdeathsig(signal.SIGKILL)
assert platforms._set_pdeathsig(-1) is None
assert libc.prctl(2, ctypes.byref(value)) == 0
assert before == (gc.isenabled(), gc.get_threshold(), dict(logging.Logger.manager.loggerDict),
                  signal.getsignal(signal.SIGUSR1))
assert not events
print(json.dumps({'result': result, 'signal': value.value,
                  'parser_loaded': 'cffi.cparser' in set(sys.modules) - modules}))
'''
    results = {}
    for impl in ('ref', 'rs'):
        run = subprocess.run([str(ROOT / f'.venv-{impl}/bin/python'), '-c', code],
                             cwd='/tmp', capture_output=True, text=True, timeout=30)
        assert run.returncode == 0, run.stderr
        results[impl] = json.loads(run.stdout)
    assert results['rs']['result'] == results['ref']['result']
    assert results['rs']['signal'] == results['ref']['signal']
    assert not results['rs']['parser_loaded']


@pytest.mark.parametrize('concurrency', [10, 50])
@pytest.mark.parametrize('task,limit_mib', [('pure', 9), ('bench-toplevel', 12)])
def test_prefork_child_memory_after_tasks(tmp_path, concurrency, task, limit_mib):
    label = tmp_path / f'child-{concurrency}-{task}'
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('CELERY_RS_MEMORY_PROBE')}
    run = subprocess.run(
        [str(ROOT / '.venv-rs/bin/python'), str(ROOT / 'bench/bench.py'),
         '--venv', str(ROOT / '.venv-rs'), '--pool', 'prefork',
         '--concurrency', str(concurrency), '--task', task, '--prefill',
         '--n', '10000', '--latency-n', '20', '--label', str(label)],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=180)
    assert run.returncode == 0, run.stdout + run.stderr
    result = json.loads(label.with_suffix('.json').read_text())
    children = [row for row in result['after_processes'] if row['role'] == 'child']
    assert result['completed_tasks'] == result['n']
    assert len(children) == concurrency
    assert max(row['pss_kb'] for row in children) <= limit_mib * 1024, result
