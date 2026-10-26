"""La sonda di capacità deve escludere la pubblicazione dal tempo del worker."""
import json
import os
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('broker', ['redis', 'amqp'])
@pytest.mark.parametrize('task', ['pure', 'bench', 'bench-toplevel'])
def test_prefill(broker, task):
    env = dict(os.environ)
    env['BENCH_BROKER'] = (env.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/0'
                           if broker == 'redis' else
                           env.get('CELERY_RS_AMQP_URL', 'amqp://guest:guest@127.0.0.1:5673//'))
    label = f'test-08c-{os.getpid()}-{broker}-{task}'
    path = ROOT / 'bench/results' / f'{label}.json'
    try:
        result = subprocess.run([
            str(ROOT / '.venv-ref/bin/python'), 'bench/bench.py',
            '--venv', '.venv-rs', '--prefill', '--concurrency', '2',
            '--n', '100', '--latency-n', '5', '--task', task, '--label', label,
        ], cwd=ROOT, env=env, capture_output=True, text=True, timeout=120)
        assert result.returncode == 0, result.stdout + result.stderr
        data = json.loads(path.read_text())
        assert data['prefill'] is True
        assert data['completed_tasks'] == data['n'] == 100
        assert data['completed_before_resume'] == 0
        assert data['worker_tasks_per_s'] == 100 / data['drain_after_publish_s']
        assert data['end_to_end_s'] == data['drain_after_publish_s']
        assert data['pss_peak_kb'] >= data['pss_idle_kb'] > 0
    finally:
        path.unlink(missing_ok=True)


def test_toplevel_import_matches_reference():
    code = """
import json, sys
import benchapp
print(json.dumps('redis' in sys.modules))
"""
    for toplevel in ('0', '1'):
        outputs = []
        for impl in ('ref', 'rs'):
            env = dict(os.environ, PYTHONPATH=str(ROOT / 'bench'), BENCH_TOPLEVEL=toplevel)
            output = subprocess.check_output([
                str(ROOT / f'.venv-{impl}/bin/python'), '-c', code,
            ], cwd=ROOT / 'bench', env=env, text=True)
            outputs.append(json.loads(output))
        assert outputs == [toplevel == '1'] * 2
