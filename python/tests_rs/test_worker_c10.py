"""Compare GC and bounded memory after 30000 jobs at concurrency 10."""
import json
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("gc_options", [[], ["disabled"]], ids=["default", "custom-disabled"])
def test_c10_gc_matches_reference_and_memory_stabilizes(gc_options):
    outcomes = {}
    for implementation in ('ref', 'rs'):
        env = {key: value for key, value in os.environ.items()
               if not key.startswith('PYTHON')}
        result = subprocess.run(
            [str(ROOT / f'.venv-{implementation}/bin/python'),
             str(Path(__file__).with_name('_c10_probe.py')), implementation, *gc_options],
            cwd=ROOT, env=env, capture_output=True, text=True, timeout=180)
        assert result.returncode == 0, result.stderr
        outcomes[implementation] = json.loads(result.stdout.splitlines()[-1])
    assert outcomes['rs']['gc'] == outcomes['ref']['gc']
    for pid, before in outcomes['rs']['before_kb'].items():
        # Allow arena and PSS noise; a per-job leak would exceed 2 MiB per child over 30000 jobs after warm-up.
        assert outcomes['rs']['after_kb'][pid] - before < 2048, outcomes


def test_zygote_warms_cffi_parser_before_fork():
    code = """
from celery.concurrency._rs_zygote import _warm_cffi
from cffi import cparser
assert cparser._parser_cache is None
_warm_cffi()
assert cparser._parser_cache is not None
"""
    result = subprocess.run(
        [str(ROOT / '.venv-rs/bin/python'), '-c', code],
        capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
