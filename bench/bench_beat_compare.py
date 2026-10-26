"""Confronto alternato del ciclo invii beat e profilo cProfile del branch.

    tools/dev/with-brokers .venv-ref/bin/python bench/bench_beat_compare.py

Il Python di main è estratto da git; l'estensione nativa e le dipendenze sono
identiche per isolare gli import. Rispetto a 303a59a, l'unica modifica Rust
08b è in saferepr, che il benchmark non chiama.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import pstats
import subprocess
import tarfile
import tempfile
from pathlib import Path

from bench_beat import BROKERS, ROOT, SENDS


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', default='303a59a')
    parser.add_argument('--seconds', type=float, default=8)
    parser.add_argument('--runs', type=int, default=3)
    parser.add_argument('--profile', action='store_true')
    args = parser.parse_args()
    results = ROOT / 'bench/results'
    rows = []
    with tempfile.TemporaryDirectory(prefix='beat-baseline-') as directory:
        archive = subprocess.run(['git', 'archive', args.baseline, 'python'],
                                 cwd=ROOT, capture_output=True, check=True)
        with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as source:
            source.extractall(directory, filter='data')
        native = Path(directory, 'python/_celery_rs/_celery_rs.abi3.so')
        native.symlink_to(ROOT / 'python/_celery_rs/_celery_rs.abi3.so')
        for run in range(args.runs):
            for label in ('main', 'branch'):
                paths = [str(ROOT / 'bench'), str(ROOT / 'python')]
                if label == 'main':
                    paths.insert(0, str(Path(directory, 'python')))
                env = {key: value for key, value in os.environ.items()
                       if not key.startswith('PYTHON')}
                env.update(PYTHONPATH=os.pathsep.join(paths), PYTHONHASHSEED='0',
                           BENCH_BROKER=BROKERS['redis'], BENCH_SECONDS=str(args.seconds))
                process = subprocess.run([str(ROOT / '.venv-rs/bin/python'), '-c', SENDS],
                                         cwd=directory, env=env, capture_output=True,
                                         text=True, check=True)
                row = dict(run=run + 1, label=label,
                           **json.loads(process.stdout.strip().splitlines()[-1]))
                rows.append(row)
                print(row, flush=True)
        if args.profile:
            profile = Path(directory, 'beat.prof')
            code = SENDS.replace('cpu0 = time.process_time()',
                                 'import cProfile\nprof = cProfile.Profile()\nprof.enable()\n'
                                 'cpu0 = time.process_time()')
            code = code.replace('scheduler.close()',
                                f'prof.disable()\nprof.dump_stats({str(profile)!r})\n'
                                'scheduler.close()')
            subprocess.run([str(ROOT / '.venv-rs/bin/python'), '-c', code],
                           cwd=ROOT, env=env, check=True, capture_output=True)
            with (results / '08b-beat-profile.txt').open('w') as output:
                pstats.Stats(str(profile), stream=output).strip_dirs().sort_stats(
                    'cumtime').print_stats(40)
    (results / '08b-beat-alternating.json').write_text(json.dumps(rows, indent=2))


if __name__ == '__main__':
    main()
