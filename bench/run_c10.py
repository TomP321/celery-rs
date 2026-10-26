"""Matrice c10 della spec 08d, da eseguire sotto tools/dev/with-brokers.

Esempio: tools/dev/with-brokers .venv-ref/bin/python bench/run_c10.py --phase after
"""
import argparse
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase', required=True)
    parser.add_argument('--repeats', type=int, default=2)
    parser.add_argument('--n', type=int, default=30000)
    args = parser.parse_args()
    if os.environ.get('CELERY_RS_BROKER_LOCKED') != '1':
        parser.error('eseguire sotto tools/dev/with-brokers')
    directory = ROOT / 'bench/results/08d'
    directory.mkdir(parents=True, exist_ok=True)
    for repeat in range(1, args.repeats + 1):
        for broker in ('redis', 'amqp'):
            env = dict(os.environ)
            env['BENCH_BROKER'] = ('redis://127.0.0.1:6390/0' if broker == 'redis'
                                   else 'amqp://guest:guest@127.0.0.1:5673//')
            for task in ('bench', 'pure'):
                # Alternare l'ordine limita gli effetti di riscaldamento.
                implementations = ('ref', 'rs') if repeat % 2 else ('rs', 'ref')
                for implementation in implementations:
                    label = f'{args.phase}-{implementation}-{broker}-{task}-{repeat}'
                    print(label, flush=True)
                    with (directory / f'{label}.log').open('w') as log:
                        subprocess.run(
                            [str(ROOT / '.venv-ref/bin/python'), 'bench/bench.py',
                             '--venv', f'.venv-{implementation}', '--pool', 'prefork',
                             '--concurrency', '10', '--n', str(args.n),
                             '--task', task, '--label', f'08d/{label}'],
                            cwd=ROOT, env=env, stdout=log, stderr=log, check=True)


if __name__ == '__main__':
    main()
