"""Matrice capacità/PSS 08c; eseguire sotto tools/dev/with-brokers."""
import argparse
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase', required=True)
    parser.add_argument('--n', type=int, default=30000)
    parser.add_argument('--repeats', type=int, default=2)
    parser.add_argument('--start-repeat', type=int, default=1)
    parser.add_argument('--brokers', nargs='+', choices=('redis', 'amqp'), default=['redis', 'amqp'])
    parser.add_argument('--concurrency', nargs='+', type=int, default=[4, 10])
    args = parser.parse_args()
    if os.environ.get('CELERY_RS_BROKER_LOCKED') != '1':
        parser.error('eseguire sotto tools/dev/with-brokers')
    directory = ROOT / 'bench/results/08c'
    directory.mkdir(parents=True, exist_ok=True)
    for repeat in range(args.start_repeat, args.repeats + 1):
        for broker in args.brokers:
            env = dict(os.environ)
            env['BENCH_BROKER'] = (env['CELERY_RS_REDIS_URL'] + '/0' if broker == 'redis'
                                   else env['CELERY_RS_AMQP_URL'])
            for concurrency in args.concurrency:
                for task in ('pure', 'bench', 'bench-toplevel'):
                    for impl in (('ref', 'rs') if repeat % 2 else ('rs', 'ref')):
                        label = f'{args.phase}-{impl}-{broker}-c{concurrency}-{task}-{repeat}'
                        print(label, flush=True)
                        with (directory / f'{label}.log').open('w') as log:
                            subprocess.run([
                                str(ROOT / '.venv-ref/bin/python'), 'bench/bench.py',
                                '--venv', f'.venv-{impl}', '--prefill',
                                '--concurrency', str(concurrency), '--n', str(args.n),
                                '--task', task, '--label', f'08c/{label}',
                            ], cwd=ROOT, env=env, stdout=log, stderr=log, check=True)


if __name__ == '__main__':
    main()
