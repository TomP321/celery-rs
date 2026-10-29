"""Probe RPC/ping and profile clients without changing worker execution."""
import argparse
import json
import os
import signal
import subprocess
import time

from bench import HERE, ROOT, memory_snapshot, venv_python

CODE = '''
import cProfile, json, statistics, time
from benchapp import app, add, timed
from bench import memory_snapshot
import os

def quantiles(values):
    values = sorted(values)
    return {'p50': statistics.median(values), 'p99': values[min(len(values)-1, int(len(values)*.99))]}
add.delay(1, 1).get(timeout=30)
assert app.control.inspect(limit=1).ping()
for i in range(50):
    assert add.delay(i, 1).get(timeout=30) == i + 1
    assert app.control.inspect(limit=1).ping()
profile = cProfile.Profile()
pub, get, total, ping = [], [], [], []
for i in range(1000):
    t0 = time.perf_counter()
    result = add.delay(i, 1)
    t1 = time.perf_counter()
    assert result.get(timeout=30) == i + 1
    t2 = time.perf_counter()
    pub.append((t1-t0)*1000)
    get.append((t2-t1)*1000)
    total.append((t2-t0)*1000)
for i in range(1000):
    t0 = time.perf_counter()
    assert app.control.inspect(limit=1).ping()
    ping.append((time.perf_counter()-t0)*1000)
outbound, execution, response = [], [], []
for i in range(100):
    sent = time.monotonic_ns()
    original, entered, exited = timed.delay(sent).get(timeout=30)
    received = time.monotonic_ns()
    assert original == sent
    outbound.append((entered-sent)/1e6)
    execution.append((exited-entered)/1e6)
    response.append((received-exited)/1e6)
profile.enable()
for i in range(100):
    assert add.delay(i, 1).get(timeout=30) == i + 1
    assert app.control.inspect(limit=1).ping()
profile.disable()
profile.dump_stats(os.environ['BENCH_PROFILE'])
print(json.dumps({'publish_ms': quantiles(pub), 'get_ms': quantiles(get),
                  'rpc_ms': quantiles(total), 'ping_ms': quantiles(ping),
                  'publish_broker_master_ipc_ms': quantiles(outbound),
                  'child_ms': quantiles(execution), 'response_ms': quantiles(response),
                  'client_pss_kb': sum(p['pss_kb'] for p in memory_snapshot(os.getpid()))}))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase', required=True)
    parser.add_argument('--pool', choices=('solo', 'prefork'), default='prefork')
    args = parser.parse_args()
    if os.environ.get('CELERY_RS_BROKER_LOCKED') != '1':
        parser.error('run under tools/dev/with-brokers')
    directory = ROOT / 'bench/results/08c'
    directory.mkdir(parents=True, exist_ok=True)
    for implementation in ('ref', 'rs'):
        env = dict(os.environ, PYTHONPATH=str(HERE),
                   BENCH_BROKER=os.environ['CELERY_RS_AMQP_URL'], BENCH_BACKEND='rpc://',
                   BENCH_QUEUE=f'probe-08c-{os.getpid()}-{implementation}')
        with (directory / f'{args.phase}-{implementation}-worker.log').open('w') as log:
            worker = subprocess.Popen([venv_python(f'.venv-{implementation}'), '-m', 'celery',
                '-A', 'benchapp', 'worker', '-P', args.pool, '-c', '10', '-l', 'warning',
                '--without-gossip', '--without-mingle'], cwd=HERE, env=env,
                stdout=log, stderr=log, start_new_session=True)
            try:
                for _ in range(60):
                    if worker.poll() is not None:
                        raise RuntimeError('worker exited')
                    ready = subprocess.run([venv_python('.venv-ref'), '-c',
                        'from benchapp import app; assert app.control.inspect(timeout=1,limit=1).ping()'],
                        env=env, cwd=HERE, capture_output=True)
                    if ready.returncode == 0:
                        break
                    time.sleep(.5)
                else:
                    raise RuntimeError('worker not ready')
                results = {}
                for client in ('ref', 'rs'):
                    env['BENCH_PROFILE'] = str(directory / f'{args.phase}-{implementation}-{client}.prof')
                    output = subprocess.run([venv_python(f'.venv-{client}'), '-c', CODE],
                        env=env, cwd=HERE, capture_output=True, text=True, check=True)
                    results[client] = json.loads(output.stdout.strip().splitlines()[-1])
                results['worker_pss_kb'] = sum(p['pss_kb'] for p in memory_snapshot(worker.pid))
                path = directory / f'{args.phase}-latency-{implementation}.json'
                path.write_text(json.dumps(results, indent=2))
                print(path, json.dumps(results), flush=True)
            finally:
                os.killpg(worker.pid, signal.SIGTERM)
                try:
                    worker.wait(30)
                except subprocess.TimeoutExpired:
                    os.killpg(worker.pid, signal.SIGKILL)
                    worker.wait()


if __name__ == '__main__':
    main()
