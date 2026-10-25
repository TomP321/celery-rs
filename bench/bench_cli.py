"""Benchmark della CLI: riferimento vs celery-rs.

    tools/dev/with-brokers .venv-rs/bin/python bench/bench_cli.py

Per ogni broker (Redis e AMQP, su database/vhost dedicati) misura, con la
mediana di ``--runs`` esecuzioni:

  * tempo e max RSS di ``celery --help``, ``call``, ``result``, ``purge -f``,
    ``status`` (nessun worker), ``inspect ping`` con 1 worker (timeout 1s e
    ``--timeout 0.2``);
  * ``app.control.inspect().ping()`` 1000 volte nello stesso processo
    (latenza p50 per chiamata con ``limit=1`` e RSS finale).

Il worker è sempre quello del riferimento: la variabile misurata è la CLI.
I risultati sono in ``bench/results/cli-<broker>.json``.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import signal
import statistics
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
REF_VENV = ROOT / '.venv-ref'
RS_VENV = ROOT / '.venv-rs'
REDIS = 'redis://127.0.0.1:6390'
BACKEND = f'{REDIS}/15'
RABBIT_API = 'http://127.0.0.1:15673/api'
RABBIT = 'amqp://guest:guest@127.0.0.1:5673'

#: Obiettivi di RSS per caso (rapporto rs/ref): `--help` non deve peggiorare,
#: gli altri devono essere almeno il 10% sotto il riferimento.
RSS_TARGETS = {'help': 1.0, 'status': 0.9, 'inspect-ping': 0.9,
               'inspect-ping-0.2': 0.9, 'call': 0.9}

TIME_RE = re.compile(r'^__BENCH__ (\d+) ([\d.]+)$', re.M)
UUID_RE = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')


def unique(prefix):
    return f'{prefix}-{uuid.uuid4().hex[:10]}'


def python(venv):
    return str((Path(venv).resolve() / 'bin' / 'python'))


def clean_env(**extra):
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('PYTHON') and not key.startswith('BENCH_')}
    env['PYTHONPATH'] = str(HERE)
    env['PYTHONHASHSEED'] = '0'
    env.update({key: str(value) for key, value in extra.items()})
    return env


def rabbit_api(method, path, data=None):
    body = json.dumps(data).encode() if data is not None else None
    request = urllib.request.Request(
        f'{RABBIT_API}{path}', method=method, data=body,
        headers={'Authorization': 'Basic ' + base64.b64encode(b'guest:guest').decode(),
                 'content-type': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = response.read()
            return json.loads(payload) if payload else None
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def broker_config(name):
    """URL del broker dedicato e (per AMQP) il vhost creato per la misura."""
    if name == 'redis':
        return f'{REDIS}/14', None
    vhost = f'bench-cli-{os.getpid()}'
    quoted = urllib.parse.quote(vhost, safe='')
    rabbit_api('PUT', f'/vhosts/{quoted}', {'description': 'bench cli'})
    rabbit_api('PUT', f'/permissions/{quoted}/guest',
               {'configure': '.*', 'write': '.*', 'read': '.*'})
    return f'{RABBIT}/{quoted}', quoted


def drop_vhost(quoted):
    if quoted:
        rabbit_api('DELETE', f'/vhosts/{quoted}')


def measure(venv, args, runs, env, cwd=HERE):
    """Mediana di tempo (s) e max RSS (KB) su *runs* esecuzioni."""
    times, rss = [], []
    for _ in range(runs):
        process = subprocess.run(
            ['/usr/bin/time', '-f', '__BENCH__ %M %e', python(venv), '-P',
             '-m', 'celery', *args],
            cwd=cwd, env=env, capture_output=True, text=True)
        match = TIME_RE.search(process.stderr)
        if match is None:
            raise SystemExit(
                f'misura fallita per {args}:\n{process.stdout[-2000:]}\n'
                f'{process.stderr[-3000:]}')
        rss.append(int(match.group(1)))
        times.append(float(match.group(2)))
    return {
        'time_s': statistics.median(times),
        'rss_kb': statistics.median(rss),
    }


class Worker:
    def __init__(self, broker, backend, queue, log):
        self.env = clean_env(
            BENCH_CLI_BROKER=broker, BENCH_CLI_BACKEND=backend,
            BENCH_CLI_QUEUE=queue)
        self.log = log
        self.process = None

    def start(self):
        self.process = subprocess.Popen(
            [python(REF_VENV), '-P', '-m', 'celery', '-A', 'cli_app',
             'worker', '-P', 'solo', '-c', '1', '-Q', self.env['BENCH_CLI_QUEUE'],
             '-n', 'bench@localhost', '-l', 'warning',
             '--without-gossip', '--without-mingle'],
            cwd=HERE, env=self.env, stdout=self.log,
            stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise SystemExit('worker terminato in avvio')
            # `status` senza worker è misurato prima: qui basta la connessione.
            result = subprocess.run(
                [python(REF_VENV), '-P', '-m', 'celery', '-A', 'cli_app',
                 '-b', self.env['BENCH_CLI_BROKER'], 'inspect', 'ping',
                 '-t', '1'],
                cwd=HERE, env=self.env, capture_output=True, text=True)
            if 'pong' in result.stdout:
                return self
            time.sleep(0.3)
        self.stop()
        raise SystemExit('worker non pronto')

    def stop(self):
        if self.process is None or self.process.poll() is not None:
            return
        try:
            os.killpg(self.process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            self.process.wait(30)
        except subprocess.TimeoutExpired:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait(10)


def call_and_wait(env, broker, backend):
    """Pubblica un task col client rs e attende il risultato: id pronto per `result`."""
    result = subprocess.run(
        [python(RS_VENV), '-P', '-m', 'celery', '-A', 'cli_app',
         'call', 'bench_cli.add', '-a', '[1, 2]'],
        cwd=HERE, env=env, capture_output=True, text=True)
    match = UUID_RE.search(result.stdout)
    if match is None:
        raise SystemExit(f'call fallita:\n{result.stdout}\n{result.stderr[-3000:]}')
    task_id = match.group(0)
    wait = (
        'import os, sys\n'
        'from celery import Celery\n'
        "app = Celery('bench_cli', broker=os.environ['BENCH_CLI_BROKER'],"
        " backend=os.environ['BENCH_CLI_BACKEND'])\n"
        'print(app.AsyncResult(sys.argv[1]).get(timeout=30))\n'
    )
    waited = subprocess.run(
        [python(REF_VENV), '-P', '-c', wait, task_id],
        cwd=HERE, env=env, capture_output=True, text=True)
    if waited.returncode != 0:
        raise SystemExit(f'risultato non disponibile:\n{waited.stderr[-3000:]}')
    return task_id


def probe(venv, env, runs=1000):
    result = subprocess.run(
        [python(venv), '-P', str(HERE / '_cli_ping_probe.py'), '--n', str(runs)],
        cwd=HERE, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(f'probe fallita ({venv}):\n{result.stderr[-3000:]}')
    return json.loads(result.stdout.strip().splitlines()[-1])


def bench_broker(name, runs):
    broker, vhost = broker_config(name)
    queue = unique(f'bench-cli-{name}')
    base_env = clean_env(BENCH_CLI_BROKER=broker, BENCH_CLI_BACKEND=BACKEND,
                         BENCH_CLI_QUEUE=queue)
    status_env = clean_env(BENCH_CLI_BROKER=broker,
                           BENCH_CLI_BACKEND=BACKEND,
                           BENCH_CLI_QUEUE=unique('bench-cli-status'))

    venvs = {'ref': REF_VENV, 'rs': RS_VENV}
    data = {'broker': name, 'runs': runs, 'cases': {}, 'control_probe': {}}

    # --- status senza worker (prima di avviarlo) -------------------------------------
    try:
        for label, venv in venvs.items():
            data['cases'][f'status/{label}'] = measure(
                venv, ['-A', 'cli_app', 'status', '-t', '1'], runs, status_env)
        log_path = Path(tempfile.gettempdir()) / f'celery-bench-cli-{os.getpid()}.log'
        with open(log_path, 'w') as log:
            worker = Worker(broker, BACKEND, queue, log).start()
        try:
            # --- call / result / purge / inspect -------------------------------------
            for label, venv in venvs.items():
                data['cases'][f'help/{label}'] = measure(
                    venv, ['--help'], runs, base_env)
                data['cases'][f'call/{label}'] = measure(
                    venv, ['-A', 'cli_app', 'call', 'bench_cli.add', '-a',
                           '[1, 2]'], runs, base_env)
            task_id = call_and_wait(base_env, broker, BACKEND)
            for label, venv in venvs.items():
                data['cases'][f'result/{label}'] = measure(
                    venv, ['-A', 'cli_app', 'result', task_id], runs, base_env)
                data['cases'][f'purge/{label}'] = measure(
                    venv, ['-A', 'cli_app', 'purge', '-f', '-Q',
                           f'{queue}-{label}'], runs, base_env)
                data['cases'][f'inspect-ping/{label}'] = measure(
                    venv, ['-A', 'cli_app', 'inspect', 'ping', '-t', '1'],
                    runs, base_env)
                data['cases'][f'inspect-ping-0.2/{label}'] = measure(
                    venv, ['-A', 'cli_app', 'inspect', 'ping', '-t', '0.2'],
                    runs, base_env)
            for label, venv in venvs.items():
                data['control_probe'][label] = probe(venv, base_env)
        finally:
            worker.stop()
    finally:
        drop_vhost(vhost)
    return data


def compare(data):
    """Riepilogo: rapporto rs/ref e verifica degli obiettivi della spec."""
    lines = []
    targets = {}
    for key, ref in data['cases'].items():
        if not key.endswith('/ref'):
            continue
        name = key[:-4]
        rs = data['cases'].get(f'{name}/rs')
        if rs is None:
            continue
        ratio_time = rs['time_s'] / ref['time_s'] if ref['time_s'] else 0.0
        ratio_rss = rs['rss_kb'] / ref['rss_kb'] if ref['rss_kb'] else 0.0
        lines.append(
            f'{data["broker"]:5} {name:20} '
            f'ref {ref["time_s"] * 1000:7.1f} ms {ref["rss_kb"] / 1024:6.1f} MB  '
            f'rs {rs["time_s"] * 1000:7.1f} ms {rs["rss_kb"] / 1024:6.1f} MB  '
            f'Δrss {(ratio_rss - 1) * 100:+5.1f}%')
        if name in RSS_TARGETS:
            targets.setdefault(name, []).append(ratio_rss)
        if name in RSS_TARGETS:
            targets.setdefault(name + '/time', []).append(ratio_time)
    ref_probe = data['control_probe']['ref']
    rs_probe = data['control_probe']['rs']
    lines.append(
        f'{data["broker"]:5} control-probe p50 ref {ref_probe["p50_ms"]:.2f} ms '
        f'rs {rs_probe["p50_ms"]:.2f} ms, RSS finale ref '
        f'{ref_probe["rss_kb"] / 1024:.1f} MB rs {rs_probe["rss_kb"] / 1024:.1f} MB')
    checks = {
        'help_rss_ok': all(ratio <= 1.0 for ratio in targets.get('help', [])),
        'status_rss_minus10': all(ratio <= 0.9 for ratio in targets.get('status', [])),
        'inspect_rss_minus10': all(
            ratio <= 0.9
            for name in ('inspect-ping', 'inspect-ping-0.2')
            for ratio in targets.get(name, [])),
        'call_rss_minus10': all(ratio <= 0.9 for ratio in targets.get('call', [])),
        'startup_time_ok': all(
            ratio <= 1.0 for key, ratios in targets.items()
            if key.endswith('/time') for ratio in ratios),
    }
    lines.append('obiettivi: ' + ', '.join(
        f'{key}={"ok" if value else "NO"}' for key, value in checks.items()))
    return lines, checks


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--runs', type=int, default=10)
    parser.add_argument('--brokers', nargs='+', default=['redis', 'amqp'],
                        choices=['redis', 'amqp'])
    parser.add_argument('--out', default=str(HERE / 'results'))
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    all_checks = {}
    for name in args.brokers:
        data = bench_broker(name, args.runs)
        (out / f'cli-{name}.json').write_text(json.dumps(data, indent=1))
        lines, checks = compare(data)
        print('\n'.join(lines))
        all_checks[name] = checks
    print()
    failures = [f'{broker}:{key}' for broker, checks in all_checks.items()
                for key, value in checks.items() if not value]
    if failures:
        print('obiettivi non raggiunti: ' + ', '.join(failures))
        return 1
    print('tutti gli obiettivi raggiunti')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
