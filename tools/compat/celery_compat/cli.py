"""Compare celery-rs API, wire traffic, CLI and test outcomes with reference Celery.

Each subcommand writes tools/compat/out/ and returns nonzero on gate failure."""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
import tomllib
import xml.etree.ElementTree as ET
from pathlib import Path

HERE = Path(__file__).resolve().parent
COMPAT = HERE.parent
ROOT = COMPAT.parent.parent
OUT = COMPAT / 'out'
REF_SRC = ROOT / 'celery'
VENV_REF = ROOT / '.venv-ref'
VENV_RS = ROOT / '.venv-rs'


def run_group(cmd, **kwargs):
    """Run a command in its own process group and terminate the entire group on exit.

    This prevents orphaned xdist workers after timeout, OOM or kill."""
    proc = subprocess.Popen(cmd, start_new_session=True, **kwargs)
    try:
        return proc.wait()
    finally:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def _exit_on_term(signum, frame):
    # Convert SIGTERM to SystemExit so run_group finally blocks clean up process groups.
    raise SystemExit(128 + signum)


signal.signal(signal.SIGTERM, _exit_on_term)
signal.signal(signal.SIGHUP, _exit_on_term)


def load_scope():
    with open(COMPAT / 'scope.toml', 'rb') as fh:
        return tomllib.load(fh)


def in_scope(name, scope):
    if any(name == e or name.startswith(e + '.') for e in scope['exclude']):
        return False
    return any(name == i or name.startswith(i + '.') for i in scope['include'])


def reference_modules():
    """List scoped modules from reference sources without importing them."""
    scope = load_scope()
    names = set()
    for f in (REF_SRC / 'celery').rglob('*.py'):
        parts = f.relative_to(REF_SRC).with_suffix('').parts
        if parts[-1] == '__init__':
            parts = parts[:-1]
        names.add('.'.join(parts))
    return sorted(n for n in names if in_scope(n, scope))


def py(venv):
    # Keep the bin/python symlink with abspath so the interpreter recognizes its venv.
    return os.path.join(os.path.abspath(venv), 'bin', 'python')




def run_dump(venv, label, modules_file):
    out = OUT / f'api-{label}.json'
    # Use a neutral cwd and -P to import Celery from the venv.
    # Avoid -I: fixed PYTHONHASHSEED makes set/frozenset signature defaults deterministic.

    env = {k: v for k, v in os.environ.items() if not k.startswith('PYTHON')}
    env['PYTHONHASHSEED'] = '0'
    subprocess.run([py(venv), '-P', '-s', str(HERE / 'dump_api.py'), '--modules', str(modules_file),
                    '--out', str(out)], check=True, cwd=OUT, env=env)
    with open(out) as fh:
        return json.load(fh)


def diff_tree(ref, rs, path, diffs):
    if isinstance(ref, dict) and isinstance(rs, dict):
        for k in sorted(set(ref) | set(rs)):
            p = f'{path}.{k}' if path else k
            if k not in rs:
                diffs.append(('missing', p, ref[k], None))
            elif k not in ref:
                diffs.append(('extra', p, None, rs[k]))
            else:
                diff_tree(ref[k], rs[k], p, diffs)
    elif ref != rs:
        diffs.append(('changed', path, ref, rs))


# Owner-approved static introspection differences, with reasons.
# Runtime behaviour stays identical; report these separately without counting them.
ACCEPTED_API_DIFFS = {
    'names.RedisBackend.members.connection_class_ssl':
        'resolved lazily to the redis-py class, saving 7.5 MB per client/master; '
        'vars(cls) sees the descriptor',
    'names.SentinelBackend.members.connection_class_ssl':
        'like RedisBackend.connection_class_ssl; Sentinel+TLS class created lazily',
    'names.TaskPool.members.Pool':
        'resolved lazily to celery.concurrency.asynpool.AsynPool, keeping billiard/asynpool '
        'out of the Rust pool master; vars(cls) sees the descriptor',
    'names.TaskPool.members.BlockingPool':
        'resolved lazily to billiard.pool.Pool; vars(cls) sees the descriptor',
}


def accepted(path):
    return next((reason for prefix, reason in ACCEPTED_API_DIFFS.items()
                 if path == prefix or path.startswith(prefix + '.')), None)


def short(v):
    s = json.dumps(v, sort_keys=True, ensure_ascii=False)
    return s if len(s) <= 160 else s[:160] + '…'


def cmd_api(args):
    OUT.mkdir(parents=True, exist_ok=True)
    modules = reference_modules()
    modules_file = OUT / 'modules.json'
    modules_file.write_text(json.dumps(modules, indent=1))

    ref = run_dump(args.ref, 'ref', modules_file)
    rs = run_dump(args.rs, 'rs', modules_file)

    report = {'modules': {}, 'sections': {}}
    total = 0
    for mod in modules:
        r, s = ref['modules'][mod], rs['modules'][mod]
        diffs = []
        if 'import_error' in s and 'import_error' not in r:
            diffs.append(('missing', mod, 'module', s['import_error']))
        else:
            diff_tree(r, s, '', diffs)
        report['accepted'] = report.get('accepted', []) + [
            (mod, d, accepted(d[1])) for d in diffs if accepted(d[1])]
        diffs = [d for d in diffs if not accepted(d[1])]
        report['modules'][mod] = diffs
        total += len(diffs)
    for section in ('settings', 'signals', 'cli'):
        diffs = []
        diff_tree(ref[section], rs[section], '', diffs)
        report['sections'][section] = diffs
        total += len(diffs)

    lines = [f'# celery-compat api — {total} differences',
             f'ref: {ref["meta"]}', f'rs:  {rs["meta"]}', '']
    lines.append('| area | missing | extra | changed |')
    lines.append('|---|---|---|---|')
    for area, diffs in [*report['modules'].items(), *report['sections'].items()]:
        if diffs:
            c = {k: sum(1 for d in diffs if d[0] == k) for k in ('missing', 'extra', 'changed')}
            lines.append(f'| {area} | {c["missing"]} | {c["extra"]} | {c["changed"]} |')
    lines.append('')
    for area, diffs in [*report['modules'].items(), *report['sections'].items()]:
        if not diffs:
            continue
        lines.append(f'## {area}')
        for kind, p, a, b in diffs[:args.max_per_area]:
            lines.append(f'- **{kind}** `{p}`: ref={short(a)} rs={short(b)}')
        if len(diffs) > args.max_per_area:
            lines.append(f'- … more {len(diffs) - args.max_per_area}')
        lines.append('')
    if report.get('accepted'):
        lines.append('## Accepted differences (not counted)')
        for mod, (kind, p, a, b), reason in report['accepted']:
            lines.append(f'- {mod} **{kind}** `{p}`: {reason}')
        lines.append('')
    (OUT / 'api-report.md').write_text('\n'.join(lines))
    (OUT / 'api-diff.json').write_text(json.dumps(report, indent=1, ensure_ascii=False))

    print(f'api: {total} differences in {len(modules)} modules + settings/signals/cli '
          f'→ {OUT / "api-report.md"}')
    return 1 if total else 0




BROKERS = {
    'redis': (os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/0', os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/1'),
    'amqp': (os.environ.get('CELERY_RS_AMQP_URL', 'amqp://guest:guest@127.0.0.1:5673//'), 'rpc://'),
}


def venv_env():
    env = {k: v for k, v in os.environ.items() if not k.startswith('PYTHON')}
    env['PYTHONHASHSEED'] = '0'
    return env


def run_scenario(venv, broker, name):
    from . import wire
    url, backend = BROKERS[broker]
    norm = wire.Normalizer()
    with wire.capture(broker) as cap:
        proc = subprocess.run([py(venv), '-P', '-s', str(HERE / 'wire_scenarios.py'), '--broker', url,
                               '--backend', backend, '--scenario', name],
                              cwd=OUT, env=venv_env(), capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        return None, proc.stderr.strip().splitlines()[-1] if proc.stderr.strip() else 'exit != 0'
    return cap.messages(norm), None


def cmd_wire(args):
    import difflib
    from . import wire
    OUT.mkdir(parents=True, exist_ok=True)
    if 'amqp' in args.broker:
        wire.enable_rabbit_trace()
    names = subprocess.run([py(args.ref), '-P', '-s', str(HERE / 'wire_scenarios.py'), '--list'],
                           cwd=OUT, env=venv_env(), capture_output=True, text=True,
                           check=True).stdout.split()
    if args.scenario:
        names = [n for n in names if any(s in n for s in args.scenario)]

    lines, failures, results = [], 0, {}
    for broker in args.broker:
        for name in names:
            ref, ref_err = run_scenario(args.ref, broker, name)
            rs, rs_err = run_scenario(args.rs, broker, name)
            key = f'{broker}:{name}'
            if ref_err and ref_err == rs_err:
                status = f'ok (same reference error: {ref_err[:60]})'
            elif ref_err:
                status = f'REFERENCE ERROR: {ref_err}'
            elif rs_err:
                status = f'rs error: {rs_err}'
            elif sorted(ref) != sorted(rs):
                status = 'different'
            elif ref != rs:
                status = 'ok (different order)'
            else:
                status = 'ok'
            ok = status.startswith('ok')
            failures += not ok
            results[key] = {'status': status, 'ref': ref, 'rs': rs}
            print(f'{"✓" if ok else "✗"} {key:45} {status}')
            if not ok and ref is not None and rs is not None:
                lines.append(f'## {key}\n```diff')
                lines += difflib.unified_diff(sorted(ref), sorted(rs), 'ref', 'rs', lineterm='', n=0)
                lines.append('```')
    (OUT / 'wire-report.md').write_text(f'# celery-compat wire — {failures} differing scenarios\n\n'
                                        + '\n'.join(lines) + '\n')
    (OUT / 'wire.json').write_text(json.dumps(results, indent=1, ensure_ascii=False))
    print(f'wire: {failures} differing scenarios out of {len(results)} → {OUT / "wire-report.md"}')
    return 1 if failures else 0




# Redis polling counts depend on timing rather than semantics.
WORKER_REDIS_READS = {'BRPOP', 'BLPOP', 'LLEN', 'GET', 'MGET', 'SMEMBERS', 'SSCAN', 'SCAN', 'KEYS',
                      'EXISTS', 'TTL', 'PTTL', 'HGET', 'HGETALL', 'ZRANGE', 'ZREVRANGEBYSCORE',
                      'ZRANGEBYSCORE', 'ZCARD', 'LRANGE', 'SUBSCRIBE', 'PSUBSCRIBE', 'UNSUBSCRIBE',
                      'PUNSUBSCRIBE', 'MULTI', 'EXEC', 'WATCH', 'UNWATCH', 'TYPE'}
# Event fields vary with timing and load.
EVENT_FIELDS_RE = __import__('re').compile(
    r'(\\?"(?:clock|runtime|freq|active|processed|utcoffset|loadavg|local_received)\\?":\s*)'
    r'(\[[^\]]*\]|-?[\d.e+-]+)')
# Source paths in tracebacks also occur inside escaped nested JSON.
TRACEBACK_RE = __import__('re').compile(r'File (\\*)"[^"]*?/([^/"\\]+)\\*", line \d+')


ANY_ID_RE = __import__('re').compile(r'<id\d+>')


def _split_multi(broker, rec):
    """Flatten task.multi records because event batching depends on timing."""
    if 'task.multi' not in rec:
        return [rec]
    try:
        if broker == 'redis':
            head, _, env = rec.partition(' {')
            env = json.loads('{' + env)
            events = json.loads(env['body'])
            mk = lambda ev: f'{head.replace("task.multi", ev.get("type", "?"))} ' + json.dumps(
                {**env, 'body': json.dumps(ev, ensure_ascii=False)}, ensure_ascii=False)
        else:
            env = json.loads(rec)
            events = json.loads(env['body'])
            mk = lambda ev: json.dumps({**env, 'routing_keys': [ev.get('type', '?')],
                                        'body': json.dumps(ev, ensure_ascii=False)},
                                       ensure_ascii=False)
    except (ValueError, KeyError, TypeError):
        return [rec]
    if not isinstance(events, list):
        return [rec]
    return [mk(ev) for ev in events if isinstance(ev, dict)]


def _decode_unacked(rec):
    """Decode the base64 envelope body in HSET unacked records."""
    from . import wire
    head, sep, payload = rec.partition(' [')
    if not sep or ' HSET unacked' not in head:
        return rec
    try:
        item = json.loads('[' + payload)
        env = wire.decode_kombu_envelope(json.dumps(item[0]))
    except (ValueError, IndexError, TypeError):
        return rec
    if env is None:
        return rec
    return f'{head} ' + json.dumps([json.loads(env), *item[1:]], ensure_ascii=False)


def _worker_semantic(broker, records, norm):
    out = []
    for raw in records:
        for rec in _split_multi(broker, _decode_unacked(raw)):
            rec = norm(rec)
            if 'heartbeat' in rec and ('worker.heartbeat' in rec or 'worker-heartbeat' in rec):
                continue
            if broker == 'redis':
                cmd = rec.split(' ', 2)[1] if rec.count(' ') >= 1 else ''
                if cmd in WORKER_REDIS_READS:
                    continue
            rec = EVENT_FIELDS_RE.sub(r'\1<v>', rec)
            rec = TRACEBACK_RE.sub(r'File \1"<path>/\2\1", line <n>', rec)
            # Scenarios fix task IDs; other UUID numbering depends on client/worker interleaving.

            rec = ANY_ID_RE.sub('<id>', rec)
            out.append(rec)
    return sorted(out)


# These scenarios start a second worker between pre and main.
TWO_WORKER_SCENARIOS = {'two_workers'}


def _spawn_worker(venv, env, pool, nodename, queues=None):
    import threading
    argv = [py(venv), '-P', '-m', 'celery', '-A', 'worker_app', 'worker', '-P', pool, '-c', '2',
            '-E', '-n', nodename, '-l', 'info']
    if queues:
        argv += ['-Q', queues]
    worker = subprocess.Popen(argv, cwd=OUT, env=env, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True, start_new_session=True)
    log, ready = [], threading.Event()

    def pump():
        for line in worker.stdout:
            log.append(line)
            if ' ready.' in line:
                ready.set()
    threading.Thread(target=pump, daemon=True).start()
    return worker, log, ready


def _stop_workers(workers):
    for worker in workers:
        if worker.poll() is None:
            worker.send_signal(15)
    for worker in workers:
        try:
            worker.wait(30)
        except subprocess.TimeoutExpired:
            pass
        if worker.poll() is None:
            os.killpg(worker.pid, 9)
            worker.wait()


def run_worker_scenario(venv, ref_venv, broker, name, pool):
    from . import wire
    url, _ = BROKERS[broker]
    backend = os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/1'
    env = venv_env()
    env.update(COMPAT_BROKER=url, COMPAT_BACKEND=backend, PYTHONPATH=str(HERE))
    norm = wire.Normalizer()
    client_argv = [py(ref_venv), '-P', '-s', str(HERE / 'worker_client.py'), '--scenario', name]
    workers = []
    with wire.capture(broker) as cap:
        worker, log, ready = _spawn_worker(venv, env, pool, 'compat@%h')
        workers.append(worker)
        try:
            if not ready.wait(30):
                return None, 'worker not ready: ' + ''.join(log[-5:])[-300:]
            if name in TWO_WORKER_SCENARIOS:
                pre = subprocess.run([*client_argv, '--phase', 'pre'], cwd=OUT, env=env,
                                     capture_output=True, text=True, timeout=90)
                if pre.returncode != 0:
                    err = pre.stderr.strip().splitlines()
                    return None, 'client pre: ' + (err[-1] if err else 'exit != 0')
                second, log2, ready2 = _spawn_worker(venv, env, pool, 'compat2@%h',
                                                     queues='celery,compat2')
                workers.append(second)
                if not ready2.wait(30):
                    return None, 'second worker not ready: ' + ''.join(log2[-5:])[-300:]
            client = subprocess.run(client_argv, cwd=OUT, env=env, capture_output=True,
                                    text=True, timeout=90)
            # Allow the dispatcher to flush buffered task events.
            time.sleep(1.5)
        finally:
            _stop_workers(workers)
    if client.returncode != 0:
        err = client.stderr.strip().splitlines()
        return None, 'client: ' + (err[-1] if err else 'exit != 0')
    # Decode before normalizing: placeholders are not JSON.
    return _worker_semantic(broker, cap.messages(lambda text: text), norm), None


def cmd_wire_worker(args):
    import difflib
    from . import wire
    OUT.mkdir(parents=True, exist_ok=True)
    if 'amqp' in args.broker:
        wire.enable_rabbit_trace()
    env = venv_env()
    env.update(COMPAT_BROKER='memory://', COMPAT_BACKEND='cache+memory://', PYTHONPATH=str(HERE))
    names = subprocess.run([py(args.ref), '-P', '-s', str(HERE / 'worker_client.py'), '--list'],
                           cwd=OUT, env=env, capture_output=True, text=True,
                           check=True).stdout.split()
    if args.scenario:
        names = [n for n in names if any(s in n for s in args.scenario)]
    lines, failures, results = [], 0, {}
    for broker in args.broker:
        for name in names:
            ref, ref_err = run_worker_scenario(args.ref, args.ref, broker, name, args.pool)
            rs, rs_err = run_worker_scenario(args.rs, args.ref, broker, name, args.pool)
            key = f'{broker}:{args.pool}:{name}'
            if ref_err:
                status = f'REFERENCE ERROR: {ref_err}'
            elif rs_err:
                status = f'rs error: {rs_err}'
            elif ref != rs:
                status = 'different'
            else:
                status = 'ok'
            ok = status == 'ok'
            failures += not ok
            results[key] = {'status': status, 'ref': ref, 'rs': rs}
            print(f'{"✓" if ok else "✗"} {key:45} {status[:120]}')
            if not ok and ref is not None and rs is not None:
                lines.append(f'## {key}\n```diff')
                lines += difflib.unified_diff(ref, rs, 'ref', 'rs', lineterm='', n=0)
                lines.append('```')
    (OUT / 'wire-worker-report.md').write_text(
        f'# celery-compat wire-worker — {failures} differing scenarios\n\n' + '\n'.join(lines) + '\n')
    (OUT / 'wire-worker.json').write_text(json.dumps(results, indent=1, ensure_ascii=False))
    print(f'wire-worker: {failures} differing scenarios out of {len(results)} → '
          f'{OUT / "wire-worker-report.md"}')
    return 1 if failures else 0




# Entries at 300–500 ms fire 5–10 times during each beat run.
BEAT_SECONDS = 3.5


def run_beat(venv, broker):
    import shutil
    import threading
    from . import wire
    url, _ = BROKERS[broker]
    env = venv_env()
    env.update(COMPAT_BROKER=url, PYTHONPATH=str(HERE))
    workdir = OUT / 'wire-beat'
    shutil.rmtree(workdir, ignore_errors=True)
    workdir.mkdir(parents=True)
    with wire.capture(broker) as cap:
        beat = subprocess.Popen(
            [py(venv), '-P', '-m', 'celery', '-A', 'beat_app', 'beat', '-s',
             str(workdir / 'celerybeat-schedule'), '-l', 'debug'],
            cwd=workdir, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            start_new_session=True)
        log, started = [], threading.Event()

        def pump():
            for line in beat.stdout:
                log.append(line)
                if 'beat: Starting...' in line:
                    started.set()
        threading.Thread(target=pump, daemon=True).start()
        try:
            if not started.wait(30):
                return None, 'beat did not start: ' + ''.join(log[-5:])[-300:]
            time.sleep(BEAT_SECONDS)
            beat.send_signal(15)
            beat.wait(30)
        finally:
            if beat.poll() is None:
                os.killpg(beat.pid, 9)
                beat.wait()
    errors = [line.strip() for line in log if 'Traceback' in line or ' ERROR' in line]
    if errors:
        return None, 'errors in beat log: ' + errors[0][:200]
    if not (workdir / 'celerybeat-schedule').exists() and not list(workdir.glob('celerybeat-schedule*')):
        return None, 'schedule file not written'
    return _worker_semantic(broker, cap.messages(lambda text: text), wire.Normalizer()), None


def _beat_counts(records):
    counts = {}
    for rec in records:
        counts[rec] = counts.get(rec, 0) + 1
    return counts


def cmd_wire_beat(args):
    """Compare beat message shapes, allowing timing-dependent send counts."""
    import difflib
    from . import wire
    OUT.mkdir(parents=True, exist_ok=True)
    if 'amqp' in args.broker:
        wire.enable_rabbit_trace()
    lines, failures, results = [], 0, {}
    for broker in args.broker:
        ref, ref_err = run_beat(args.ref, broker)
        rs, rs_err = run_beat(args.rs, broker)
        problems = []
        if ref_err:
            status = f'REFERENCE ERROR: {ref_err}'
        elif rs_err:
            status = f'rs error: {rs_err}'
        else:
            rc, sc = _beat_counts(ref), _beat_counts(rs)
            if set(rc) != set(sc):
                problems.append('different messages')
            for rec in set(rc) & set(sc):
                # Allow timing differences within the same order of magnitude of sends per entry.
                if not (rc[rec] * 0.6 - 1 <= sc[rec] <= rc[rec] * 1.4 + 1):
                    problems.append(f'invii {rc[rec]} vs {sc[rec]}: {rec[:100]}')
            status = '; '.join(problems) if problems else 'ok'
        ok = status == 'ok'
        failures += not ok
        results[broker] = {'status': status, 'ref': ref, 'rs': rs}
        print(f'{"✓" if ok else "✗"} {broker:45} {status[:160]}')
        if not ok and ref is not None and rs is not None:
            lines.append(f'## {broker}\n{status}\n```diff')
            lines += difflib.unified_diff(sorted(set(ref)), sorted(set(rs)), 'ref', 'rs',
                                          lineterm='', n=0)
            lines.append('```')
    (OUT / 'wire-beat-report.md').write_text(
        f'# celery-compat wire-beat — {failures} differing brokers\n\n' + '\n'.join(lines) + '\n')
    (OUT / 'wire-beat.json').write_text(json.dumps(results, indent=1, ensure_ascii=False))
    print(f'wire-beat: {failures} differing brokers out of {len(results)} → {OUT / "wire-beat-report.md"}')
    return 1 if failures else 0




# Dedicated workerless brokers ensure status and inspect receive no replies.
EMPTY_BROKERS = {'redis': os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/3', 'amqp': os.environ.get('CELERY_RS_AMQP_URL', 'amqp://guest:guest@127.0.0.1:5673//')}
NUMBER_RE = __import__('re').compile(r'(?<![\w<])-?\d+(?:\.\d+)?(?![\w>])')


def _start_ref_worker(ref_venv, env):
    import threading
    worker = subprocess.Popen(
        [py(ref_venv), '-P', '-m', 'celery', '-A', 'worker_app', 'worker', '-P', 'prefork', '-c',
         '2', '-n', 'compat@%h', '-l', 'info', '--without-mingle', '--without-gossip'],
        cwd=OUT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        start_new_session=True)
    log, ready = [], threading.Event()

    def pump():
        for line in worker.stdout:
            log.append(line)
            if ' ready.' in line:
                ready.set()
    threading.Thread(target=pump, daemon=True).start()
    if not ready.wait(30):
        os.killpg(worker.pid, 9)
        raise RuntimeError('reference worker not ready: ' + ''.join(log[-5:])[-300:])
    return worker


def _stop_worker(worker):
    if worker is None:
        return
    worker.send_signal(15)
    try:
        worker.wait(30)
    except subprocess.TimeoutExpired:
        os.killpg(worker.pid, 9)
        worker.wait()


def _cli_normalize(text, venv, tmp, numbers):
    from . import wire
    import socket
    text = text.replace(str(Path(venv).resolve()), '<venv>').replace(str(venv), '<venv>')
    text = text.replace(str(tmp), '<tmp>').replace(str(HERE), '<compat>')
    # pip writes python3 in the celery shebang; maturin develop writes python.
    text = text.replace('<venv>/bin/python3', '<venv>/bin/python')
    text = TRACEBACK_RE.sub(r'File \1"<path>/\2\1", line <n>', text)
    text = wire.Normalizer()(text)
    text = ANY_ID_RE.sub('<id>', text)
    text = text.replace(socket.gethostname(), '<host>')
    if numbers:
        text = NUMBER_RE.sub('<n>', text)
    return text


def run_cli_cases(venv, ref_venv, broker, cases):
    import re
    import shutil
    import socket
    url, _ = BROKERS[broker]
    # Cross-process celery result needs a persistent backend, excluding rpc://.
    backend = os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/1'
    tmp = OUT / 'cli-tmp'
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    env = venv_env()
    env.update(COMPAT_BROKER=url, COMPAT_BACKEND=backend, PYTHONPATH=str(HERE), COLUMNS='100',
               TERM='dumb')
    subst = {'broker': url, 'backend': backend, 'tmp': str(tmp), 'host': socket.gethostname(),
             'empty_broker': EMPTY_BROKERS[broker]}
    out, last_id, worker = {}, '', None
    venv = Path(venv).resolve()
    celery = str(venv / 'bin' / 'celery')
    # Leftover messages would change the worker inspect stats.
    subprocess.run([py(ref_venv), '-P', '-m', 'celery', '-A', 'worker_app', 'purge', '-f'],
                   cwd=tmp, env=env, capture_output=True, timeout=60)
    try:
        for case in cases:
            if case.get('worker') and worker is None:
                worker = _start_ref_worker(ref_venv, env)
            if case.get('wait'):
                time.sleep(case['wait'])
            values = {**subst, 'last_id': last_id}
            argv = []
            for a in case['argv']:
                for k, v in values.items():
                    a = a.replace('{' + k + '}', v)
                argv.append(a)
            t0 = time.monotonic()
            try:
                proc = subprocess.run([celery, *argv], cwd=tmp, env=env, input=case.get('stdin', ''),
                                      capture_output=True, text=True, timeout=30)
                rc, stdout, stderr = proc.returncode, proc.stdout, proc.stderr
            except subprocess.TimeoutExpired as exc:
                rc, stdout, stderr = 'timeout', exc.stdout or '', exc.stderr or ''
            elapsed = time.monotonic() - t0
            ids = re.findall(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', stdout)
            if ids:
                last_id = ids[-1]
            numbers = case.get('numbers', False)
            out[case['name']] = {
                'rc': rc,
                'stdout': _cli_normalize(stdout, venv, tmp, numbers),
                'stderr': _cli_normalize(stderr, venv, tmp, numbers),
                'seconds': round(elapsed, 3),
            }
    finally:
        _stop_worker(worker)
    return out


def cmd_cli(args):
    """Compare stdout, stderr and exit codes for identical ref and rs CLI commands."""
    import difflib
    from . import cli_cases
    OUT.mkdir(parents=True, exist_ok=True)
    cases = cli_cases.CASES
    if args.scenario:
        cases = [c for c in cases if any(s in c['name'] for s in args.scenario)
                 or c['name'] in ('call', 'call-fail', 'call-kwargs-countdown')]
    lines, failures, results, timing = [], 0, {}, {'ref': 0.0, 'rs': 0.0}
    for broker in args.broker:
        cs = [c for c in cases if c.get('only', broker) == broker]
        ref = run_cli_cases(args.ref, args.ref, broker, cs)
        rs = run_cli_cases(args.rs, args.ref, broker, cs)
        for case in cs:
            name = case['name']
            if args.scenario and not any(s in name for s in args.scenario):
                continue
            a, b = ref[name], rs[name]
            timing['ref'] += a['seconds']
            timing['rs'] += b['seconds']
            diff = [k for k in ('rc', 'stdout', 'stderr') if a[k] != b[k]]
            ok = not diff
            failures += not ok
            key = f'{broker}:{name}'
            status = 'ok' if ok else 'different: ' + ', '.join(diff)
            results[key] = {'status': status, 'ref': a, 'rs': b}
            print(f'{"✓" if ok else "✗"} {key:45} {status}  (rc {a["rc"]})')
            if not ok:
                lines.append(f'## {key}\n```diff')
                for k in diff:
                    lines += difflib.unified_diff(str(a[k]).splitlines(), str(b[k]).splitlines(),
                                                  f'ref {k}', f'rs {k}', lineterm='', n=1)
                lines.append('```')
    (OUT / 'cli-report.md').write_text(
        f'# celery-compat cli — {failures} differing commands\n\n' + '\n'.join(lines) + '\n')
    (OUT / 'cli.json').write_text(json.dumps(results, indent=1, ensure_ascii=False))
    print(f'cli: {failures} differences in {len(results)} commands '
          f'(total time ref {timing["ref"]:.1f}s, rs {timing["rs"]:.1f}s) → {OUT / "cli-report.md"}')
    return 1 if failures else 0




def junit_failures(xml_path):
    failed = set()
    if not xml_path.exists():
        return failed
    for case in ET.parse(xml_path).getroot().iter('testcase'):
        if case.find('failure') is not None or case.find('error') is not None:
            cls = case.get('classname', '')

            parts = cls.split('.')
            for i in range(len(parts), 0, -1):
                f = Path(TEST_SRC, *parts[:i]).with_suffix('.py')
                if f.exists():
                    node = '/'.join(parts[:i]) + '.py'
                    rest = parts[i:] + [case.get('name')]
                    failed.add('::'.join([node, *rest]))
                    break
    return failed


def summary(xml_path):
    if not xml_path.exists():
        return {}
    root = ET.parse(xml_path).getroot()
    suite = root if root.tag == 'testsuite' else root.find('testsuite')
    return {k: int(suite.get(k, 0)) for k in ('tests', 'failures', 'errors', 'skipped')}


TEST_SRC = OUT / 'upstream-src'


def prepare_test_tree():
    """Copy Celery tests outside the source package.

    Otherwise pytest adds the reference source to sys.path and the Rust venv
    imports reference Celery."""
    import shutil
    shutil.rmtree(TEST_SRC, ignore_errors=True)
    shutil.copytree(REF_SRC / 't', TEST_SRC / 't', ignore=shutil.ignore_patterns('__pycache__'))
    for name in ('pyproject.toml', 'setup.cfg'):
        if (REF_SRC / name).exists():
            shutil.copy(REF_SRC / name, TEST_SRC / name)


def cmd_upstream(args):
    OUT.mkdir(parents=True, exist_ok=True)
    prepare_test_tree()
    label = Path(args.venv).name.lstrip('.')
    paths = args.paths or [f't/{args.suite}']
    # Use -P to keep cwd off sys.path and import Celery from the venv.
    pytest = [py(args.venv), '-P', '-m', 'pytest']
    opts = ['-q', '-p', 'no:cacheprovider', '--timeout=300', '-o', 'addopts=']
    if args.k:
        opts += ['-k', args.k]
    env = {**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'}


    x1 = OUT / f'upstream-{args.suite}-{label}-pass1.xml'
    run_group([*pytest, *paths, *opts, '-n', str(args.jobs), '--dist', 'loadfile',
               f'--junitxml={x1}'],
              cwd=TEST_SRC, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    failed1 = junit_failures(x1)

    # Retry failed files in fresh serial processes to isolate leftover threads and global state.

    # Only repeated failures remain broken.
    failed = set()
    by_file = {}
    for node in failed1:
        by_file.setdefault(node.split('::', 1)[0], []).append(node)
    for i, (path, nodes) in enumerate(sorted(by_file.items())):
        x2 = OUT / f'upstream-{args.suite}-{label}-pass2-{i}.xml'
        run_group([*pytest, *sorted(nodes), *opts, f'--junitxml={x2}'],
                  cwd=TEST_SRC, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        failed |= junit_failures(x2)
        x2.unlink(missing_ok=True)

    # Failures shared with the reference are not celery-rs regressions.
    known = set()
    ref_json = OUT / f'upstream-{args.suite}-venv-ref.json'
    if label != 'venv-ref' and ref_json.exists():
        known = set(json.loads(ref_json.read_text())['failed'])
    regressions = failed - known

    s = summary(x1)
    result = {'venv': str(args.venv), 'paths': paths, 'pass1': s,
              'flaky': sorted(failed1 - failed), 'failed': sorted(failed),
              'regressions': sorted(regressions)}
    out = OUT / f'upstream-{args.suite}-{label}.json'
    out.write_text(json.dumps(result, indent=1))
    passed = s.get('tests', 0) - s.get('skipped', 0) - len(failed1)
    print(f'upstream {args.suite} [{label}]: {s.get("tests", 0)} tests, {passed} passed initially, '
          f'{len(failed1) - len(failed)} flaky, {len(failed)} failed '
          f'({len(failed & known)} known reference failures) → {out}')
    for n in sorted(regressions)[:args.show]:
        print('  FAIL', n)
    if label == 'venv-ref':
        return 0  # The reference defines the baseline.
    return 1 if regressions else 0


def main(argv=None):
    ap = argparse.ArgumentParser(prog='celery-compat')
    sub = ap.add_subparsers(dest='cmd', required=True)

    a = sub.add_parser('api', help='compare ref and rs public API')
    a.add_argument('--ref', default=VENV_REF)
    a.add_argument('--rs', default=VENV_RS)
    a.add_argument('--max-per-area', type=int, default=40)
    a.set_defaults(fn=cmd_api)

    w = sub.add_parser('wire', help='compare ref and rs client broker traffic')
    w.add_argument('--ref', default=VENV_REF)
    w.add_argument('--rs', default=VENV_RS)
    w.add_argument('--broker', nargs='+', choices=tuple(BROKERS), default=list(BROKERS))
    w.add_argument('-s', '--scenario', nargs='+', help='filter scenarios by substring')
    w.set_defaults(fn=cmd_wire)

    ww = sub.add_parser('wire-worker',
                        help='compare ref and rs worker traffic (ref client)')
    ww.add_argument('--ref', default=VENV_REF)
    ww.add_argument('--rs', default=VENV_RS)
    ww.add_argument('--broker', nargs='+', choices=tuple(BROKERS), default=list(BROKERS))
    ww.add_argument('--pool', default='solo')
    ww.add_argument('-s', '--scenario', nargs='+', help='filter scenarios by substring')
    ww.set_defaults(fn=cmd_wire_worker)

    wb = sub.add_parser('wire-beat',
                        help='compare ref and rs beat messages')
    wb.add_argument('--ref', default=VENV_REF)
    wb.add_argument('--rs', default=VENV_RS)
    wb.add_argument('--broker', nargs='+', choices=tuple(BROKERS), default=list(BROKERS))
    wb.set_defaults(fn=cmd_wire_beat)

    c = sub.add_parser('cli', help='compare ref and rs CLI output and exit codes')
    c.add_argument('--ref', default=VENV_REF)
    c.add_argument('--rs', default=VENV_RS)
    c.add_argument('--broker', nargs='+', choices=tuple(BROKERS), default=list(BROKERS))
    c.add_argument('-s', '--scenario', nargs='+', help='filter cases by substring')
    c.set_defaults(fn=cmd_cli)

    u = sub.add_parser('upstream', help='run the Celery test suite in a venv')
    u.add_argument('--venv', default=VENV_RS)
    u.add_argument('--suite', choices=('unit', 'integration'), default='unit')
    u.add_argument('-k')
    u.add_argument('-j', '--jobs', type=int, default=16)
    u.add_argument('--show', type=int, default=50)
    u.add_argument('paths', nargs='*', help='test subset (relative to celery/)')
    u.set_defaults(fn=cmd_upstream)

    from .integration import register
    register(sub, VENV_REF, VENV_RS)

    args = ap.parse_args(argv)
    sys.exit(args.fn(args))
