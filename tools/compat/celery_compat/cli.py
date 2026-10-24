"""celery-compat: conformità di celery-rs rispetto a Celery di riferimento.

    python -m celery_compat api        [--ref VENV] [--rs VENV]
    python -m celery_compat wire       [--ref VENV] [--rs VENV] [--broker redis amqp] [-s SCENARIO...]
    python -m celery_compat wire-worker [--ref VENV] [--rs VENV] [--broker redis amqp] [--pool solo]
    python -m celery_compat upstream   [--venv VENV] [--suite unit|integration] [-k EXPR] [paths...]

Ogni comando scrive i risultati in tools/compat/out/ ed esce con codice != 0 se il gate fallisce.
"""
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
    """Come subprocess.run, ma in un gruppo di processi proprio, terminato del tutto all'uscita.

    Se il pytest principale muore (timeout, OOM, kill) i worker xdist restano orfani e inattivi
    per sempre: alla fine si uccide l'intero gruppo, qualunque sia l'esito.
    """
    proc = subprocess.Popen(cmd, start_new_session=True, **kwargs)
    try:
        return proc.wait()
    finally:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def _exit_on_term(signum, frame):
    # SIGTERM → SystemExit, così i `finally` di run_group ripuliscono i gruppi.
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
    """Moduli in scope, enumerati dal sorgente di riferimento (non serve importarli)."""
    scope = load_scope()
    names = set()
    for f in (REF_SRC / 'celery').rglob('*.py'):
        parts = f.relative_to(REF_SRC).with_suffix('').parts
        if parts[-1] == '__init__':
            parts = parts[:-1]
        names.add('.'.join(parts))
    return sorted(n for n in names if in_scope(n, scope))


def py(venv):
    # abspath e non resolve(): il symlink bin/python va mantenuto o il venv non viene riconosciuto
    return os.path.join(os.path.abspath(venv), 'bin', 'python')


# --------------------------------------------------------------------------- api

def run_dump(venv, label, modules_file):
    out = OUT / f'api-{label}.json'
    # cwd neutro e -P: il dumper deve importare `celery` dal venv, non da cwd/cartella script.
    # Niente -I perché ignorerebbe PYTHONHASHSEED, che serve fisso: set/frozenset compaiono
    # nei default delle firme e il loro repr dipende dall'hash.
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
        report['modules'][mod] = diffs
        total += len(diffs)
    for section in ('settings', 'signals', 'cli'):
        diffs = []
        diff_tree(ref[section], rs[section], '', diffs)
        report['sections'][section] = diffs
        total += len(diffs)

    lines = [f'# celery-compat api — {total} differenze',
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
            lines.append(f'- … altre {len(diffs) - args.max_per_area}')
        lines.append('')
    (OUT / 'api-report.md').write_text('\n'.join(lines))
    (OUT / 'api-diff.json').write_text(json.dumps(report, indent=1, ensure_ascii=False))

    print(f'api: {total} differenze su {len(modules)} moduli + settings/signals/cli '
          f'→ {OUT / "api-report.md"}')
    return 1 if total else 0


# -------------------------------------------------------------------------- wire

BROKERS = {
    'redis': ('redis://127.0.0.1:6390/0', 'redis://127.0.0.1:6390/1'),
    'amqp': ('amqp://guest:guest@127.0.0.1:5673//', 'rpc://'),
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
                status = f'ok (stesso errore del riferimento: {ref_err[:60]})'
            elif ref_err:
                status = f'ERRORE NEL RIFERIMENTO: {ref_err}'
            elif rs_err:
                status = f'errore rs: {rs_err}'
            elif sorted(ref) != sorted(rs):
                status = 'diverso'
            elif ref != rs:
                status = 'ok (ordine diverso)'
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
    (OUT / 'wire-report.md').write_text(f'# celery-compat wire — {failures} scenari diversi\n\n'
                                        + '\n'.join(lines) + '\n')
    (OUT / 'wire.json').write_text(json.dumps(results, indent=1, ensure_ascii=False))
    print(f'wire: {failures} scenari diversi su {len(results)} → {OUT / "wire-report.md"}')
    return 1 if failures else 0


# ------------------------------------------------------------------ wire-worker

#: comandi Redis di lettura/polling: il loro numero dipende dai tempi, non dalla semantica
WORKER_REDIS_READS = {'BRPOP', 'BLPOP', 'LLEN', 'GET', 'MGET', 'SMEMBERS', 'SSCAN', 'SCAN', 'KEYS',
                      'EXISTS', 'TTL', 'PTTL', 'HGET', 'HGETALL', 'ZRANGE', 'ZREVRANGEBYSCORE',
                      'ZRANGEBYSCORE', 'ZCARD', 'LRANGE', 'SUBSCRIBE', 'PSUBSCRIBE', 'UNSUBSCRIBE',
                      'PUNSUBSCRIBE', 'MULTI', 'EXEC', 'WATCH', 'UNWATCH', 'TYPE'}
#: campi degli eventi che dipendono da tempi e carico
EVENT_FIELDS_RE = __import__('re').compile(
    r'(\\?"(?:clock|runtime|freq|active|processed|utcoffset|loadavg|local_received)\\?":\s*)'
    r'(\[[^\]]*\]|-?[\d.e+-]+)')
#: percorsi dei sorgenti nei traceback (anche dentro JSON annidato, con virgolette escaped)
TRACEBACK_RE = __import__('re').compile(r'File (\\*)"[^"]*?/([^/"\\]+)\\*", line \d+')


ANY_ID_RE = __import__('re').compile(r'<id\d+>')


def _split_multi(broker, rec):
    """Eventi `task.multi` → un record per evento: il raggruppamento dipende dai tempi."""
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
    """`HSET unacked <tag> [envelope, exchange, rk]`: il body dell'envelope è base64."""
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
            # gli id dei task sono fissati dagli scenari; gli uuid restanti (delivery tag, code di
            # risposta) prendono numeri diversi secondo l'interleaving client/worker
            rec = ANY_ID_RE.sub('<id>', rec)
            out.append(rec)
    return sorted(out)


def run_worker_scenario(venv, ref_venv, broker, name, pool):
    import threading
    from . import wire
    url, _ = BROKERS[broker]
    backend = 'redis://127.0.0.1:6390/1'
    env = venv_env()
    env.update(COMPAT_BROKER=url, COMPAT_BACKEND=backend, PYTHONPATH=str(HERE))
    norm = wire.Normalizer()
    with wire.capture(broker) as cap:
        worker = subprocess.Popen(
            [py(venv), '-P', '-m', 'celery', '-A', 'worker_app', 'worker', '-P', pool, '-c', '2',
             '-E', '-n', 'compat@%h', '-l', 'info'],
            cwd=OUT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            start_new_session=True)
        log, ready = [], threading.Event()

        def pump():
            for line in worker.stdout:
                log.append(line)
                if ' ready.' in line:
                    ready.set()
        threading.Thread(target=pump, daemon=True).start()
        try:
            if not ready.wait(30):
                return None, 'worker non pronto: ' + ''.join(log[-5:])[-300:]
            client = subprocess.run([py(ref_venv), '-P', '-s', str(HERE / 'worker_client.py'),
                                     '--scenario', name],
                                    cwd=OUT, env=env, capture_output=True, text=True, timeout=90)
            # gli eventi dei task sono bufferizzati: lascia che il dispatcher li scarichi
            time.sleep(1.5)
            worker.send_signal(15)
            worker.wait(30)
        finally:
            if worker.poll() is None:
                os.killpg(worker.pid, 9)
                worker.wait()
    if client.returncode != 0:
        err = client.stderr.strip().splitlines()
        return None, 'client: ' + (err[-1] if err else 'exit != 0')
    # scomposizione e decodifica prima della normalizzazione (i segnaposto non sono JSON)
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
                status = f'ERRORE NEL RIFERIMENTO: {ref_err}'
            elif rs_err:
                status = f'errore rs: {rs_err}'
            elif ref != rs:
                status = 'diverso'
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
        f'# celery-compat wire-worker — {failures} scenari diversi\n\n' + '\n'.join(lines) + '\n')
    (OUT / 'wire-worker.json').write_text(json.dumps(results, indent=1, ensure_ascii=False))
    print(f'wire-worker: {failures} scenari diversi su {len(results)} → '
          f'{OUT / "wire-worker-report.md"}')
    return 1 if failures else 0


# ---------------------------------------------------------------------- upstream

def junit_failures(xml_path):
    failed = set()
    if not xml_path.exists():
        return failed
    for case in ET.parse(xml_path).getroot().iter('testcase'):
        if case.find('failure') is not None or case.find('error') is not None:
            cls = case.get('classname', '')
            # classname = "t.unit.app.test_app.test_App" → file + classe
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
    """Copia dei test di Celery in una cartella senza il pacchetto `celery`.

    Eseguiti da `celery/`, pytest metterebbe in sys.path la cartella che contiene `t/`
    e quindi il sorgente di riferimento: il venv-rs importerebbe il Celery originale.
    """
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
    # -P: niente cwd in sys.path; il pacchetto celery deve venire dal venv
    pytest = [py(args.venv), '-P', '-m', 'pytest']
    opts = ['-q', '-p', 'no:cacheprovider', '--timeout=300', '-o', 'addopts=']
    if args.k:
        opts += ['-k', args.k]
    env = {**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'}

    # passo 1: parallelo per file
    x1 = OUT / f'upstream-{args.suite}-{label}-pass1.xml'
    run_group([*pytest, *paths, *opts, '-n', str(args.jobs), '--dist', 'loadfile',
               f'--junitxml={x1}'],
              cwd=TEST_SRC, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    failed1 = junit_failures(x1)

    # passo 2: ripete in seriale solo i falliti, un processo per file: alcuni test passano da soli
    # ma falliscono per interferenze con altri test (thread rimasti, stato globale).
    # Restano rotti solo quelli che falliscono di nuovo.
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

    # fallimenti che si verificano anche col riferimento non sono regressioni di celery-rs
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
    print(f'upstream {args.suite} [{label}]: {s.get("tests", 0)} test, {passed} ok al primo passo, '
          f'{len(failed1) - len(failed)} instabili, {len(failed)} falliti '
          f'({len(failed & known)} noti anche nel riferimento) → {out}')
    for n in sorted(regressions)[:args.show]:
        print('  FAIL', n)
    if label == 'venv-ref':
        return 0  # il riferimento definisce la baseline
    return 1 if regressions else 0


def main(argv=None):
    ap = argparse.ArgumentParser(prog='celery-compat')
    sub = ap.add_subparsers(dest='cmd', required=True)

    a = sub.add_parser('api', help='diff della superficie pubblica ref vs rs')
    a.add_argument('--ref', default=VENV_REF)
    a.add_argument('--rs', default=VENV_RS)
    a.add_argument('--max-per-area', type=int, default=40)
    a.set_defaults(fn=cmd_api)

    w = sub.add_parser('wire', help='diff del traffico sul broker generato dal client ref vs rs')
    w.add_argument('--ref', default=VENV_REF)
    w.add_argument('--rs', default=VENV_RS)
    w.add_argument('--broker', nargs='+', choices=tuple(BROKERS), default=list(BROKERS))
    w.add_argument('-s', '--scenario', nargs='+', help='filtra gli scenari per sottostringa')
    w.set_defaults(fn=cmd_wire)

    ww = sub.add_parser('wire-worker',
                        help='diff del traffico sul broker generato dal worker ref vs rs (client ref)')
    ww.add_argument('--ref', default=VENV_REF)
    ww.add_argument('--rs', default=VENV_RS)
    ww.add_argument('--broker', nargs='+', choices=tuple(BROKERS), default=list(BROKERS))
    ww.add_argument('--pool', default='solo')
    ww.add_argument('-s', '--scenario', nargs='+', help='filtra gli scenari per sottostringa')
    ww.set_defaults(fn=cmd_wire_worker)

    u = sub.add_parser('upstream', help='esegue la test suite di Celery contro un venv')
    u.add_argument('--venv', default=VENV_RS)
    u.add_argument('--suite', choices=('unit', 'integration'), default='unit')
    u.add_argument('-k')
    u.add_argument('-j', '--jobs', type=int, default=16)
    u.add_argument('--show', type=int, default=50)
    u.add_argument('paths', nargs='*', help='sottoinsieme di test (relativo a celery/)')
    u.set_defaults(fn=cmd_upstream)

    args = ap.parse_args(argv)
    sys.exit(args.fn(args))
