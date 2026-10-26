"""Confronto della suite di integrazione senza alterare i test originali."""
import json
import os
from pathlib import Path
import shutil
import subprocess
from urllib.parse import urlsplit
import xml.etree.ElementTree as ET


def outcomes(path, source):
    result = {}
    for case in ET.parse(path).iter('testcase'):
        parts = list(filter(None, case.get('classname', '').split('.')))
        node = None
        for i in range(len(parts), 0, -1):
            file = Path(*parts[:i]).with_suffix('.py')
            if (source / file).exists():
                node = '::'.join([file.as_posix(), *parts[i:], case.get('name', '')])
                break
        if node is None:
            node = case.get('name', '').replace('.', '/') + '.py'
        status, detail = 'passed', ''
        for tag, value in [('skipped', 'skipped'), ('failure', 'failed'), ('error', 'error')]:
            element = case.find(tag)
            if element is not None:
                status = value
                detail = '\n'.join(filter(None, [element.get('message', ''), element.text]))
        # Un errore di teardown non deve sparire dietro un risultato passato.
        if result.get(node, {}).get('status') in ('error', 'failed') and status == 'passed':
            continue
        result[node] = {'status': status, 'detail': detail}
    return result


def regressions(reference, candidate):
    return sorted(node for node, item in candidate.items()
                  if item['status'] in ('failed', 'error') and
                  reference.get(node, {}).get('status') == 'passed')


def cmd_integration(args):
    from .cli import OUT, ROOT, REF_SRC, py, run_group
    if os.environ.get('CELERY_RS_BROKER_LOCKED') != '1':
        return subprocess.call([str(ROOT / 'tools/dev/with-brokers'), str(ROOT / 'compat'),
                                'integration', *args.forward])
    OUT.mkdir(parents=True, exist_ok=True)
    source = OUT / 'integration-src'
    shutil.rmtree(source, ignore_errors=True)
    shutil.copytree(REF_SRC / 't', source / 't', ignore=shutil.ignore_patterns('__pycache__'))
    for name in ('pyproject.toml', 'setup.cfg'):
        if (REF_SRC / name).exists():
            shutil.copy(REF_SRC / name, source / name)
    redis = os.environ['CELERY_RS_REDIS_URL']
    address = urlsplit(redis)
    report = {}
    infrastructure = []
    for broker in args.broker:
        env = {**os.environ, 'TEST_BROKER': redis + '/0' if broker == 'redis' else
               os.environ['CELERY_RS_AMQP_URL'].replace('amqp://', 'pyamqp://', 1),
               'TEST_BACKEND': redis + '/1', 'REDIS_HOST': address.hostname,
               'REDIS_PORT': str(address.port), 'PYTHONDONTWRITEBYTECODE': '1'}
        runs = {}

        def clean_brokers():
            # Il lock rende esclusivi anche i database usati dai task di supporto.
            script = ("import os, redis; from celery import Celery; "
                      "url = os.environ['CELERY_RS_REDIS_URL']; "
                      "redis.Redis.from_url(url + '/0').flushdb(); "
                      "redis.Redis.from_url(url + '/1').flushdb(); "
                      "Celery(broker=os.environ['TEST_BROKER']).control.purge()")
            subprocess.run([py(args.ref), '-I', '-c', script], cwd=source,
                           env=env, check=True, capture_output=True)

        for label, venv in [('ref', args.ref), ('rs', args.rs)]:
            clean_brokers()
            prefix = OUT / f'integration-{broker}-{label}'
            # niente rerun interni (`@flaky` della suite: 5 tentativi × timeout per test): le
            # riprove dei soli falliti le fa questo runner, uguali per ref e rs
            command = [py(venv), '-P', '-m', 'pytest', '-q', '-p', 'no:cacheprovider',
                       '-p', 'no:rerunfailures', '--timeout=120',
                       '--continue-on-collection-errors', '-o', 'addopts=']
            if args.k:
                command += ['-k', args.k]
            xml = prefix.with_suffix('.xml')
            xml.unlink(missing_ok=True)
            with prefix.with_suffix('.log').open('w') as log:
                code = run_group([*command, *(args.paths or ['t/integration']),
                                  f'--junitxml={xml}'], cwd=source, env=env,
                                 stdout=log, stderr=subprocess.STDOUT)
            if not xml.exists() or code not in (0, 1):
                infrastructure.append(f'{broker}/{label}: pytest exit {code}')
            cases = outcomes(xml, source) if xml.exists() else {}
            initial = {node for node, item in cases.items() if item['status'] in ('failed', 'error')}
            retries = {}
            for node in sorted(initial):
                retries.setdefault(node.split('::')[0], []).append(node)
            for index, nodes in enumerate(retries.values()):
                clean_brokers()
                retry = OUT / f'integration-{broker}-{label}-retry-{index}.xml'
                retry.unlink(missing_ok=True)
                with retry.with_suffix('.log').open('w') as log:
                    code = run_group([*command, *nodes, f'--junitxml={retry}'],
                                     cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT)
                if code not in (0, 1) or not retry.exists():
                    infrastructure.append(f'{broker}/{label}: retry exit {code}')
                else:
                    cases.update(outcomes(retry, source))
            runs[label] = {'tests': cases, 'flaky': sorted(node for node in initial
                          if cases[node]['status'] == 'passed'),
                          'counts': {status: sum(item['status'] == status for item in cases.values())
                                     for status in ('passed', 'failed', 'skipped', 'error')}}
        differences = regressions(runs['ref']['tests'], runs['rs']['tests'])
        missing = sorted(set(runs['ref']['tests']) - set(runs['rs']['tests']))
        if missing:
            infrastructure.append(f'{broker}: {len(missing)} test assenti in rs')
        report[broker] = {**runs, 'regressions': differences, 'missing': missing}
        print(f'integration {broker}: ref {runs["ref"]["counts"]}; '
              f'rs {runs["rs"]["counts"]}; {len(differences)} regressioni', flush=True)
    (OUT / 'integration-report.json').write_text(json.dumps(
        {'configurations': report, 'infrastructure': infrastructure}, indent=2))
    lines = ['# Integration', '', '| Broker | Venv | Passed | Failed | Skipped | Error | Regressions |',
             '|---|---|---:|---:|---:|---:|---:|']
    for broker, run in report.items():
        for label in ('ref', 'rs'):
            counts = run[label]['counts']
            lines.append(f'| {broker} | {label} | ' + ' | '.join(str(counts[s]) for s in
                         ('passed', 'failed', 'skipped', 'error')) + f' | {len(run["regressions"])} |')
    for broker, run in report.items():
        for label in ('ref', 'rs'):
            lines += ['', f'## {broker}/{label}: falliti e servizi assenti', '']
            for node, item in run[label]['tests'].items():
                if item['status'] != 'passed':
                    detail = ' '.join(item['detail'].split())
                    lines.append(f'- `{node}`: {item["status"]}; {detail}')
        lines += ['', f'## {broker}: regressioni', *[f'- `{n}`' for n in run['regressions']], '']
    lines += ['## Errori di esecuzione', *infrastructure]
    (OUT / 'integration-report.md').write_text('\n'.join(lines) + '\n')
    total = sum(len(run['regressions']) for run in report.values())
    print(f'integration: {total} regressioni; {len(infrastructure)} errori di esecuzione → '
          f'{OUT / "integration-report.md"}', flush=True)
    return int(bool(total or infrastructure))


def register(sub, ref, rs):
    parser = sub.add_parser('integration', help='confronta tutta t/integration su ref e rs')
    parser.add_argument('--ref', default=ref)
    parser.add_argument('--rs', default=rs)
    parser.add_argument('--broker', nargs='+', choices=('redis', 'amqp'), default=['redis', 'amqp'])
    parser.add_argument('-k')
    parser.add_argument('paths', nargs='*')

    def run(args):
        args.forward = ['--ref', str(args.ref), '--rs', str(args.rs), '--broker', *args.broker]
        if args.k:
            args.forward += ['-k', args.k]
        if args.paths:
            args.forward += ['--', *args.paths]
        return cmd_integration(args)

    parser.set_defaults(fn=run)
