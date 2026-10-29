"""Tabulate measurements saved by run_08c, probe_08c and memory_08c."""
import json
from pathlib import Path
import statistics

DIRECTORY = Path(__file__).resolve().parent / 'results/08c'


def load(name):
    return json.loads((DIRECTORY / name).read_text())


def matrix(phase, impl, broker, concurrency, task):
    records = [json.loads(path.read_text()) for path in DIRECTORY.glob(
        f'{phase}-{impl}-{broker}-c{concurrency}-{task}-*.json')]
    records = [record for record in records if record['task'] == task]
    if phase == 'before' and broker == 'redis' and concurrency == 10:
        # The second run has no other Python probes active on the machine.
        records = [max(records, key=lambda record: int(record['label'].rsplit('-', 1)[1]))]
    return {key: statistics.median(record[key] for record in records)
            for key in ('worker_tasks_per_s', 'pss_idle_kb', 'pss_peak_kb', 'pss_after_kb')}


def main():
    lines = ['# 08c measurements', '',
             '10,000 tasks per run; PSS in MiB; full-queue capacity measured from worker resume.', '',
             '## Capacity', '', '| Broker | c | Task | ref task/s | rs before | rs after |',
             '|---|---:|---|---:|---:|---:|']
    for broker in ('redis', 'amqp'):
        for concurrency in (4, 10):
            for task in ('pure', 'bench', 'bench-toplevel'):
                values = [matrix(phase, impl, broker, concurrency, task)['worker_tasks_per_s']
                          for phase, impl in [('after', 'ref'), ('before', 'rs'), ('after', 'rs')]]
                lines.append(f'| {broker} | {concurrency} | {task} | ' +
                             ' | '.join(f'{value:.0f}' for value in values) + ' |')
    lines += ['', '## PSS c10', '',
              '| Broker | Task | ref idle/peak | rs before idle/peak | rs after idle/peak | Peak savings |',
              '|---|---|---:|---:|---:|---:|']
    for broker in ('redis', 'amqp'):
        for task in ('pure', 'bench', 'bench-toplevel'):
            records = [matrix(phase, impl, broker, 10, task)
                       for phase, impl in [('after', 'ref'), ('before', 'rs'), ('after', 'rs')]]
            values = [f"{d['pss_idle_kb']/1024:.1f}/{d['pss_peak_kb']/1024:.1f}" for d in records]
            saved = records[0]['pss_peak_kb'] - records[2]['pss_peak_kb']
            lines.append(f'| {broker} | {task} | ' + ' | '.join(values) +
                         f" | {saved/1024:.1f} ({100*saved/records[0]['pss_peak_kb']:.1f}%) |")
    lines += ['', '## AMQP prefork c10 latency', '',
              '| Worker | Metric | ref p50/p99 ms | rs before | rs after |',
              '|---|---|---:|---:|---:|']
    for worker in ('ref', 'rs'):
        before = load(f'isolated-before-latency-{worker}.json')
        after = load(f'isolated-after-latency-{worker}.json')
        for metric in ('rpc_ms', 'ping_ms'):
            values = [d[client][metric] for d, client in [(after, 'ref'), (before, 'rs'), (after, 'rs')]]
            lines.append(f'| {worker} | {metric} | ' +
                         ' | '.join(f"{d['p50']:.3f}/{d['p99']:.3f}" for d in values) + ' |')
    lines += ['', '## In-process ping to AMQP solo workers', '',
              '| Worker | ref p50/p99 ms | rs before | rs after |', '|---|---:|---:|---:|']
    for worker in ('ref', 'rs'):
        before = load(f'isolated-before-solo-latency-{worker}.json')
        after = load(f'isolated-after-solo-latency-{worker}.json')
        values = [d[client]['ping_ms'] for d, client in [(after, 'ref'), (before, 'rs'), (after, 'rs')]]
        lines.append(f'| {worker} | ' +
                     ' | '.join(f"{d['p50']:.3f}/{d['p99']:.3f}" for d in values) + ' |')
    lines += ['', '## Beat PSS (3 entries)', '', '| Broker | ref | rs before | rs after |', '|---|---:|---:|---:|']
    before, after = load('isolated-before-beat.json'), load('isolated-after-beat.json')
    for broker in ('redis', 'amqp'):
        values = [after['ref'][broker], before['rs'][broker], after['rs'][broker]]
        lines.append(f'| {broker} | ' + ' | '.join(f'{v/1024:.2f}' for v in values) + ' |')
    lines += ['', '## RPC breakdown after changes (rs client)', '',
              '| Worker | delay() | Outbound to child | Task | Response |',
              '|---|---:|---:|---:|---:|']
    for worker in ('ref', 'rs'):
        data = load(f'isolated-after-latency-{worker}.json')['rs']
        values = [data[key]['p50'] for key in ('publish_ms', 'publish_broker_master_ipc_ms',
                                              'child_ms', 'response_ms')]
        lines.append(f'| {worker} | ' + ' | '.join(f'{v:.4f}' for v in values) + ' |')
    lines += ['', 'Times in ms, medians from separate probes: outbound includes delay().',
              'This probe cannot separate broker, master and IPC time.', '',
              '## Isolated PSS before/after', '',
              '| Process | Before MiB | After MiB | Delta MiB |', '|---|---:|---:|---:|']
    for worker in ('ref', 'rs'):
        before = load(f'isolated-before-latency-{worker}.json')
        after = load(f'isolated-after-latency-{worker}.json')
        pairs = [(f'client rs, worker {worker}', before['rs']['client_pss_kb'],
                  after['rs']['client_pss_kb']),
                 (f'worker {worker} rpc c10', before['worker_pss_kb'], after['worker_pss_kb'])]
        for name, a, b in pairs:
            lines.append(f'| {name} | {a/1024:.3f} | {b/1024:.3f} | {(b-a)/1024:+.3f} |')
    lines += ['', '## Method and limits', '',
              'Other probes were active in the first Redis c10 matrix; the baseline uses',
              'the second isolated run. Final c10 values are medians of both runs.',
              'Isolated latency/PSS probes temporarily restore the native library',
              'and both original Python modules, then measure the final version without other probes.',
              'Profiling is separate from latency measurement; see README-08c.md to reproduce the probes.']
    gates = {}
    for path in sorted(DIRECTORY.glob('gate-*.json')):
        for name, code, seconds, summary in json.loads(path.read_text()):
            gates[name] = (code, seconds, summary)
    if gates:
        lines += ['', '## Gates (latest result per group)', '',
                  '| Gate | Result | Summary |', '|---|---|---|']
        for name, (code, seconds, summary) in gates.items():
            status = 'OK' if code == 0 else f'FAIL ({code})'
            lines.append(f'| {name} | {status} | {summary.replace("|", ";")} |')
        lines += ['', 'The initial full run also failed the new reply queue test',
                  '(exclusive/durable setup and copied mailbox probe needed fixes) and canvas',
                  'AMQP prefork. After fixing the test: 221 Python tests passed; targeted canvas',
                  'passed; full prefork rerun: 0 differences in 22 scenarios, without',
                  'production changes. Upstream still fails only on the known',
                  'test_pytest_celery_marker_registration failure, not investigated.',
                  'gate-* JSON files preserve all initial and rerun outcomes.']
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
