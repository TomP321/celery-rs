"""Genera il rapporto ripetibile c10 dai JSON di bench.py (spec 08d)."""
import argparse
import json
import statistics
from pathlib import Path


def average(rows, key):
    values = [row[key] for row in rows]
    return statistics.mean(values), min(values), max(values)


def memory(value):
    return f'{value / 1024:.1f}'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, default=Path('bench/results/08d'))
    parser.add_argument('--phase', default='after')
    args = parser.parse_args()
    groups = {}
    for path in sorted(args.directory.glob(f'{args.phase}-*.json')):
        row = json.loads(path.read_text())
        broker = 'amqp' if row['broker'].startswith('amqp') else 'redis'
        impl = 'ref' if row['venv'].endswith('ref') else 'rs'
        groups.setdefault((broker, row['task'], impl), []).append(row)
    print('PSS in MiB; media e intervallo min–max di almeno due esecuzioni.\n')
    print('| Broker/task | impl (n) | idle | picco | dopo | task/s | p50/p99 ms |')
    print('|---|---|---:|---:|---:|---:|---:|')
    for (broker, task, impl), rows in sorted(groups.items()):
        cells = []
        for key in ('pss_idle_kb', 'pss_peak_kb', 'pss_after_kb'):
            mean, low, high = average(rows, key)
            cells.append(f'{memory(mean)} ({memory(low)}–{memory(high)})')
        rate, low, high = average(rows, 'end_to_end_tasks_per_s')
        latency = [statistics.mean(row['latency_ms'][key] for row in rows)
                   for key in ('p50', 'p99')]
        print(f'| {broker}/{task} | {impl} ({len(rows)}) | ' + ' | '.join(cells)
              + f' | {rate:.0f} ({low:.0f}–{high:.0f}) | {latency[0]:.2f}/{latency[1]:.2f} |')
    print('\n| Broker/task | risparmio idle MiB (%) | picco MiB (%) | dopo MiB (%) |')
    print('|---|---:|---:|---:|')
    for broker in ('redis', 'amqp'):
        for task in ('bench', 'pure'):
            ref = groups.get((broker, task, 'ref'))
            rs = groups.get((broker, task, 'rs'))
            if not ref or not rs:
                continue
            cells = []
            for key in ('pss_idle_kb', 'pss_peak_kb', 'pss_after_kb'):
                reference = average(ref, key)[0]
                saved = reference - average(rs, key)[0]
                cells.append(f'{memory(saved)} ({saved / reference * 100:.1f}%)')
            print(f'| {broker}/{task} | ' + ' | '.join(cells) + ' |')
    print('\nPSS e Private_Dirty per processo, ogni esecuzione (idle → dopo, MiB).\n')
    print('| misura | ruolo/pid | PSS | Private_Dirty |')
    print('|---|---|---:|---:|')
    for key, rows in sorted(groups.items()):
        for index, row in enumerate(rows, 1):
            after = {process['pid']: process for process in row['after_processes']}
            for process in row['idle_processes']:
                end = after.get(process['pid'])
                if end is None:
                    continue
                print(f'| {"/".join(key)}/{index} | {process["role"]}/{process["pid"]} | '
                      f'{memory(process["pss_kb"])} → {memory(end["pss_kb"])} | '
                      f'{memory(process["private_dirty_kb"])} → {memory(end["private_dirty_kb"])} |')


if __name__ == '__main__':
    main()
