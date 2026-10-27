#!/usr/bin/env python3
"""Tabella delle serie PSS: nessun risultato viene nascosto o aggregato tra pid."""
import json
from pathlib import Path
import sys

print('| Implementazione/broker | Processo | PSS iniziale MiB | Δ max MiB | MiB/h | fd | thread | Esito |')
print('|---|---|---:|---:|---:|---|---|---|')
for arg in sys.argv[1:]:
    path = Path(arg)
    data = json.loads(path.read_text())
    if 'implementation' not in data:
        continue
    for row in data.get('analysis', []):
        slope = row['slope_mib_hour']
        slope_text = f'{slope:.3f}' if slope is not None else 'n/d'
        print(f"| {data['implementation']}/{data['broker']} | {row['process']} | "
              f"{row['baseline_mib']:.2f} | {row['max_growth_mib']:.2f} | {slope_text} | "
              f"{row['fds']} | {row['threads']} | {'OK' if row['passed'] else 'FAIL'} |")
    print(f"\n{path.name}: complete={data['complete']}, passed={data.get('passed')}, "
          f"error={data.get('error')}\n")
