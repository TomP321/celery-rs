"""Controllo PSS beat a tre voci per la spec 08c."""
import argparse
import json
from pathlib import Path
import tempfile

from bench_beat import beat_pss

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--phase', required=True)
args = parser.parse_args()
results = {}
for impl in ('ref', 'rs'):
    results[impl] = {}
    for broker in ('redis', 'amqp'):
        with tempfile.TemporaryDirectory(prefix='beat-08c-') as directory:
            results[impl][broker] = beat_pss(f'.venv-{impl}', broker, 3, directory)
path = Path(__file__).resolve().parent / 'results/08c' / f'{args.phase}-beat.json'
path.write_text(json.dumps(results, indent=2))
print(json.dumps(results))
