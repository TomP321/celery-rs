"""Tabelle delle misure salvate da run_08c, probe_08c e memory_08c."""
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
        # Il secondo giro non ha altre sonde Python attive sulla macchina.
        records = [max(records, key=lambda record: int(record['label'].rsplit('-', 1)[1]))]
    return {key: statistics.median(record[key] for record in records)
            for key in ('worker_tasks_per_s', 'pss_idle_kb', 'pss_peak_kb', 'pss_after_kb')}


def main():
    lines = ['# Misure 08c', '',
             '10.000 task per prova; PSS in MiB; capacità con coda piena, dalla ripresa del worker.', '',
             '## Capacità', '', '| Broker | c | Task | ref task/s | rs prima | rs dopo |',
             '|---|---:|---|---:|---:|---:|']
    for broker in ('redis', 'amqp'):
        for concurrency in (4, 10):
            for task in ('pure', 'bench', 'bench-toplevel'):
                values = [matrix(phase, impl, broker, concurrency, task)['worker_tasks_per_s']
                          for phase, impl in [('after', 'ref'), ('before', 'rs'), ('after', 'rs')]]
                lines.append(f'| {broker} | {concurrency} | {task} | ' +
                             ' | '.join(f'{value:.0f}' for value in values) + ' |')
    lines += ['', '## PSS c10', '',
              '| Broker | Task | ref idle/picco | rs prima idle/picco | rs dopo idle/picco | Risparmio picco |',
              '|---|---|---:|---:|---:|---:|']
    for broker in ('redis', 'amqp'):
        for task in ('pure', 'bench', 'bench-toplevel'):
            records = [matrix(phase, impl, broker, 10, task)
                       for phase, impl in [('after', 'ref'), ('before', 'rs'), ('after', 'rs')]]
            values = [f"{d['pss_idle_kb']/1024:.1f}/{d['pss_peak_kb']/1024:.1f}" for d in records]
            saved = records[0]['pss_peak_kb'] - records[2]['pss_peak_kb']
            lines.append(f'| {broker} | {task} | ' + ' | '.join(values) +
                         f" | {saved/1024:.1f} ({100*saved/records[0]['pss_peak_kb']:.1f}%) |")
    lines += ['', '## Latenze AMQP prefork c10', '',
              '| Worker | Misura | ref p50/p99 ms | rs prima | rs dopo |',
              '|---|---|---:|---:|---:|']
    for worker in ('ref', 'rs'):
        before = load(f'isolated-before-latency-{worker}.json')
        after = load(f'isolated-after-latency-{worker}.json')
        for metric in ('rpc_ms', 'ping_ms'):
            values = [d[client][metric] for d, client in [(after, 'ref'), (before, 'rs'), (after, 'rs')]]
            lines.append(f'| {worker} | {metric} | ' +
                         ' | '.join(f"{d['p50']:.3f}/{d['p99']:.3f}" for d in values) + ' |')
    lines += ['', '## Ping in-process verso worker AMQP solo', '',
              '| Worker | ref p50/p99 ms | rs prima | rs dopo |', '|---|---:|---:|---:|']
    for worker in ('ref', 'rs'):
        before = load(f'isolated-before-solo-latency-{worker}.json')
        after = load(f'isolated-after-solo-latency-{worker}.json')
        values = [d[client]['ping_ms'] for d, client in [(after, 'ref'), (before, 'rs'), (after, 'rs')]]
        lines.append(f'| {worker} | ' +
                     ' | '.join(f"{d['p50']:.3f}/{d['p99']:.3f}" for d in values) + ' |')
    lines += ['', '## Beat PSS (3 voci)', '', '| Broker | ref | rs prima | rs dopo |', '|---|---:|---:|---:|']
    before, after = load('isolated-before-beat.json'), load('isolated-after-beat.json')
    for broker in ('redis', 'amqp'):
        values = [after['ref'][broker], before['rs'][broker], after['rs'][broker]]
        lines.append(f'| {broker} | ' + ' | '.join(f'{v/1024:.2f}' for v in values) + ' |')
    lines += ['', '## Scomposizione RPC dopo (client rs)', '',
              '| Worker | delay() | Andata fino al figlio | Task | Risposta |',
              '|---|---:|---:|---:|---:|']
    for worker in ('ref', 'rs'):
        data = load(f'isolated-after-latency-{worker}.json')['rs']
        values = [data[key]['p50'] for key in ('publish_ms', 'publish_broker_master_ipc_ms',
                                              'child_ms', 'response_ms')]
        lines.append(f'| {worker} | ' + ' | '.join(f'{v:.4f}' for v in values) + ' |')
    lines += ['', 'Tempi in ms, mediane di sonde distinte: delay() è incluso nell’andata.',
              'Broker, master e IPC non sono separabili con questa sonda.', '',
              '## PSS isolata prima/dopo', '',
              '| Processo | Prima MiB | Dopo MiB | Delta MiB |', '|---|---:|---:|---:|']
    for worker in ('ref', 'rs'):
        before = load(f'isolated-before-latency-{worker}.json')
        after = load(f'isolated-after-latency-{worker}.json')
        pairs = [(f'client rs, worker {worker}', before['rs']['client_pss_kb'],
                  after['rs']['client_pss_kb']),
                 (f'worker {worker} rpc c10', before['worker_pss_kb'], after['worker_pss_kb'])]
        for name, a, b in pairs:
            lines.append(f'| {name} | {a/1024:.3f} | {b/1024:.3f} | {(b-a)/1024:+.3f} |')
    lines += ['', '## Metodo e limiti', '',
              'La prima matrice Redis c10 aveva altre sonde attive: per il prima si usa',
              'il secondo giro isolato. Il dopo c10 riporta la mediana dei due giri.',
              'Le prove di latenza/PSS isolate ripristinano temporaneamente la libreria nativa',
              'e i due moduli Python originali, poi misurano la versione finale senza altre sonde.',
              'Profilazione separata dalle latenze; vedi README-08c.md per riprodurre le sonde.']
    gates = {}
    for path in sorted(DIRECTORY.glob('gate-*.json')):
        for name, code, seconds, summary in json.loads(path.read_text()):
            gates[name] = (code, seconds, summary)
    if gates:
        lines += ['', '## Gate (ultimo esito per gruppo)', '',
                  '| Gate | Esito | Sintesi |', '|---|---|---|']
        for name, (code, seconds, summary) in gates.items():
            status = 'OK' if code == 0 else f'FAIL ({code})'
            lines.append(f'| {name} | {status} | {summary.replace("|", ";")} |')
        lines += ['', 'Il giro completo iniziale ha fallito anche il nuovo test della reply queue',
                  '(setup esclusivo/durevole e sonda del mailbox copiato da correggere) e canvas',
                  'AMQP prefork. Dopo la correzione del test: 221 test Python verdi; canvas',
                  'mirato verde e ripetizione completa prefork 0 differenze su 22, senza cambi',
                  'di produzione. Upstream rimane rosso solo per il fallimento noto',
                  '`test_pytest_celery_marker_registration`, non indagato.',
                  'I JSON gate-* conservano tutti gli esiti iniziali e delle ripetizioni.']
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
