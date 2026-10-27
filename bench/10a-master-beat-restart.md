# 10a — risultato e limiti

Rapporto storico del commit `a9b80be`. La decisione successiva del proprietario
e la configurazione adottata sono in [10a-owner-memory.md](10a-owner-memory.md).

Al termine di quel commit la spec **restava aperta**: il salto Redis è corretto e verificato; gli obiettivi
assoluti di master ≤13 MiB e beat ≤22 MiB non sono raggiunti. Nessuna nuova
dipendenza. `celery/`, i test upstream e il codice di `tools/compat/` sono invariati.

Misure su Linux/Python 3.13, PSS in MiB (KiB/1024). Baseline: `e6ae45c`, sorgenti
estratti in `/tmp/10a-baseline/python` e modulo nativo della build iniziale, senza
modificare il riferimento. Misure finali senza compilazioni concorrenti; ogni
singola prova dura meno di cinque minuti. Per worker, baseline mediana di tre
prove e finale mediana di due; il dettaglio è in `results/10a-repeats.json`.

| PSS idle / salto | ref misurato | rs prima | rs finale |
|---|---:|---:|---:|
| master prefork c10 | 26,75 | 19,09 | 18,78 |
| zygote | — | 7,93 | 7,86 |
| figlio c10, media | 28,13 | 7,48 | 7,39 |
| beat 3 voci Redis | 47,52 | 29,15 | 28,35 |
| beat 3 voci AMQP | 39,62 | 28,47 | 28,39 |
| client dopo get Redis | 33,77 | 26,30 | 26,27 |
| client dopo get AMQP | 33,68 | 26,46 | 26,48 |
| peggior restart Redis, totale c10 | +2,30 | +43,74 | +2,15 |
| peggior restart AMQP, totale c10 | +2,06 | non misurato | +3,73 |

Il probe del riavvio usa **tutti e dieci** i figli prima di ogni campionamento,
così il primo utilizzo di un backend non viene contato come riconnessione.
La baseline Redis genera 30 errori di task nei due riavvii: tenta il primo GET su
socket backend chiusi, importa redis-py nel master quando riceve l'eccezione e
conserva sorgenti/traceback nei figli. Questo probe mirato amplifica il problema
rispetto ai +14 MiB del soak della spec; non è un nuovo soak.

Nel codice finale nessun errore di task, nessun nuovo modulo nel master e stessi
PID prima/dopo entrambi i riavvii. Incrementi successivi: Redis +2,15/+0,20 MiB,
AMQP +2,04/+3,73 MiB. Sono sotto 5 MiB **per riavvio**; il cumulato AMQP dopo due
riavvii è +5,76 MiB. Dati finali conservati dai test in
`results/10a-restart-final-{redis,amqp}.json`.

## Modifiche

- `apps/worker.py`: `cry` è l'oggetto originale risolto con PEP 562; il normale
  master non importa `celery.utils.debug` e psutil. `dir`, identità e pickle sono
  verificati contro il riferimento.
- `concurrency/_rs_zygote.py`: Redis e il mixin asincrono si caricano tramite il
  backend effettivo, evitando di trattenerli per backend memory/disabled.
- `concurrency/rs_prefork.py`: `TERM_SIGNAL` proviene direttamente da billiard.common.
- `worker/_rs_consumer.py` e `backends/_rs_redis.py`: alla riconnessione Redis si
  scartano solo i socket inattivi del backend nativo sullo stesso host/porta e
  con lo stesso tipo TLS, anche se il database è diverso. Backend indipendenti,
  redis-py e client personalizzati restano sul loro percorso.
- Pool Rust, binding PyO3 e `_rs_child.py`: un frame IPC dedicato raggiunge tutti
  i figli prima dei nuovi task. Non conta come job e non modifica le statistiche
  del pool. I figli occupati lo eseguono dopo il task corrente.
- `bench.py`, `bench_beat.py`, `bench_client.py`: `--source` permette confronti
  isolati; il client accetta anche `--label`. Nuovo `bench_restart.py` e undici
  test in `python/tests_rs/test_master_beat_restart.py`.

## Profili, prestazioni e varianti scartate

Beat carica ancora 364 moduli: heap Python tracciato circa 16,73 MiB, distinto
dalla PSS perché tracemalloc occupa memoria propria. Il campione fisico Redis
si divide in anonimo 19,49, librerie 5,29, Python 1,41 e modulo Rust 2,19 MiB.
Profili completi: `results/08f-10a-profile.json`,
`results/10a-master-{before,final}.json`; gli `*-importtime.txt` descrivono gli
import dei moduli di bootstrap, non un'intera sessione CLI.

Throughput worker rs prima 2439–2680 task/s, finale 2452–2567; ref 842.
Latenza p50 prima 1,73–2,10 ms, finale 1,96–2,04; ref 2,35.
Tick beat Redis 1012→1025 µs, AMQP 1058→1044 µs; invii/s 650→689 e 777→738.
I campioni brevi mostrano variabilità e intervalli sovrapposti; non dimostrano un
miglioramento delle latenze di coda. Nessuna crescita misurabile del client.

Varianti non adottate, misure esplorative in `results/10a-discarded.json`:

- classi del pool tradizionale su descrittori pigri: sei differenze API statiche;
  master 18,81 MiB, sostanzialmente uguale al finale. Ripristinate le classi originali.
- `opt-level=s` su tutto: modulo nativo circa 11→7,6 MiB, ma crontab semplice
  3,66→4,04 µs nella prima tornata; non adottato.
- solo binding a `s`: modulo circa 12 MiB, maggiore della baseline; scartato.
- binding e dipendenze a `s`: master 18,52 MiB nella prima prova isolata, p50
  2,14 ms contro 1,73 della baseline della stessa tornata. Beneficio insufficiente
  per cambiare i profili del progetto. Tutti i profili Cargo sono ripristinati.

## Verifiche eseguite

- `cargo build --release`, `cargo clippy --all-targets -- -D warnings`: verdi.
- `tools/dev/with-brokers cargo test`: verde, 242 test passati, 4 ignorati.
- `tools/dev/build-package`, `tools/dev/build-native`: completati, anche nella
  configurazione finale a `opt-level=3`.
- `.venv-ref/bin/python -m pytest crates/celery-py/tests -q`, sotto lock:
  81 passati.
- Suite mirata rs: worker_pool, worker_prefork, kombu_identity, beat,
  beat_memory, import_diet, master_beat_restart, backend_redis: 110 passati e
  un test raccolto prima della rimozione dei descrittori rimasto obsoleto.
  Dopo l'aggiornamento, rieseguito l'intero `test_master_beat_restart.py`:
  **11 passati**, inclusi entrambi i broker e l'assenza di errori di task.
- Test upstream `t/unit/concurrency/test_prefork.py` e
  `t/unit/apps/test_worker.py`, `-o pythonpath=python`, con
  `CELERY_RS_WORKER=0 CELERY_RS_PRODUCER=0 CELERY_RS_BACKEND=0`: 83 passati.
- `./compat api`: **0 differenze** su 137 moduli e settings/signals/CLI.
- `./compat wire-beat`: **0 differenze** su Redis e AMQP.
- Pyflakes sui file Python modificati e `git diff --check`: verdi.
- `cargo fmt --all -- --check`: non pulito per formattazione preesistente in
  più crate; nessuna riformattazione globale applicata.

I benchmark riportati sono stati eseguiti sotto `tools/dev/with-brokers`:
`bench.py --pool prefork --concurrency 10 --task pure --n 3000 --latency-n 100|200`,
`bench_beat.py`, `bench_client.py`, `bench_restart.py`, `bench_memory.py` e
`bench_beat_memory.py --trace`. Nessun gate globale, soak o compat integration.

Restano aperti i target assoluti del master e di beat: trace, canvas, result,
backend e asynpool sono ancora presenti nel master. La riduzione degli import
ottenuta è piccola; non considero soddisfatti tutti i criteri della spec.
