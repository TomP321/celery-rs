# 10a — chiusura del follow-up del proprietario

Adottata la decisione successiva a `ccd5bc3`: minimizzare la RAM mantenendo
prestazioni superiori al riferimento. Le richieste del follow-up sono completate;
non dichiaro raggiunti i target storici di 13 MiB per il master e 22 MiB per beat.

`TaskPool.Pool` e `TaskPool.BlockingPool` risolvono e conservano le classi vere
al primo accesso. Due test in interpreti puliti esercitano come primo accesso
rispettivamente la classe e l'istanza, verificando l'identità con `AsynPool` e
`billiard.pool.Pool`. Il probe del worker c10 verifica che `asynpool`,
`billiard.pool` e `billiard.queues` siano assenti dal master, anche dopo la
riconnessione. Restano le utility di billiard richieste dagli oggetti kombu veri.
Il percorso tradizionale continua a usare le implementazioni originali.
Anche il bootstep Hub evita di importare il pool billiard solo per sostituire
il lock di ApplyResult quando è selezionato il pool Rust. Il lock dell'orologio
e la patch del percorso tradizionale restano attivi.

Codice modificato: `Cargo.toml`, `python/celery/concurrency/prefork.py`,
`python/celery/worker/components.py` e
`python/tests_rs/test_master_beat_restart.py`; aggiunti rapporti e misure.
L'elenco dei moduli osservati nei master è conservato in
`results/10a-owner-master-imports.json`.

## Misure finali

PSS idle in MiB; figlio = media dei dieci figli. Prima: Python e modulo nativo
di `ccd5bc3`, copiati in `/tmp/10a-owner-before/python`. Dopo: pool pigri e
build release a `opt-level="s"`, con **solo `celery-beat` a `opt-level=3`**.
Nessuna nuova dipendenza né modifica a `tools/compat`.

| Processo | Prima | Dopo |
|---|---:|---:|
| master prefork c10 | 22,04 | 18,27 |
| zygote | 8,07 | 7,99 |
| figlio medio | 7,61 | 7,49 |
| totale worker c10 | 106,17 | 101,18 |
| beat 3 voci Redis | 29,07 | 28,55 |
| beat 3 voci AMQP | 28,37 | 28,57 |
| client dopo get Redis | 26,23 | 26,00 |
| client dopo get AMQP | 26,57 | 26,28 |

Il risparmio del worker è 4,99 MiB, circa il 4,7%. Il beat AMQP misura invece
**+0,20 MiB**: non sostengo un miglioramento in ogni singola riga. La scelta
favorisce la riduzione complessiva e quella di master, figli, client e beat Redis.

| Prestazioni worker Redis, task pure | Prima | Dopo | Riferimento misurato |
|---|---:|---:|---:|
| p50 delay/get, ms | 2,120 | **2,012** | 2,483 |
| task/s end-to-end | 2419 | **2591** | 823 |

La p50 è sotto 3 ms e sotto quella del riferimento; il throughput è circa tre
volte quello del riferimento. Campioni brevi: 3000 task e 200 misure di latenza.

Sono state confrontate tre build compatte, tutte con pool pigri, prima della
correzione del bootstep Hub. La tabella iniziale include anche tale correzione.

| Variante | Master | Zygote | Figlio | Totale c10 | p50 ms |
|---|---:|---:|---:|---:|---:|
| solo dipendenze e binding a s | 18,54 | 7,82 | 7,34 | 99,74 | 2,097 |
| anche worker/transport/backend a s | 18,35 | 7,88 | 7,41 | 100,33 | 2,065 |
| tutto a s, crontab a 3 — adottata | 18,37 | 7,84 | 7,36 | 99,87 | 1,962 |

La differenza di 0,13 MiB sul totale c10 fra la prima e l'ultima variante è
piccola; l'ultima riduce ulteriormente master, client Redis e le due misure di
beat rispetto alla prima. I dati grezzi sono rispettivamente
`results/*10a-owner-after.json`, `results/*10a-owner-io-s.json` e
`results/*10a-owner-cron3.json`; il worker dopo la correzione Hub è in
`results/10a-owner-selected.json`. Baseline e riferimento hanno suffissi
`10a-owner-before` e `10a-owner-ref`.

## Comandi e verifica

Misure sequenziali, senza compilazioni o test concorrenti, tutte sotto
`tools/dev/with-brokers`; ogni singolo benchmark dura meno di cinque minuti:

```text
.venv-rs/bin/python bench/bench.py --venv .venv-rs --pool prefork --concurrency 10 --task pure --n 3000 --latency-n 200 --label <variante>
.venv-rs/bin/python bench/bench_beat.py --venv .venv-rs --label <variante>
.venv-rs/bin/python bench/bench_client.py --label <variante>
```

Per la baseline è stato aggiunto `--source /tmp/10a-owner-before/python`; per
il riferimento worker `--venv .venv-ref`. Nessun gate o soak eseguito.

Verifiche completate nella configurazione adottata:

- `cargo build --release` e `cargo clippy --all-targets -- -D warnings`: verdi.
- `tools/dev/with-brokers cargo test`: 242 passati, 4 ignorati; il profilo debug
  non cambia con le impostazioni release.
- `tools/dev/build-package` e `tools/dev/build-native`: completati.
- Test rs `test_master_beat_restart.py`, `test_kombu_identity.py`,
  `test_beat_memory.py`: **21 passati**, inclusi i due broker, gli accessi da
  classe/istanza e l'assenza dei moduli del pool nel master reale.
- Unit upstream `concurrency/test_prefork.py`, `apps/test_worker.py` e
  `worker/test_components.py`, con `-o pythonpath=python` e
  `CELERY_RS_WORKER=0 CELERY_RS_PRODUCER=0 CELERY_RS_BACKEND=0`: **97 passati**.
- `.venv-ref/bin/python -m pytest crates/celery-py/tests -q`, sotto lock:
  **81 passati**; benchmark JSON 625k/s contro 247k/s del riferimento,
  encode task v2 88k/s contro 16k/s.
- `./compat api`: **0 differenze contate** su 137 moduli e settings/signals/CLI,
  usando le accettazioni aggiunte dal proprietario in `ccd5bc3`.
- `./compat wire-beat`: **0 differenze** su Redis e AMQP.
- Pyflakes sui file Python modificati e `git diff --check`: verdi.

La prima esecuzione del probe aveva individuato due fallimenti per l'import
residuo di billiard.pool dal bootstep Hub; dopo la correzione l'intero gruppo
di 21 test è stato rieseguito con successo.
