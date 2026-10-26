# 08f — rapporto misurato

Le soglie della spec **non sono tutte raggiunte**. Il lavoro riduce la crescita della schedule e migliora il tick; restano la base ≤24 MB, il limite base+3 MB a 1000 voci su AMQP e una riduzione dimostrabile del costo di `-B`.

PSS in MiB da `/proc/*/smaps`; tutti i benchmark con broker sono stati eseguiti sotto `tools/dev/with-brokers`. Le librerie condivise e la concorrenza esterna introducono rumore: i delta negativi di `-B` nella baseline non sono risparmi attribuibili al codice.

| Broker | Voci | Ref | rs prima | rs dopo |
|---|---:|---:|---:|---:|
| redis | 3 | 66.06 | 32.60 | 27.23 |
| redis | 1000 | 54.97 | 39.12 | 29.14 |
| redis | 10000 | 117.82 | 118.26 | 53.87 |
| amqp | 3 | 39.61 | 28.51 | 28.46 |
| amqp | 1000 | 46.42 | 39.68 | 33.79 |
| amqp | 10000 | 109.58 | 118.31 | 56.29 |

Tra 1000 e 10000 voci la crescita rs è di redis: 2.81 KiB/voce, amqp: 2.56 KiB/voce. Il confronto diretto 3→1000 include anche il costo fisso della prima pubblicazione (runtime e percorsi client), assente nelle tre voci a intervallo di un’ora.

| Broker | Tick ref / prima / dopo (µs) | Invii ref / prima / dopo (/s) |
|---|---:|---:|
| redis | 1239 / 1307 / 1049 | 388 / 435 / 674 |
| amqp | 1250 / 1328 / 1070 | 442 / 459 / 740 |

Worker: PSS totale del master e di **tutti** i discendenti; ogni cella è senza `-B` → con `-B` (MiB). Il beat rs resta in thread. Non è stato introdotto un nuovo processo zygote per beat: i dati non dimostrano che ridurrebbe la PSS totale.

| Broker / pool | Ref | rs prima | rs dopo |
|---|---:|---:|---:|
| redis / solo  | 51.77 → 64.16 | 36.30 → 34.21 | 30.95 → 31.92 |
| redis / prefork c10 | 317.47 → 336.56 | 93.08 → 87.28 | 79.08 → 85.69 |
| amqp / solo  | 40.13 → 52.89 | 32.02 → 33.99 | 32.04 → 33.93 |
| amqp / prefork c10 | 254.85 → 266.01 | 83.64 → 83.36 | 84.46 → 87.80 |

Scomposizione a 3 voci. L’heap Python è misurato in un processo separato con tracemalloc, per non falsare la PSS. Le pagine anonime comprendono heap Python, heap Rust, stack e frammentazione: **non sono una misura isolata dell’heap Rust**. La colonna nativa misura le mappature dell’estensione, non le sue allocazioni.

| Broker / versione | Anonimo MiB | Estensione MiB | Altre mappature MiB | Heap Python tracciato MiB | Moduli |
|---|---:|---:|---:|---:|---:|
| redis / ref | 57.46 | 0.00 | 8.60 | 35.05 | 755 |
| redis / before | 23.63 | 2.28 | 6.70 | 16.79 | 366 |
| redis / final | 19.48 | 2.28 | 5.47 | 16.73 | 364 |
| amqp / ref | 31.25 | 0.00 | 8.36 | 27.55 | 656 |
| amqp / before | 19.55 | 2.28 | 6.68 | 16.79 | 366 |
| amqp / final | 19.49 | 2.28 | 6.69 | 16.73 | 364 |

Modifiche:

- `PersistentScheduler.sync` conserva la sola mappa delle voci nella cache writeback dopo il flush: heap e scheduler non rileggono copie dello stesso pickle. Formato shelve invariato.
- `crontab` conserva i campi espansi nel core Rust finché non sono osservati. Attributi e `vars()` materializzano set veri, mutabili; i set forniti dall’utente conservano identità. Le modifiche in-place vengono rispettate.
- Il confronto di crontab compatti usa i campi Rust. Il percorso comune evita di riespandere i campi durante il confronto della schedule.
- Il nodo pidbox è importato soltanto quando richiesto: 366→364 moduli nel beat rs, circa 65 KiB di heap Python tracciato in meno. Gli oggetti kombu restano quelli reali.
- Il testo del log DEBUG della schedule viene costruito solo quando il livello è abilitato.

Scartato GC + `malloc_trim`: 30499→30439 KiB nella prova iniziale, solo 60 KiB; nessuna modifica all’allocatore adottata. Nessuna nuova dipendenza Rust.

Verifiche mirate prima del gate finale: differenziale crontab (240000 casi) 4 passati; nuovi test memoria/API/persistenza/pidbox 5 passati; un’ora accelerata, 10800 invii, crescita PSS fra i campioni dopo il warm-up <2 MiB. `pyflakes` pulito sui nuovi file e su beat; schedules conserva quattro segnalazioni preesistenti per import pubblici di compatibilità inutilizzati internamente. `git diff --check` pulito.

Comandi dei benchmark:
```sh
tools/dev/with-brokers .venv-rs/bin/python bench/bench_beat_memory.py --venv .venv-rs --label before --source "$PWD/target/08f-baseline/python" --workers --trace
tools/dev/with-brokers .venv-ref/bin/python bench/bench_beat_memory.py --venv .venv-ref --label ref --workers --trace
tools/dev/with-brokers .venv-rs/bin/python bench/bench_beat_memory.py --venv .venv-rs --label final --workers --trace
```

La baseline Python è estratta da HEAD prima delle modifiche (nessuna modifica al sorgente di riferimento). JSON completi, elenchi moduli e statistiche tracemalloc sono nei file `08f-{before,ref,final}.json`; i profili di import sono in `08f-importtime-{before,after}.txt`.

Gate finale: `tools/dev/gates`, una sola esecuzione completa dopo le modifiche,
714,5 s; log in `target/gates/20261026-173646-2559402`.

| Verifica | Esito reale |
|---|---|
| cargo build --release | OK |
| cargo clippy --all-targets -- -D warnings | OK |
| build-package / build-native | OK / OK |
| cargo test sotto lock | 234 passati |
| crates/celery-py/tests | 81 passati |
| python/tests_rs (quattro gruppi) | 209 passati |
| compat api | 0 differenze, 137 moduli |
| compat wire | 0 differenze, 40 scenari |
| compat wire-worker solo / prefork | 0 / 0 differenze, 22 scenari ciascuno |
| compat wire-beat | 0 differenze, 2 broker |
| compat cli | 0 differenze, 190 comandi |
| compat upstream | exit 1: solo il fallimento noto test_pytest_celery_marker_registration, ignorato su istruzione dell’utente; 3 instabili |

Il gate nel suo complesso restituisce quindi exit 1, non viene dichiarato
interamente verde. Nessuna modifica a `celery/`, `celery/t/`, `tools/compat/`
o ai test d’identità kombu. La spec resta parzialmente aperta per i target
RAM indicati all’inizio. L’heap Rust non è stato isolato dalle altre allocazioni
anonime: questo limite della scomposizione resta esplicito.

Una prima sonda importtime è terminata con SIGBUS mentre il gate reinstallava
l’estensione nativa; rieseguita dopo la conclusione delle build, passa in entrambe
le varianti. Non è stata usata per le misure di PSS né per i test del gate.
