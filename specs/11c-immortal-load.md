# 11c — RAM del worker `-c 10`: oggetti immortali e consumo sotto carico

Leggi `AGENTS.md`, `PLAN.md` (fase 10), `docs/DIFFERENZE.md`. Priorità: RAM del worker prefork
`-c 10`, **sia a riposo sia sotto carico**; il throughput non deve peggiorare più del 5%.

## Diagnosi dell'orchestratore (già misurata, Python 3.13, Redis, task `add`)
Strumenti in `bench/memprobe/` (`breakdown.py`: PSS per processo e mappatura; `loadpeak.py`:
picco PSS durante 30.000 task; `sd.py`: pagine scritte dopo il fork via soft-dirty → tipi
di oggetto). Uso: `tools/dev/with-brokers .venv-rs/bin/python bench/memprobe/loadpeak.py
$PWD/.venv-rs 10 $PWD/bench/memprobe 30000`.

| | riposo | picco carico | dopo carico |
|---|---|---|---|
| originale | 307 MB | 307 | 307 |
| celery-rs | 67,5 | 74,6 (figli 55,6) | 76 |

- Ogni figlio ha ~3 MB privati, sporcati tutti nel primo secondo (fork + `process_initializer`),
  poi +0,6–0,8 MB col carico. Le pagine toccate sono soprattutto oggetti `code` (contatori
  dell'interprete adattivo), refcount di `str`/`type`/`tuple`/`function`, `dict`.
- **Esperimento riuscito** (patch temporanea, già rimossa): rendere immortali (PEP 683,
  refcount a 32 bit bassi = `0xFFFFFFFF`) tutti gli oggetti raggiungibili da
  `gc.get_objects()` + referenti diretti, **nel master subito prima del fork dello zygote** e di
  nuovo **nello zygote prima di `gc.freeze()`**, escludendo gli oggetti il cui tipo ha un
  finalizzatore (`__del__`/`tp_finalize`) e le istanze dei tipi di `_celery_rs`:
  riposo 69,6 → 53,3 MB, dopo 2000 task 76,1 → 56,1 MB (master 13,4 → 10,9; figlio 5,7 → 4,1).
- Va fatto **senza strutture temporanee grandi** (un `set` di visitati in Python faceva
  ereditare allo zygote un heap gonfio: risultato peggiore). Il refcount immortale stesso fa
  da marcatore di "già visitato". Dopo la visita: `_release_task_memory(True)` prima del fork.
- Immortalizzare solo nello zygote è controproducente (rompe la condivisione col master: +12 MB).

## Lavoro richiesto
1. **Immortalizzazione nativa** in `celery-py` (niente `ctypes`): funzione interna di
   `_celery_rs` che riceve la lista di `gc.get_objects()` e marca oggetti e referenti
   (`tp_traverse` via `PyType_GetSlot`; scendere anche nei contenitori non tracciati dal GC,
   es. tuple). Condizioni: CPython ≥ 3.12, build non free-threaded, 64 bit little-endian;
   altrimenti no-op (3.10/3.11 restano come oggi). Salta gli oggetti già immortali, quelli con
   `tp_finalize`/`tp_del` e i tipi di `_celery_rs`. `// SAFETY:` sul perché la scrittura del
   refcount è corretta (layout PEP 683 per versione; verifica su 3.12, 3.13 e, se disponibile
   con `uv python`, 3.14: per 3.14 usa il valore iniziale immortale corretto).
   Chiamata nel master prima del fork dello zygote e nello zygote prima di `gc.freeze()`.
   Disattivabile con `CELERY_RS_IMMORTALIZE=0`. Documenta in `docs/DIFFERENZE.md` cosa cambia
   (oggetti esistenti al fork mai liberati: niente `__del__` per quelli senza finalizzatore di
   tipo, weakref/`WeakValueDictionary`/receiver deboli dei segnali che non decadono,
   `sys.getrefcount` enorme) e perché è accettato.
2. **Sotto carico**: misura con `loadpeak.py` e `sd.py` (adattalo per contare le pagine che un
   figlio sporca tra il task 10 e il task 2000) cosa cresce durante l'esecuzione dei task nei
   figli e nel master, e riducilo. Piste da verificare con misure, non da applicare alla cieca:
   allocazioni pymalloc sparse nei pool parzialmente liberi ereditati (riempirli/compattarli
   nello zygote prima del freeze così le nuove allocazioni del figlio finiscono in arene
   proprie), oggetti per-task evitabili nel percorso `_rs_child` → trace → READY (buffer,
   pickle, frame), buffer del master per messaggio/prefetch. Ripeti anche con argomenti da
   16 KB (aggiungi un'opzione a `loadpeak.py`).
3. Test: `python/tests_rs/` — oggetto marcato immortale dopo la chiamata, oggetti con
   finalizzatore e di `_celery_rs` non toccati, oggetti creati dopo normali, no-op con
   `CELERY_RS_IMMORTALIZE=0` e su versioni non supportate; un test che misura le pagine private
   di un figlio forkato (con margine). Test unitari Rust dove possibile.

## Obiettivi (`-c 10`, Redis, task `add`, Python 3.13)
Riposo ≤ 53 MB, picco sotto carico ≤ 58 MB, dopo carico ≤ 56 MB, task/s ≥ 95% di oggi
(~2800). Riporta anche i numeri con argomenti da 16 KB e su RabbitMQ.

## Verifica
`cargo build --release`, `cargo clippy --all-targets -- -D warnings`, `cargo test`
(`tools/dev/with-brokers`), `tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q`,
`./compat api`. **Non lanciare `tools/dev/gates`, `./compat upstream|integration` né soak**:
li esegue l'orchestratore. Comandi bloccanti, niente messaggi di attesa, niente push.
Committa nel branch (`11c: …`); rapporto finale con tabella prima/dopo e misure reali.
