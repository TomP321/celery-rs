# 08b — Memoria: dieta degli import, figli prefork, beat, allocatore

Fase 8 di `PLAN.md` (ottimizzazione), seconda parte. La 08a ha tolto kombu dal master del worker;
qui si riduce la memoria in tutti gli altri processi (client, figli prefork, beat) e si valuta
l'allocatore. Ogni intervento entra **solo se il bench lo conferma** e i gate restano verdi.

## Misure di partenza (main @ 303a59a, PSS da `/proc/<pid>/smaps_rollup`)

**Client** (`bench/bench_client.py`, nuovo: `import celery` → app+task → `delay()` → `get()`):

| passo | ref | rs |
|---|---|---|
| app + task | 24,2 MB, 353 moduli | 23,9 MB, 342 moduli |
| dopo `get()` | 35,2 MB, 522 moduli | 30,6 MB, 383 moduli |

rs carica ancora: `kombu.connection`, `kombu.serialization` (→ `yaml`, `msgpack`),
`kombu.transport`, `amqp` (→ `ssl`, `decimal`), `billiard` (→ `multiprocessing`, `pickle`, `cffi`),
`click`, `greenlet`, `importlib.metadata` (→ `email`, `zipfile`), `dateutil`, `tzlocal`. Catene
trovate con `python -X importtime`:
- `celery.utils` → `kombu.utils.objects` → `kombu/utils/__init__` → `kombu.utils.compat` →
  `importlib.metadata`, `kombu.exceptions` → `amqp`, `billiard.util` → `billiard`, `cffi`;
- `celery._state` → `celery.utils.threads` → `greenlet`;
- `celery.app.base` → `click.exceptions` e `dateutil.parser`;
- `celery.signals` → `celery.utils.time` → `tzlocal`;
- `celery.worker` (caricato da chi importa l'app) → `celery.bootsteps` → `kombu.common` →
  `kombu.entity` → `kombu.serialization` (→ `yaml`, `msgpack`) e `kombu.connection`;
  `celery.worker.state` → `shelve`.

**Worker prefork c4, Redis** (`bench/bench.py`, 20 000 `noop`; PSS per processo):

| processo | idle | dopo il carico |
|---|---|---|
| master | 20,9 MB (anon 11,7, file 9,2), 7 thread | 19,8 MB |
| zygote | 8,9 MB | 11,4 MB |
| ogni figlio | 11,7 MB (Private_Dirty 7,3) | 25,1 MB (Private_Dirty 20,5) |
| totale | 76,5 MB | 131,8 MB (ref: 149 → 154 MB) |

La crescita è tutta nei figli (+13 MB ciascuno): al primo task il figlio importa 144 moduli,
fra cui `kombu.transport.redis`, `kombu.transport.virtual*`, `kombu.utils.scheduling/text/time`
(da capire chi li chiede: col trasporto e il backend Rust non servono) e redis-py (`redis`,
`asyncio`, `cryptography`, `opentelemetry`, `email`, `hmac`…), che però è importato dal task del
bench (`benchapp.counter()`: è codice utente). Il figlio ref cresce di 16 MB senza nuovi moduli
(copy-on-write rotto da refcount/GC); il figlio rs ha già `gc.freeze()` (41k oggetti congelati).

**Worker solo, Redis:** 37,0 MB idle → 46,0 MB di picco (ref 49,7 → 50,0).

**Beat** (`bench/bench_beat.py`, 3 voci): ref Redis 51,3 / AMQP 43,2 MB; rs Redis 45,7 /
AMQP 45,7 MB (su AMQP rs è **sopra** il riferimento).

## Progetto

1. **Dieta degli import del client** (il processo dell'utente: `import celery`, `Celery(...)`,
   `@app.task`, `delay`/`apply_async`, `AsyncResult.get`, canvas). Con i producer/backend Rust
   attivi non si devono caricare: `amqp`, `billiard`, `multiprocessing`, `cffi`, `click`,
   `greenlet`, `yaml`, `msgpack`, `ssl`, `importlib.metadata`, `dateutil`, `kombu.connection`,
   `kombu.transport.*`, `kombu.serialization`, `kombu.entity`, `redis` (salvo che li usi l'utente o
   una funzione che li richiede davvero: serializer `yaml`/`pickle`/`msgpack`, `CELERY_RS_*=0`,
   broker/backend non supportati dal core Rust, eventlet/gevent, CLI).
   Tecniche: import pigri nel punto d'uso, PEP 562 (`__getattr__`/`__dir__` di modulo) per i
   nomi pubblici re-esportati, sostituzione delle utilità di kombu usate solo per comodità
   (`cached_property`, `symbol_by_name`, `maybe_list`…) con equivalenti locali **solo dove il
   riferimento non le espone come API** (gli oggetti pubblici restano gli stessi oggetti: se
   `celery.utils.X is kombu.utils.X` nel riferimento, deve restarlo). `celery.worker` non deve
   essere caricato da chi importa solo l'app.
   Il registry dei serializer di kombu deve restare equivalente quando viene usato (stessi
   `content_type`, stessi encoder/decoder registrati, stesso comportamento di
   `app.conf.accept_content`, `register_*`/`disable_untrusted_serializers`).
2. **Figli prefork e zygote**: trovare e togliere ciò che importa `kombu.transport.redis`,
   `kombu.transport.virtual*` e gli altri moduli celery/kombu caricati al primo task nel figlio;
   tutto ciò che il figlio usa per ogni task va importato nello **zygote prima di `gc.freeze()`**
   (così è condiviso in copy-on-write), non al primo task. Misura con una sonda (task che legge
   `sys.modules` e `smaps_rollup`, vedi `bench/bench.py` e il campo nuovo
   `pss_after_per_process_kb`). Valuta anche, numeri alla mano, `gc.freeze()` dopo il warm-up
   del primo import nel figlio e la disattivazione/soglie del GC ciclico solo se identico per
   l'utente (nessun cambio a `gc.get_threshold()` visibile al codice del task, salvo che il
   riferimento lo faccia).
   Il task del bench importa redis-py da sé: non contare quel costo come "nostro", ma riportalo
   separatamente (PSS dei figli con un task che non importa nulla, per esempio una variante
   `--task pure` di `bench.py` che conta i completamenti via result backend o eventi).
3. **Master del worker**: scomporre i 20,9 MB del master prefork (Python heap vs allocazioni Rust
   vs librerie mappate, via `smaps` per mappatura, `tracemalloc`, contatori dell'allocatore) e
   ridurli: moduli Python non necessari nel master (stessa dieta del punto 1), numero di thread
   tokio/blocking (ogni thread porta un'arena glibc) e dimensione dei buffer. Target PLAN: master
   < 12 MB; obiettivo minimo di questa spec ≤ 16 MB in prefork e ≤ 30 MB in `-P solo` Redis.
4. **Allocatore**: provare `mimalloc` (già in `[workspace.dependencies]`) come
   `#[global_allocator]` del modulo `celery._rs` e, separatamente, il limite delle arene glibc
   per i thread Rust (es. `mallopt(M_ARENA_MAX)` solo per il processo worker, se misurabile e
   senza effetti sul codice utente). Adottare solo ciò che riduce PSS idle/picco senza peggiorare
   throughput/latenza oltre il rumore; riportare i numeri anche delle varianti scartate.
   Attenzione al fork (zygote/figli): l'allocatore deve essere sicuro dopo `fork()`.
5. **Beat**: stessa dieta degli import nel processo `celery beat` (Redis e AMQP), con PSS a 3 voci
   **sotto** il riferimento su entrambi i broker (obiettivo ≤ 38 MB).
6. Niente modifiche a nomi, firme, default, testi di log o comportamento osservabile; nessuna nuova
   impostazione pubblica. `CELERY_RS_PRODUCER=0`, `CELERY_RS_BACKEND=0`, `CELERY_RS_WORKER=0`
   continuano a tornare al percorso del riferimento (lì caricare kombu è giusto).

## Test richiesti
- `python/tests_rs/test_import_diet.py` (`.venv-rs`, sottoprocessi con interprete pulito):
  per client (Redis e AMQP: import, app+task, `delay`, `get`, `group`/`chord`), worker master
  (`-P solo` e prefork, via `CELERY_RS_WORKER_STATE` o un task/probe), figlio prefork dopo il primo
  task, `celery beat`: elenco dei moduli vietati assenti. E all'opposto: con serializer `yaml` /
  `pickle` / `msgpack`, con `CELERY_RS_PRODUCER=0`, con `celery.utils.X`/`kombu` usati dall'utente,
  tutto funziona come nel riferimento (messaggi e risultati identici).
- Per ogni nome reso pigro: lo stesso oggetto del riferimento (`is`), `dir()` del modulo uguale,
  pickle/`__reduce__` invariati dove applicabile.
- Test Rust per eventuali cambi di runtime/buffer/allocatore.

## Benchmark (riporta ref vs rs, prima/dopo, Redis e AMQP)
- `bench/bench_client.py` — obiettivo: PSS dopo `get()` ≤ 20 MB (ref 35), nessun modulo vietato.
- `bench/bench.py --pool solo` e `--pool prefork --concurrency 4` (anche `--n 20000`): PSS idle,
  picco, dopo, per processo; throughput e latenza non devono peggiorare. Obiettivi: prefork c4
  Redis picco ≤ 100 MB con il task del bench (ora 125), ≤ 60 MB con `--task pure`.
- `bench/bench_beat.py` — PSS a 3 e 1000 voci, entrambi i broker.
- `bench/bench_cli.py` — tempi della CLI non devono peggiorare.

## Gate (tutti obbligatori, esito reale nel messaggio finale)
```
cargo build --release && cargo clippy --all-targets -- -D warnings
tools/dev/with-brokers cargo test
tools/dev/build-package && tools/dev/build-native
tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q
tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q
./compat api && ./compat wire && ./compat upstream --venv .venv-rs   # 0 / 0 / 0 regressioni
./compat wire-worker --pool solo && ./compat wire-worker --pool prefork   # 0 / 0
./compat wire-beat && ./compat cli                                      # 0 / 0
```
Dopo ogni modifica agli import esegui almeno `pyflakes` sui file toccati e `CELERY_RS_PRODUCER=0`
/ `CELERY_RS_BACKEND=0` sul gruppo di test relativo: nelle fasi 07 e 08a gli import pigri hanno
lasciato nomi usati senza import in rami non coperti (`NameError` a runtime).

## Fuori scope
Latenze rpc/ping/tick di beat (08c), beat embedded via zygote (09).
