# celery-rs — Piano

Riscrittura di Celery (riferimento: `celery/` 5.7.0rc1, ~47k righe Python) con core in Rust.

## Obiettivi

1. **Drop-in Python**: `pip install celery-rs` fornisce il pacchetto `celery`. Gli import, `Celery(...)`,
   `@app.task`, `Task`, `.delay/.apply_async`, `AsyncResult`, canvas (`chain/group/chord/signature`),
   segnali, config (`app.conf.*`), CLI `celery worker|beat|inspect|call|purge|...` restano identici.
2. **Interop di protocollo**: messaggi Task Protocol v2 (e lettura v1), eventi e control/broadcast
   identici byte-per-byte (a meno di id/timestamp). Client Celery originale ⇄ worker Rust e viceversa.
3. **Broker/backend v1**: Redis, RabbitMQ (AMQP 0.9.1 + backend `rpc://`), `memory://` / `cache+memory://`.
4. **Memoria e CPU al massimo livello** (vedi §Ottimizzazioni), misurate sempre contro il riferimento.

Fuori scope v1: SQS, DB/SQLAlchemy, Mongo, DynamoDB, GCS, ecc. (restano `ImportError` chiari);
pool `eventlet/gevent` (v2).

## Architettura: "Rust core, guscio Python sottile"

Non si può eseguire codice utente Python senza un interprete Python, e l'API pubblica include classi
che l'utente estende (`Task`, `Celery`, bootsteps, signal handler). Quindi:

```
                      ┌───────────────────────── processo Python utente (client) ─────────────┐
                      │ celery/ (guscio Python: stesse classi/firme)                          │
                      │   └─ celery._rs  (PyO3) → protocollo, serializzazione, producer,      │
                      │                          result backend, pool connessioni             │
                      └───────────────────────────────────────────────────────────────────────┘
 broker (Redis/AMQP/memory)
                      ┌──────── celery worker (master Python + core Rust incorporato) ────────┐
                      │ consumer async (tokio), QoS/prefetch, ack, heartbeat, eventi,         │
                      │ control/broadcast, timer ETA, rate limit, revoke, autoscale           │
                      │   └─ pool: zygote (fork prima dei thread Rust) → N figli Python       │
                      │        IPC: pipe/shm con frame binari, esecuzione via celery.app.trace │
                      └───────────────────────────────────────────────────────────────────────┘
```

- **Master del worker ibrido** (decisione presa dopo le misure del 2026-10-24, vedi sotto):
  processo Python che importa l'app e contiene il core Rust (thread tokio per broker I/O,
  consumer, QoS, timer ETA, rate limit, revoke, eventi, heartbeat, control built-in, supervisione
  del pool). Segnali, bootstep e comandi di controllo personalizzati restano Python, nello stesso
  processo dove li esegue Celery: semantica identica. Kombu, billiard e asynpool non vengono
  più caricati nel worker.
- **Zygote**: forkato dal master *prima* di avviare i thread Rust (fork sicuro, un solo thread),
  con l'app già importata e `gc.freeze()`; crea i figli su richiesta del master via pipe.
- **Figli**: CPython che eseguono solo `trace_task`, condividendo in copy-on-write le pagine
  dell'app con zygote e master.

Perché non un master in Rust puro: interprete vuoto 5 MB PSS, `import celery` 6 MB, app +
macchina del worker Celery 38 MB. I figli hanno comunque bisogno di un processo Python con l'app
caricata da cui essere forkati; se quel processo è il master, le sue pagine sono condivise con
i figli e la PSS aggiuntiva è piccola, mentre un master Rust puro richiederebbe lo stesso processo
Python come zygote e dovrebbe emulare segnali/bootstep/control utente fuori dal loro processo.
- Il **guscio Python** mantiene i moduli e le firme pubbliche di Celery; le parti interne pesanti
  delegano a `celery._rs`. `kombu`/`billiard`/`vine` restano dipendenze importabili solo per i
  tipi pubblici che l'utente usa (`kombu.Queue`, `Exchange`), non per il trasporto.

## Struttura repository

```
celery-rs/
  Cargo.toml                  workspace, profile release: lto=fat, codegen-units=1, panic=abort
  crates/
    celery-proto/             messaggi v1/v2, headers, embed, serializer (json/msgpack/pickle-passthrough), compressione
    celery-transport/         trait Transport + redis (fred/redis-rs), amqp (lapin), memory
    celery-backend/           result backend: redis, rpc, memory; chord counter, group results
    celery-worker/            consumer, strategy, request, timer, rate limit, revoke, control, eventi, pool supervisor
    celery-beat/              scheduler (crontab/solar/interval), store persistente
    celery-py/                PyO3 → modulo top-level `_celery_rs` (installabile anche accanto a
                              Celery originale per i test differenziali; il guscio lo ri-esporta)
  python/celery/              guscio Python (struttura identica a celery/celery)
  tools/compat/               tool di conformità (§Test)
  bench/                      benchmark memoria/throughput vs riferimento
  vendor/celery-ref/ -> ../celery  sorgente di riferimento (sola lettura)
```

## Ottimizzazioni (obiettivi misurabili)

| Area | Tecnica | Target vs Celery 5.7 |
|---|---|---|
| Master worker | Python + core Rust (niente kombu/billiard/asynpool), tokio, mimalloc | PSS master < 12 MB (vs 29 MB) |
| Figli | fork da zygote + `gc.freeze()` → copy-on-write massimo; `max_memory_per_child` gestito dal master | RSS privato/figlio −40% |
| Messaggi | `bytes::Bytes` zero-copy dal socket al pipe del figlio; il master non decodifica il body | 0 alloc per body nel master |
| Header | parse lazy/borrowed (`serde` con `&str`), nomi task internati (`Arc<str>`), `compact_str` | |
| JSON | `sonic-rs`/`simd-json` per encode/decode nel client e nel backend | encode ≥ 3× più veloce |
| IPC pool | frame length-prefixed su pipe, buffer riusati, nessun pickle per il trasporto | |
| Strutture | `revoked`/`active` con `hashbrown` + LRU limitati, timer a heap, `SmallVec` | |
| Eventi | batch + coalescenza heartbeat, `events.state` compattato | |
| Build | LTO fat, `opt-level=3`, `panic=abort`, target-cpu configurabile | |

Ogni ottimizzazione entra solo se il tool di conformità resta verde e il bench la conferma.

## Tool di conformità — `tools/compat` (`celery-compat`)

Due virtualenv: `venv-ref` (Celery originale da `celery/`) e `venv-rs` (nostro pacchetto).
Infrastruttura via docker su porte dedicate: Redis `:6390`, RabbitMQ `:5673`.

1. **`compat api`** – diff della superficie pubblica: per ogni modulo pubblico enumera nomi, tipi,
   firme (`inspect.signature`), MRO, costanti (`states`, `defaults` con valori), eccezioni e
   gerarchia. Output JSON + report delle differenze. Gate: 0 differenze sui moduli in scope.
2. **`compat wire`** – cattura dei messaggi grezzi sul broker (Redis `MONITOR`/lettura liste, AMQP
   consumer su coda di cattura) per uno scenario eseguito da ref e da rs; normalizza id/timestamp/
   hostname; diff di properties, headers, body. Copre apply_async con tutte le opzioni, canvas,
   eventi, control/broadcast, result nel backend.
3. **`compat matrix`** – scenari end-to-end su {client ref, client rs} × {worker ref, worker rs} ×
   {redis, amqp, memory}: delay/get, retry, eccezioni, countdown/eta/expires, revoke/terminate,
   time limit, rate limit, chain/group/chord/starmap/chunks, inspect/control, beat.
4. **`compat upstream`** – esegue `celery/t/unit` e `celery/t/integration` contro `venv-rs`;
   traccia pass-rate per file, con lista esplicita dei test esclusi (test di dettagli interni
   kombu/billiard) motivati uno per uno.
5. **`compat bench`** – RSS idle/carico (master+figli, PSS via `/proc/*/smaps_rollup`), throughput
   task/s, latenza p50/p99, a parità di config, ref vs rs.

## Fasi

| # | Fase | Contenuto | Gate di uscita |
|---|---|---|---|
| 0 ✅ | Setup | workspace, venv ref/rs, docker redis+rabbit, scheletro `tools/compat`, `compat api` funzionante, baseline bench del riferimento | baseline salvata |
| 1 ✅ | Protocollo | `celery-proto`: JSON compatibile CPython, compressione; `_celery_rs`: `json_dumps/loads`, `saferepr`, `encode_task_v2` | test differenziali byte-identici vs riferimento (`crates/celery-py/tests`) |
| 2 ✅ | Trasporti | 02a API+memory, poi 02b Redis e 02c AMQP in parallelo (worktree separati) | interop con kombu + `compat wire` sui comandi |
| 3 ✅ | Client Python | guscio `celery`: app, conf/defaults, Task, registry, routing, amqp producer, signature/canvas, AsyncResult/GroupResult | `compat api` verde; unit `t/unit/app`, `tasks`, `test_canvas` |
| 4 ✅ | Result backend | redis/rpc/memory, chord join/counter, get/wait/propagate | matrix: client rs + worker ref verde |
| 5 ✅ | Worker | master Python + core Rust (consumer, rsloop su epoll, zygote+pool), trace, ack late, retry, time limit, revoke, eventi, control, autoscale, shutdown/warm/cold, segnali | matrix completa verde |
| 6 ✅ | Beat | scheduler, crontab/solar, persistenza | `test_beat` + `t/unit/app/test_schedules` |
| 7 ✅ | CLI | `celery worker/beat/call/inspect/control/purge/result/events/status/multi` | output e exit code identici |
| 8 ✅ | Ottimizzazione | **Priorità: RAM di worker (prefork c10) e beat vs originale, poi prestazioni.** 8a ✅ pidbox/mingle/gossip sul trasporto Rust; 8b ✅ dieta della memoria con oggetti kombu veri (client 27 MB vs 35, beat 32 MB vs 43–51, mimalloc scartato); 8d ✅ worker c10 (PSS idle 87 MB vs 309, figlio 6,3 MB vs 29; precarico cffi nello zygote, una copia IPC in meno); 8e ✅ gate in parallelo su 4 corsie di broker (`tools/dev/gates`, giro completo 9,5 min invece di ~60); 8f ✅ RAM di beat (crontab compatti in Rust, una sola copia della schedule: 2,7 KB/voce, 10.000 voci 54 MB vs 118 ref, invii/s +50%; aperti: base 27 MB vs obiettivo 24, `-B`); 8c ✅ latenze senza aumentare la RAM (rpc p50 2,80 vs 3,52 ms ref, ping AMQP 8,49 vs 8,92 ms; capacità c10 con `--prefill`: Redis ×3,2, AMQP ×1,9; app realistica c10 153 MB vs ~313); aperti: ack Redis sincroni; `get_type_hints(worker_direct)` | target tabella §Ottimizzazioni |
| 9 ✅ | Hardening | 9a ✅ `t/integration` completa identica al riferimento (`./compat integration`, Redis e AMQP, 0 regressioni; corretti revoche, TTL Redis, pubsub, proprietà AMQP, fd di logging chiusi dallo zygote che corrompevano socket Redis); 9b ✅ limiti del parser (profondità JSON, decompressione, dati dopo l'envelope) + fuzz proptest; fork safety dei distruttori Rust (segfault billiard non più riprodotto); memoria Redis nei figli (errori precaricati nello zygote, rilascio pagine glibc dopo payload ≥1 MB, ultimo job liberato): worker c10 dopo 1 h sotto carico 286 MB vs 378 ref (Redis), 284 vs 422 (AMQP); 9c ✅ crescita nel tempo pari al riferimento: cicli dei traceback (`ExceptionInfo`) liberati dopo READY; soak 60 min Redis c10 190→220 MB (+30, 1110 cicli) vs ref 333→365 (+32, 890 cicli), dopo il riavvio rs ~12 MB/h vs ref ~30; aperto: salto al riavvio del broker +14 MB vs +7 ref | `t/integration` completa |

## Metodo di lavoro (orchestrazione)

- **Io (Claude)**: piano, specifiche per modulo, tool di conformità, review del codice, esecuzione
  test/bench, decisioni architetturali.
- **DeepSeek v4.1 flash** (`opencode run -m opencode-go/deepseek-v4.1-flash --auto`) implementa un
  task alla volta partendo da una spec in `specs/NN-nome.md` (riferimenti esatti ai file Celery da
  replicare, criteri di accettazione, comandi di test).
- Ciclo per task: spec → implementazione DeepSeek → `cargo test` + `celery-compat` → review →
  fix (rimandati a DeepSeek con l'output dei test) → merge su git con commit per task.
- Nessun task è "fatto" finché il gate della sua fase non è verde.

## Rischi principali

- **Test upstream che mockano internals** (kombu/billiard): alcuni non sono significativi per noi;
  ogni esclusione va documentata in `tools/compat/excluded.toml`.
- **pickle**: serve un interprete; il master lo tratta come body opaco, decodifica solo nei figli/client.
- **Sottoclassi utente di internals** (custom Consumer bootsteps, custom pool): supportate solo
  dove l'API è documentata; i bootstep del Consumer in Python sono v2.
- **Dimensione**: ~47k righe di riferimento; la priorità è il percorso comune (fasi 1–5).

## Baseline del riferimento (Fase 0, 2026-10-23)

Celery 5.7.0rc1, Python 3.13.9, pool prefork `-c 4`, 20.000 task `noop` (`bench/bench.py`,
risultati in `bench/results/`). Memoria = PSS sommata su master + figli.

| | Redis | RabbitMQ |
|---|---|---|
| PSS worker a riposo | 146 MB | 145 MB |
| PSS picco sotto carico | 151 MB | 149 MB |
| Client: publish/s | 1.720 | 2.915 |
| End-to-end task/s | 902 | 2.878 |
| Latenza `delay().get()` p50 / p99 | 3,05 / 3,96 ms | 2,95 / 3,83 ms |

Test suite unit del riferimento: 31.691 test; 3 fallimenti noti anche nel riferimento
(`tools/compat/out/upstream-unit-venv-ref.json`), che `compat upstream` non conta come regressioni.
Superficie API in scope: 137 moduli, 750 nomi pubblici, 207 impostazioni, 34 segnali, 38 comandi CLI.

## Fase 10 — RAM oltre il piano iniziale (priorità del proprietario: meno RAM possibile)
- 10a ✅ salto al riavvio del broker (Redis +44 → +2,2 MB, AMQP +3,7 MB, 0 task falliti: reset dei socket backend chiusi); master c10 22 → 18,3 MB (asynpool/billiard pigri, `TaskPool.Pool/BlockingPool` tra le differenze accettate di `compat api`); build delle dipendenze `opt-level=s` con crontab a 3 (p50 2,01 ms vs 2,48 ref). Aperti: master ≤ 13 MB, beat ≤ 22 MB (heap Python e moduli di base).
- 10b ✅ RAM dei figli sotto carico: `prctl(PR_SET_PDEATHSIG)` nativo invece di cffi in ogni figlio, rilascio mirato delle pagine libere; figlio c10 pure 9,97 → 8,09 MB, c10 bench-toplevel 12,42 → 11,38 MB, c50 5,88 / 9,39 MB.
- 10c ✅ bilancio della RAM (`bench/ram_budget_probe.py`): figli senza riscansione di fd/logger (CoW preservato), rilascio pagine del master all'avvio; worker c10 78,1 → 73,3 MB a riposo, 86,0 → 81,4 sotto carico; master 17,7 → 14,2. Verifica indipendente `bench/ram_check.py`: PSS = calo di MemAvailable (rs 78,2 vs 77,6 MB/worker).
- 10d ✅ lo zygote chiude solo gli fd del runtime Rust (registro in `celery-worker/src/fds.rs`): file, socket e logger aperti dal codice utente restano ereditati come nel riferimento.
- Pubblicato su github.com/TomP321/celery-rs; wheel manylinux_2_28 x86_64 nel branch `wheels`.
- Aperto: `test_worker_pool.py::test_hard_time_limit_replaces_child[prefork]` (pool billiard dentro pytest con thread Rust) si è bloccato una volta e ha fallito 2 volte su ~30 con la macchina sotto il carico dei gate; 0/20 senza carico, su main e 10c.
- 11c ✅ oggetti immortali (PEP 683, `crates/celery-py/src/immortal.rs`) nel master prima dello zygote e nello zygote prima del freeze, esclusi tipi con finalizzatore e oggetti `_celery_rs`; solo CPython 3.12–3.14 con GIL. Python 3.13 c10 Redis: riposo 66,3 → 49,5 MB, picco sotto carico 73,1 → 53,7 (`bench/memprobe/loadpeak.py`); ref 352/353. `CELERY_RS_IMMORTALIZE=0` per disattivare.
- 11d ✅ RAM prima della velocità (pavimento = throughput del riferimento, 834 task/s Redis c10): `gc.collect()`+`gc.freeze()` nel master prima dello zygote, rilascio della memoria dopo ogni task. Python 3.11 c10: riposo 76,5 → 72,5, picco 88,9 → 84,3 MB; 3.13 invariato (±2 MB). Scartate con misure: pagine condivise tra fratelli, cache adattive spente, tracer pre-costruiti, riempimento pool pymalloc, lapin con buffer ridotti (`bench/results/11d-ram-over-speed.md`).
- Wheel 0.1.2 (x86_64, aarch64) e selettore 0.1.2 nel branch `wheels`.
- 12 ✅ initializer dei figli preparato nello zygote (`prepare_process_initializer`: `log.setup`, `finalize`, tracer condivisi; nel figlio restano segnali, hook del loader, `worker_process_init`). c10 Redis: 3.11 riposo 72,5 → 62,1, picco 84,3 → 78,6 MB; 3.13 51,2 → 46,5 e 56,2 → 52,1. Scartati con misure: `opt-level = "z"` (nessun guadagno), moduli superflui di kombu (~2 MB, richiederebbero di alterare kombu). Wheel e selettore 0.1.3.
- Aperto: `t/integration/test_backend_leak_in_fixture_teardown.py` fallisce con rs (`RsConnectionPool` non espone `_available_connections`/`_in_use_connections` di redis-py); prima mascherato dal timeout del riferimento a ~120 s. Non è una regressione di 11c/11d.
- Misure di riferimento: `bench/fleet.py` (8 GB: ref 27 worker c10, rs 89), `bench/results/scale-*` (c1–c100).
