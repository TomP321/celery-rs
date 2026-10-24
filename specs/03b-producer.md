# 03b — Pubblicazione dei task in Rust (Producer compatibile con kombu)

Fase 3 di `PLAN.md`. `python/celery/` è oggi una copia del sorgente di Celery 5.7.0rc1 (fase 3a):
i gate sono tutti verdi e **devono restarlo**. Questa spec sostituisce il percorso caldo di
pubblicazione dei task (`apply_async` → `send_task` → `amqp.send_task_message` → `producer.publish`)
con il core Rust: serializzazione `_celery_rs.json_dumps`, compressione `_celery_rs.compress`,
`saferepr` nativo, publish tramite `celery-transport` (Redis/AMQP), senza cambiare né l'API pubblica
né il traffico sul broker.

## Riferimenti
- `python/celery/app/base.py`: `send_task`, `producer_or_acquire`, `_acquire_producer`,
  `producer_pool`, `connection_for_write`, `_after_fork`/`on_after_fork` (riga ~1470), `pool`.
- `python/celery/app/amqp.py`: `as_task_v2`, `_create_task_sender`/`send_task_message`,
  `producer_pool`, `default_exchange`, `queues`, `Queues`.
- `python/celery/events/dispatcher.py::EventDispatcher.publish` (evento `task-sent` pubblicato con
  il producer dei task e un `kombu.Exchange`).
- `python/celery/backends/redis.py::on_task_call`, `python/celery/backends/rpc.py::on_task_call`
  (usa `producer.channel` di kombu).
- kombu: `messaging.py::Producer` (API completa: `publish`, `declare`, `maybe_declare`, `revive`,
  `channel`, `connection`, `exchange`, `routing_key`, `serializer`, `compression`, `auto_declare`,
  `on_return`, `__enter__/__exit__`, `release`), `common.py::maybe_declare` (cache delle
  dichiarazioni per connessione), `connection.py::Connection.ensure`/`ensure_connection`
  (retry policy), `pools.py::ProducerPool`.
- `crates/celery-py/src/transport.rs` (`_celery_rs.Connection`), `crates/celery-transport`.

## Progetto
1. **`RsProducer`** (Python, `python/celery/app/_rs_producer.py`) con la stessa interfaccia di
   `kombu.Producer` usata da Celery e dagli utenti. `.connection` è il normale `kombu.Connection`
   lazy (nessun socket finché non serve; usato per metadati come
   `transport.driver_type` e da chi accede a `.channel`), `.channel` crea il canale kombu solo se
   qualcuno lo richiede (es. backend `rpc://`). `publish(...)` accetta gli stessi argomenti di
   `kombu.Producer.publish` (exchange come stringa o `kombu.Exchange`, `declare` con entità kombu
   da convertire in dichiarazioni Rust con cache come `maybe_declare`, `retry`/`retry_policy`,
   `headers`, `priority`, `expiration`, `delivery_mode`, `serializer`, `compression`,
   `content_type`/`content_encoding` già impostati, `**properties`), produce lo stesso messaggio
   e lo pubblica con `_celery_rs.Connection`. Serializer `json` nativo (con il `default` di kombu);
   altri serializer via `kombu.serialization.dumps`.
2. **Pool**: `AMQP.producer_pool` restituisce un pool di `RsProducer` con la stessa interfaccia di
   `kombu.pools.ProducerPool` (`acquire(block, timeout)`, `release`, `limit`, `connections`,
   `LimitExceeded` come kombu) quando il percorso veloce è attivo; altrimenti il pool kombu
   originale. Una `_celery_rs.Connection` per processo per URL, thread-safe (il GIL è rilasciato
   durante l'I/O).
3. **Quando il percorso veloce è attivo**: trasporto del broker `redis`/`rediss`/`amqp`/`amqps`/
   `pyamqp` e nessuna opzione non supportata (failover su più URL, `broker_use_ssl` con opzioni
   non mappate, transport options sconosciute al trasporto Rust). `memory://` resta su kombu fino
   al worker Rust (fase 5): il worker attuale è kombu e non vedrebbe lo stato del trasporto memory
   Rust. Per disattivarlo si usa la variabile d'ambiente `CELERY_RS_PRODUCER=0` e non un'opzione
   di `app.conf`: una nuova impostazione cambierebbe la superficie verificata da `compat api`.
4. **Retry**: stessa semantica di `kombu.Connection.ensure` con la `retry_policy` di Celery
   (`max_retries`, `interval_start`, `interval_step`, `interval_max`, errback), stesse eccezioni
   finali (`kombu.exceptions.OperationalError`) e stessi messaggi.
5. **Fork safety**: dopo `fork()` (pool prefork, `multiprocessing`) il figlio non deve usare socket
   o runtime tokio del padre. Usa `os.register_at_fork(after_in_child=...)` e il meccanismo
   `app._after_fork` già esistente per scartare connessioni/pool Rust nel figlio; nel codice Rust
   il runtime va ricreato lazy nel nuovo processo. Test con `os.fork()` e con pool prefork reale.
6. **`saferepr`**: in `as_task_v2` usa `_celery_rs.saferepr` (identico, già verificato).
   `celery.utils.saferepr` resta il modulo Python originale (i suoi test lo esercitano).
7. Niente modifiche a firme, nomi o default pubblici.

## Gate (tutti obbligatori, esito reale nel messaggio finale)
```
cargo build --release && cargo clippy --all-targets -- -D warnings && cargo test
env -u CONDA_PREFIX VIRTUAL_ENV=$PWD/.venv-rs .venv-rs/bin/maturin develop --release
VIRTUAL_ENV=.venv-ref .venv-ref/bin/maturin develop --release -m crates/celery-py/Cargo.toml
.venv-ref/bin/python -m pytest crates/celery-py/tests -q
./compat api                      # 0 differenze
./compat wire                     # 0 scenari diversi (ora il venv-rs pubblica con Rust!)
./compat upstream --venv .venv-rs # 0 regressioni
```
Inoltre:
- `python/tests_rs/test_producer.py` (eseguito con `.venv-rs`): conferma che negli scenari di
  `compat wire` il publish passa davvero dal percorso Rust (es. contatore/flag interno), test di
  retry con broker irraggiungibile (stessa eccezione e stesso numero di tentativi di kombu), fork
  safety, publish concorrente da 16 thread, `CELERY_RS_PRODUCER=0` torna a kombu.
- Benchmark: `bench/bench.py --venv .venv-rs` con broker Redis e AMQP. Riporta
  `client_publish_per_s` e `client_maxrss_kb` contro la baseline in `PLAN.md` (Redis 1.720/s,
  AMQP 2.915/s). Il worker usato dal bench è ancora quello Python: è atteso.

## Fuori scope
Result backend, worker, eventi diversi da `task-sent`, control/broadcast (`app.control` resta su
kombu), `memory://`.
