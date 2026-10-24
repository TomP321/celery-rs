# 04b — Result backend `rpc://` sul trasporto Rust

Fase 4 di `PLAN.md`. Il backend `rpc://` pubblica i risultati con il producer dell'app (già Rust
dalla fase 3b) e li consuma lato client con un `kombu.Consumer` su una connessione kombu
separata. Questa spec porta sul core Rust **il lato client** (consumo delle risposte, `get_task_meta`
via basic.get, dichiarazione della coda di risposta) lasciando invariata la logica di
`python/celery/backends/rpc.py` e `asynchronous.py`. In parallelo un altro agente lavora sul backend
Redis (spec 04a): non toccare `backends/redis.py` né `backends/base.py`.

## Riferimenti
- `python/celery/backends/rpc.py`: `ResultConsumer` (`start`, `drain_events`, `_reconnect`,
  `reconnect_on_error`, `stop`, `on_after_fork`, `consume_from`, `cancel_for`, `on_state_change`),
  `RPCBackend` (`on_task_call` — oggi `producer.channel` apre un canale **kombu** anche con
  `RsProducer`: è da eliminare —, `destination_for`, `on_reply_declare`, `store_result`,
  `on_out_of_band_result`, `get_task_meta`, `_slurp_from_queue`, `_set_cache_by_message`,
  `binding`, `oid`, `_after_fork`).
- `python/celery/backends/asynchronous.py`: `BaseResultConsumer`, `Drainer` (default/eventlet/gevent),
  `AsyncBackendMixin` (`iter_native`, `wait_for_pending`, `_wait_for_pending`, `add_pending_result`).
- kombu: `Consumer` (`add_queue`, `consume`, `cancel_by_queue`, `consuming_from`, `queues`, `accept`,
  `no_ack`), `Message` (`payload`, `decode`, `properties`, `headers`, `delivery_info`,
  `content_type`, `ack`, `reject`, `requeue`), `Connection.drain_events(timeout)` → `socket.timeout`.
- `crates/celery-py/src/transport.rs` (`Connection.consume`/stream/`get`/`ack`, `declare_queue`),
  `python/celery/app/_rs_producer.py` (`FastConnection`, `fast_connection`, `RsProducer.maybe_declare`,
  fork, `stats()`).

## Progetto
1. **`RsResultConsumer`** (`python/celery/backends/_rs_rpc.py`), sottoclasse di
   `rpc.ResultConsumer` con la stessa interfaccia: usa un `_celery_rs.Connection` dedicato
   (non quello condiviso del producer: deve poter bloccare in `drain_events`), consumer su più
   code di risposta con `no_ack`, `add_queue`/`cancel_by_queue` senza ricreare il consumer
   (`ConsumerControl` del trasporto), `drain_events(timeout)` con la stessa eccezione di timeout
   di kombu (`socket.timeout`/`TimeoutError`) e GIL rilasciato. Per ogni consegna costruisce un
   oggetto messaggio compatibile con `kombu.Message` per gli attributi usati da Celery e dai
   callback utente (`payload` decodificato con `_celery_rs.json_loads` se JSON, altrimenti kombu;
   `accept` rispettato con la stessa `ContentDisallowed`; `properties['correlation_id']`,
   `headers`, `delivery_info`, `ack()`/`reject()` sul delivery tag Rust).
   `_reconnect` e `reconnect_on_error` mantengono la semantica (stessi log, ri-sottoscrizione delle
   code), con gli errori di connessione Rust nella tupla `_connection_errors`.
2. **`get_task_meta`/`_slurp_from_queue`**: basic.get sul trasporto Rust con lo stesso algoritmo
   (backlog_limit, `BacklogLimitExceeded`, ack/requeue come oggi, cache `_out_of_band`).
3. **`on_task_call`**: se il producer è un `RsProducer`, dichiara la coda di risposta con
   `producer.maybe_declare(..., retry=True)` (niente canale kombu); altrimenti codice originale.
   Verifica con un test che pubblicare un task con backend rpc **non apra** connessioni kombu
   (RabbitMQ management API: una sola connessione `lapin` per il producer + una per il consumer
   di risultati, nessuna `py-amqp`).
4. **Attivazione**: `RPCBackend.ResultConsumer` diventa `RsResultConsumer` quando la connessione
   dell'app è idonea al percorso veloce (`_rs_producer.fast_connection(app)` non `None`, solo
   AMQP: `rpc://` con broker Redis resta kombu in questa spec) e `CELERY_RS_BACKEND` non è `0`.
   I test upstream che sostituiscono `Consumer`/connessioni con mock devono restare sul ramo
   originale: decidi l'attivazione in un punto che quei mock rispettano (per esempio in `start`
   solo se `self.Consumer is kombu.Consumer` e `self.app.connection` non è stato sostituito).
5. Fork safety come 03b (`register_at_fork`, `on_after_fork`). Niente modifiche a firme, nomi
   o default pubblici; nessuna nuova impostazione in `app.conf`.
6. Il trasporto AMQP Rust: se serve, aggiungi in `celery-transport` ciò che manca (consumer
   `no_ack` multi-coda con add/cancel dinamici, basic.get) con test in `crates/celery-transport/tests/amqp.rs`.

## Gate (tutti obbligatori, esito reale nel messaggio finale)
```
cargo build --release && cargo clippy --all-targets -- -D warnings && cargo test
tools/dev/build-package
tools/dev/build-native
.venv-ref/bin/python -m pytest crates/celery-py/tests -q
.venv-rs/bin/python -m pytest python/tests_rs -q
./compat api                      # 0 differenze
./compat wire                     # 0 scenari diversi (include chord/group su rpc)
./compat upstream --venv .venv-rs # 0 regressioni
```
Inoltre `python/tests_rs/test_backend_rpc.py` (`.venv-rs`, RabbitMQ su 5673), con worker reale
del riferimento (`.venv-ref/bin/celery worker -P solo`) e client rs:
- il consumer usato è `RsResultConsumer` (contatore/flag) e nessuna connessione py-amqp è aperta;
- `AsyncResult.get(timeout)`, `propagate`, risultati di più task in volo, `ResultSet.join_native`,
  `iter_native`, group, stati intermedi (`update_state` PROGRESS → `on_message`), `forget`,
  `get_task_meta` con backlog (più messaggi per lo stesso task) e `BacklogLimitExceeded`,
  timeout → stessa `TimeoutError` di Celery;
- connessione chiusa via management API durante `get()` → riconnessione e risultato ricevuto;
- `os.fork()` e billiard: il figlio riceve i propri risultati;
- `CELERY_RS_BACKEND=0` torna al consumer kombu.
- Benchmark in `bench/bench_result.py` (se l'altro agente lo crea, aggiungi solo l'opzione
  `--backend rpc`; altrimenti crealo): latenza `apply_async().get()` p50/p99 e throughput di
  `join_native` su 1000 risultati, RSS client; `.venv-ref` vs `.venv-rs`.

## Fuori scope
Backend Redis (04a), `rpc://` su broker Redis, eventi, control/broadcast, il worker.
