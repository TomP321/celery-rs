# 05a — Worker: consumer del broker e loop principale in Rust

Fase 5 di `PLAN.md` (architettura: master **Python + core Rust incorporato**). Il master del worker
resta il codice Python del riferimento (`WorkController`, bootsteps, `Consumer`, strategie,
`Request`, segnali): questa spec sostituisce **solo** la parte kombu/Hub del consumer — connessione
di lettura, consumer dei task, QoS/prefetch, ack/reject, `asynloop` — con il core Rust.
Traguardo: `celery worker -P solo` su Redis/AMQP completo e indistinguibile dal riferimento.
In parallelo un altro agente scrive il pool di processi Rust (spec 05b): **non toccare**
`celery/concurrency/` né `crates/celery-worker/src/pool*`. Il Waker condiviso esiste già:
`crates/celery-worker/src/wake.rs` e `_celery_rs.Waker` (non cambiarne l'interfaccia).

## Riferimenti
- `python/celery/worker/loops.py` (`asynloop`, `synloop`, heartbeat AMQP), `worker/consumer/consumer.py`
  (`Consumer`, `connect`, `connection_for_read`, `ensure_connected`, `on_connection_error_*`,
  `restart_count`, `on_ready`, `_restore_prefetch_count_after_connection_restart`,
  `_maybe_restore_prefetch`, `on_close`, `on_decode_error`, `on_unknown_message`,
  `on_unknown_task`, `on_invalid_task`, `create_task_handler`, `update_strategies`,
  `add_task_queue`/`cancel_task_queue`), `worker/consumer/tasks.py` (`Tasks`: `TaskConsumer`,
  `QoS`, `qos_global`), `worker/consumer/connection.py`, `worker/strategy.py`, `worker/request.py`
  (usa `message.headers/body/content_type/content_encoding/delivery_info/properties`,
  `message.ack/reject/requeue/ack_log_error/reject_log_error`, `message.delivery_tag`).
- kombu: `common.QoS` (`increment_eventually`, `decrement_eventually`, `set`, `update`),
  `Consumer`, `Message`, `Connection.drain_events`, `asynchronous.hub.Hub` (timer), `asynchronous.timer`.
- Già fatto: `crates/celery-transport` (Transport, ConsumerControl, ack/reject, prefetch,
  `consume_no_ack`), `crates/celery-py/src/transport.rs`, `python/celery/backends/_rs_rpc.py`
  (`RsMessage` compatibile kombu: riusalo spostandolo in un modulo comune, per esempio
  `python/celery/_rs_message.py`, senza cambiarne il comportamento per `rpc://`),
  `python/celery/app/_rs_producer.py` (`FastConnection`, `fast_connection`, fork, `stats()`).

## Progetto
1. **`crates/celery-worker/src/consumer.rs`**: consumer del worker sopra `celery-transport`, su
   un proprio task tokio: code multiple, prefetch dinamico (`set_prefetch(count, global)`),
   add/cancel coda a caldo, consegne in una coda limitata (dal prefetch) + `Waker::notify()`,
   ack/reject/requeue per delivery tag (anche da thread diversi dal loop), evento di
   connessione persa (una volta) per far scattare la riconnessione di Celery. Nessuna copia del
   body: `Bytes` fino a Python (`bytes` creati una sola volta). Test unitari + test su
   Redis/RabbitMQ reali (sotto `tools/dev/with-brokers`).
2. **Binding** `_celery_rs.WorkerConsumer` (`crates/celery-py/src/worker.rs`, accanto a `Waker`):
   `consume(queues)`, `add_queue`, `cancel_queue`, `set_prefetch`, `drain(max)` non bloccante
   (lista di consegne), `ack/reject/requeue(tag)`, `close`, accetta un `Waker`. GIL rilasciato
   per tutto l'I/O.
3. **Guscio Python** `python/celery/worker/_rs_consumer.py`:
   - `RsConnection`: il sottoinsieme di `kombu.Connection` usato da `Consumer` e dai bootsteps
     (`connection_errors`, `channel_errors`, `recoverable_*`, `transport.driver_type`,
     `drain_events(timeout)`, `heartbeat_check`, `get_heartbeat_interval`,
     `supports_heartbeats`, `ensure_connection`, `collect`, `close`/`release`, `as_uri`, `info`,
     `qos_semantics_matches_spec`, `default_channel`/`channel()` lazy su kombu solo se qualcuno li
     chiede — vedi punto 5);
   - `RsTaskConsumer`: sottoinsieme di `kombu.Consumer` (`consume`, `cancel`, `add_queue`,
     `cancel_by_queue`, `consuming_from`, `queues`, `qos(prefetch_count, apply_global)`,
     `on_message`, `on_decode_error`, `accept`), restituito da `app.amqp.TaskConsumer` quando la
     connessione è `RsConnection`;
   - messaggi: `RsMessage` comune (punto Riferimenti) con `ack/reject/requeue/ack_log_error/
     reject_log_error`, `acknowledged`, `delivery_tag`, `payload`/`decode()` lazy;
   - `rsloop(obj, connection, consumer, blueprint, hub, qos, heartbeat, clock, hbrate)`: stessa
     semantica di `asynloop` (ordine di `on_ready`, `did_start_ok`, `maybe_shutdown`,
     `qos.update`, errori di heartbeat, uscita su `blueprint.state`), ma il loop dorme in
     `Waker.wait(timeout)` con timeout = prossima scadenza del `hub.timer` (timer kombu Python:
     ETA, countdown, heartbeat degli eventi restano lì) e poi svuota le consegne chiamando
     `consumer.on_message(message)`. Altri fd registrati sull'hub (se presenti) devono continuare
     a funzionare: documenta come li gestisci.
4. **Attivazione**: il consumer Rust si usa quando la connessione di lettura è idonea (stesse
   regole di `use_fast_producer`: redis/amqp, niente failover/opzioni sconosciute), il pool è
   `solo` (in questa spec; il pool Rust arriverà con 05b/05c) e `CELERY_RS_WORKER` non è `0`.
   Altrimenti tutto resta come nel riferimento. Il punto di scelta deve rispettare i mock dei
   test upstream (`t/unit/worker`): `Consumer.loop`, `connection_for_read`, `amqp.TaskConsumer`.
5. **Fuori dal percorso Rust in questa spec** (restano kombu, su una connessione kombu lazy aperta
   solo se servono): remote control/pidbox, mingle, gossip, eventi in lettura. Devono continuare
   a funzionare con il worker Rust: `celery inspect ping/active/stats`, `control
   add_consumer/cancel_consumer/rate_limit/revoke` contro un worker `-P solo` rs. Se il pidbox
   kombu ha bisogno di essere drenato, integralo nel loop (per esempio drenando la sua
   connessione kombu con timeout 0 a ogni giro, o registrando il suo fd) e documentalo.
   Gli **eventi del worker** (`worker-online/heartbeat/offline`, `task-received/started/
   succeeded/failed/...` con `-E`) devono uscire dal producer Rust (`RsProducer`/`FastConnection`).
6. Riconnessione: connessione persa → stesso percorso del riferimento (`on_connection_error_after_connected`,
   `restart_count`, messaggi non confermati riconsegnati dal broker, prefetch ripristinato),
   stessi log. Shutdown warm (SIGTERM), cold (SIGQUIT), soft shutdown
   (`worker_soft_shutdown_timeout`): come il riferimento.
7. Niente modifiche a firme, nomi o default pubblici; nessuna nuova impostazione in `app.conf`.

## Gate (tutti obbligatori, esito reale nel messaggio finale)
```
cargo build --release && cargo clippy --all-targets -- -D warnings
tools/dev/with-brokers cargo test
tools/dev/build-package && tools/dev/build-native
tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q
tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q
./compat api && ./compat wire && ./compat upstream --venv .venv-rs   # 0 / 0 / 0 regressioni
```
Inoltre `python/tests_rs/test_worker_consumer.py` (`.venv-rs`), worker **rs** `celery worker -P solo`
avviato come sottoprocesso, client del **riferimento** (`.venv-ref`) e viceversa client rs:
- il worker usa davvero il consumer Rust (flag/contatore esposto, per esempio in `inspect stats`
  o in un file di stato del test) su Redis e AMQP;
- task semplici, args/kwargs grandi, retry (`self.retry` con countdown), ETA/countdown, `expires`,
  `acks_late` + `reject_on_worker_lost`, `task_acks_on_failure_or_timeout`, priorità (Redis
  priority steps, AMQP `x-max-priority`), rate limit, revoke prima dell'esecuzione, routing su
  più code e `-Q`, `add_consumer`/`cancel_consumer` a caldo, `inspect ping/active/reserved/
  scheduled/stats/registered`, eventi con `-E` catturati da un `EventReceiver` del riferimento
  (stessi campi/tipi del worker del riferimento), prefetch effettivo
  (`worker_prefetch_multiplier`, verificato con code piene), broker killato durante il lavoro
  (`CLIENT KILL`, DELETE connessione via management API) → riconnessione e nessun task perso,
  shutdown warm/cold con task in corso (stessi esiti di ack/requeue del riferimento),
  `CELERY_RS_WORKER=0` torna al riferimento;
- confronto ref vs rs dello stesso scenario: stessi messaggi su broker (ack/requeue), stessi
  risultati nel backend, stessi eventi.
- Benchmark: `bench/bench.py` con `-P solo` per `.venv-ref` e `.venv-rs` (Redis e AMQP):
  throughput end-to-end, latenza p50/p99, PSS del worker idle e di picco. Riporta i numeri.

## Fuori scope
Pool di processi (05b), pidbox/mingle/gossip in Rust, beat, autoscale, `-P threads/eventlet/gevent`.
