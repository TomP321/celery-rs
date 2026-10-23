# 02c — Trasporto AMQP 0.9.1

Fase 2 di `PLAN.md`, parte c. Obiettivo generale della fase: trasporti async in Rust
**interoperabili** con kombu (un messaggio pubblicato da Rust è consumato da kombu identico, e
viceversa; lo stato sul broker è quello che kombu si aspetta).

Prerequisito: 02a completata (API in `crates/celery-transport`). **Non modificare l'API comune**
(`message.rs`, `transport.rs`, `url.rs`, `routing.rs`): se ti serve un cambiamento, fermati e
spiegalo nel messaggio finale.
**Il contratto da rispettare è nel doc di modulo di `crates/celery-transport/src/transport.rs`**
(semantica di ack/reject/requeue, prefetch e `ConsumerControl` — `set_prefetch`, `add_queue`,
`cancel_queue`, `cancel` —, `get`, passive declare, `ConnectionLost`). Il trasporto memory
(`src/memory.rs`) è l'implementazione di riferimento del contratto: i tuoi test Rust devono
coprire gli stessi casi dei suoi. Il tuo codice va in `crates/celery-transport/src/amqp/` più il ramo
`amqp://`/`amqps://`/`pyamqp://` di `connect`. Un altro agente sta facendo Redis in parallelo: non
toccare `src/redis/`.

## Riferimenti (sola lettura; `K=.venv-ref/lib/python3.13/site-packages/kombu`)
- `$K/transport/redis.py` — **la specifica del modello dati Redis**. In particolare:
  `Channel._put`, `_put_fanout`, `_get`, `_brpop_start/_brpop_read`, `_q_for_pri`, `priority`,
  `_queue_bind`, `_new_queue`, `_get_queue_expire`, `_maybe_update_queues_expire`,
  `_refresh_bindings`, `sweep_bindings`, `REFRESH_BINDING`, `SWEEP_BINDINGS`, `_delete`, `_purge`,
  `_size`, `get_table`, `_do_restore_message`, `_restore`, `_get_publish_topic(s)`,
  `_subscribe`, `_handle_message`, `_receive_one`, `QoS.append/ack/reject/restore_visible/restore_by_tag`,
  `Mutex`, `PublishBatch`, `MultiChannelPoller` (round-robin delle code, timeout BRPOP),
  `Transport` (opzioni URL e `transport_options`: `visibility_timeout`, `sep`, `priority_steps`,
  `queue_order_strategy`, `fanout_prefix`, `fanout_patterns`, `global_keyprefix`,
  `unacked_key`, `unacked_index_key`, `unacked_mutex_key`, `unacked_mutex_expire`,
  `health_check_interval`, `socket_timeout`, …).
- `$K/transport/virtual/base.py` e `virtual/exchange.py` — envelope del messaggio
  (`Channel.prepare_message`, `encode_body`, `Message`), routing virtuale direct/topic/fanout
  (`DirectExchange`, `TopicExchange`, `FanoutExchange`), `QoS` (prefetch, `can_consume`),
  delivery tag, `basic_publish`, `basic_get`, `basic_ack`, `basic_reject`, `basic_recover`.
- `$K/transport/memory.py` — trasporto in-memory.
- `$K/transport/pyamqp.py`, `$K/messaging.py`, `$K/entity.py` (`Exchange`, `Queue`,
  `queue_arguments`, `binding_arguments`, `delivery_mode`, `expires`, `message_ttl`, `max_priority`)
  e la libreria `amqp` (`.venv-ref/lib/python3.13/site-packages/amqp/serialization.py` per le
  tabelle di header AMQP).
- `celery/celery/app/amqp.py::send_task_message` — quali exchange/routing key/declare usa Celery.


### AMQP 0.9.1
Usa `lapin`. Dichiarazioni (exchange direct/topic/fanout/headers, code con argomenti
`x-max-priority`, `x-expires`, `x-message-ttl`, `x-queue-type` quorum), `basic.qos` prefetch,
publisher confirms opzionali, proprietà (`content_type`, `content_encoding`, `delivery_mode`,
`priority`, `correlation_id`, `reply_to`, `expiration`, `headers`). Le tabelle di header devono
essere decodificate da py-amqp negli stessi tipi Python che produce kombu (es. liste → list,
`None` → None, interi → int, stringhe → str).


## Test (gate)
1. `cargo test -p celery-transport` (RabbitMQ su `CELERY_RS_TEST_AMQP`, default
   `amqp://guest:guest@127.0.0.1:5673//`).
2. `crates/celery-py/tests/test_transport_amqp.py` con `.venv-ref`:
   - Rust publish → kombu consumer e kombu publish → Rust consumer: body, headers (con i tipi
     Python decodificati da py-amqp), properties, delivery_info uguali;
   - exchange direct/topic/fanout/headers, code con `x-max-priority` e priorità miste, `x-expires`,
     `x-message-ttl`, `x-queue-type: quorum`, broadcast a più consumer;
   - ack, reject con/senza requeue, prefetch (`basic.qos`), publisher confirms;
   - riconnessione: il broker chiude la connessione (API di management, porta 15673) e il
     consumer Rust riprende senza perdere messaggi confermati.
3. Confronto con il firehose tracer (usa `tools/compat/celery_compat/wire.py::RabbitCapture` e
   `Normalizer`): gli stessi publish fatti da kombu e da Rust producono record identici.
4. Benchmark informativo publish/s e consume/s Rust vs kombu (riportalo).

## Criteri di accettazione
```
cargo build --release && cargo clippy --all-targets -- -D warnings && cargo test
VIRTUAL_ENV=.venv-ref .venv-ref/bin/maturin develop --release -m crates/celery-py/Cargo.toml
.venv-ref/bin/python -m pytest crates/celery-py/tests -q
```
Tutto verde, inclusi i test della spec 01. Nessun test saltato per far passare la suite.


## Fuori scope
Guscio Python `celery/`, result backend, worker, SQS e altri trasporti, Redis Sentinel/Cluster
(lascia un punto di estensione), credenziali dinamiche/reauth.

