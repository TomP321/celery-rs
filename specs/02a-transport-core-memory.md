# 02a — Trasporti: API comune + memory

Fase 2 di `PLAN.md`, parte a. Obiettivo generale della fase: trasporti async in Rust
**interoperabili** con kombu (un messaggio pubblicato da Rust è consumato da kombu identico, e
viceversa; lo stato sul broker è quello che kombu si aspetta).

Questa parte definisce l'API su cui 02b (Redis) e 02c (AMQP) verranno implementate **in parallelo**
da agenti diversi: deve essere completa e stabile, così le parti successive aggiungono solo un modulo
ciascuna (`src/redis/`, `src/amqp/`) senza modificare l'API comune.

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


## API Rust (crate `celery-transport`)
Progetta un'API piccola e a costo zero; indicativamente:
- `Message { body: Bytes, content_type, content_encoding, headers: Headers, properties: Properties }`
  dove `Headers` è una mappa **ordinata** (preserva l'ordine di inserimento) di `HeaderValue`
  (`Null, Bool, Int(i64), BigInt(String)?, Float(f64), Str, Bytes, List, Map, Timestamp, Decimal`).
  Niente `serde_json::Value`.
- `trait Transport` (async, tokio): `declare_exchange`, `declare_queue`, `bind`, `publish`,
  `publish_batch`, `consume(queues, prefetch) -> stream di Delivery`, `ack`, `reject(requeue)`,
  `purge`, `queue_size`, `delete_queue`, `close`. Broadcast/fanout inclusi.
- `Delivery { message, delivery_tag, exchange, routing_key, redelivered }`.
- Connessione da URL con la stessa sintassi di kombu (`redis://[:pass@]host:port/db`,
  `rediss://`, `amqp://user:pass@host:port/vhost`, `memory://`) e `transport_options`.
- Riconnessione con backoff (stessi default di `broker_connection_retry`/`max_retries` di Celery),
  esposta come politica configurabile.


Struttura richiesta del crate: `src/lib.rs` (solo `pub mod` e re-export), `src/message.rs`
(Message, Headers, HeaderValue, Properties, Delivery), `src/url.rs` (parsing URL e
transport_options per tutti e tre gli schemi), `src/transport.rs` (trait, errori, politica di
riconnessione), `src/routing.rs` (routing virtuale direct/topic/fanout come
`kombu/transport/virtual/exchange.py`, riusato da memory e Redis), `src/memory.rs`.
Registra i trasporti in un `connect(url)` che per `redis://`/`amqp://` restituisce per ora un errore
`NotImplemented` chiaro: 02b e 02c sostituiranno solo quel ramo.

### memory
Trasporto in-process (registro globale per virtual host) con la stessa semantica di
`kombu/transport/memory.py`: serve ai test e a `memory://` (client e worker nello stesso processo).


## Binding Python (in `crates/celery-py`, modulo `_celery_rs`)
Espone una classe `Connection(url, transport_options=None)` minimale con `publish(...)`,
`declare_*`, `bind`, `drain(queues, timeout) -> list of (body: bytes, headers, properties, delivery_info, delivery_tag)`,
`ack`, `reject`, `purge`, `size`, `close`. Usa un runtime tokio single-thread condiviso e rilascia
il GIL durante l'I/O. Serve ai test di interoperabilità e sarà usata dal guscio nella fase 3.


La classe Python `Connection` deve funzionare per qualunque trasporto restituito da `connect(url)`.

## Test (gate)
1. `cargo test -p celery-transport`: test unitari di message/url/routing (routing topic con `*`
   e `#` confrontato con una tabella di casi generata da kombu `TopicExchange.lookup`) e del
   trasporto memory (publish/consume/ack/reject/requeue/prefetch/fanout/priorità).
2. `crates/celery-py/tests/test_transport_memory.py` (con `.venv-ref`): stesse operazioni fatte con
   kombu `memory://` e con `_celery_rs.Connection('memory://')` producono gli stessi messaggi
   ricevuti (body, headers, properties, delivery_info) nello stesso ordine.
3. Parsing URL: tabella di ≥ 30 URL confrontata con `kombu.Connection(url).info()` (hostname, port,
   userid, password, virtual_host, transport).

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

