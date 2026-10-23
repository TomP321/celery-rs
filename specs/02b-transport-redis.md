# 02b — Trasporto Redis

Fase 2 di `PLAN.md`, parte b. Obiettivo generale della fase: trasporti async in Rust
**interoperabili** con kombu (un messaggio pubblicato da Rust è consumato da kombu identico, e
viceversa; lo stato sul broker è quello che kombu si aspetta).

Prerequisito: 02a completata (API in `crates/celery-transport`). **Non modificare l'API comune**
(`message.rs`, `transport.rs`, `url.rs`, `routing.rs`): se ti serve un cambiamento, fermati e
spiegalo nel messaggio finale.
**Il contratto da rispettare è nel doc di modulo di `crates/celery-transport/src/transport.rs`**
(semantica di ack/reject/requeue, prefetch e `ConsumerControl` — `set_prefetch`, `add_queue`,
`cancel_queue`, `cancel` —, `get`, passive declare, `ConnectionLost`). Il trasporto memory
(`src/memory.rs`) è l'implementazione di riferimento del contratto: i tuoi test Rust devono
coprire gli stessi casi dei suoi. Il tuo codice va in `crates/celery-transport/src/redis/` più il ramo
`redis://`/`rediss://` di `connect`. Un altro agente sta facendo AMQP in parallelo: non toccare
`src/amqp/`.

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


### Redis
Replica **esattamente** il modello dati di kombu: liste per coda con suffissi di priorità
(`sep` = `\x06\x16`, `priority_steps` = `[0, 3, 6, 9]`, priorità invertita come in
`_get_message_priority(reverse=False)`), envelope JSON con le stesse chiavi e lo stesso ordine
(`body` base64 con `body_encoding`, `content-encoding`, `content-type`, `headers`, `properties`
con `delivery_info`, `delivery_tag` uuid4, …), insieme `_kombu.binding.<exchange>` con membri
`routing_key<sep>pattern<sep>queue` (+ set permanente e zset delle scadenze dove kombu li usa),
fanout via `PUBLISH` sui topic `/{db}.<exchange>` (con `fanout_prefix`/`fanout_patterns`),
hash `unacked` + zset `unacked_index` + mutex `unacked_mutex` con lo stesso script/semantica,
ripristino dei messaggi non confermati dopo `visibility_timeout` e alla chiusura,
`global_keyprefix`. Consumo con `BRPOP` su tutte le code (ordine round-robin come
`queue_order_strategy`) su una connessione dedicata, publish su un pool di connessioni.
Usa la crate `redis` (già in workspace) in modalità async con multiplexing per i publish.


## Test (gate)
1. `cargo test -p celery-transport` (Redis su `CELERY_RS_TEST_REDIS`, default `redis://127.0.0.1:6390`).
2. `crates/celery-py/tests/test_transport_redis.py` con `.venv-ref`:
   - Rust publish → kombu consumer e kombu publish → Rust consumer: body, headers, properties,
     delivery_info uguali a quelli di un messaggio pubblicato da kombu con gli stessi parametri;
   - priorità, code multiple, routing direct/topic/fanout, broadcast a più consumer;
   - ack, reject senza requeue, reject con requeue (torna in testa come kombu), prefetch;
   - dopo publish e dopo un consume senza ack lo **stato delle chiavi** (contenuto di liste, hash,
     set, zset, normalizzando delivery tag e timestamp) è identico tra Rust e kombu; un messaggio
     non confermato da Rust è ripristinato da un consumer kombu dopo `visibility_timeout` e viceversa;
   - sequenza dei comandi con `MONITOR` (usa `tools/compat/celery_compat/wire.py::RedisCapture` e
     `Normalizer`) tra Rust e kombu per declare+bind, publish semplice, con priorità, fanout.
     Differenze ammesse solo per comandi di gestione connessione e per pipeline/MULTI con semantica
     identica, ciascuna elencata nel test con il motivo.
3. Benchmark informativo publish/s e consume/s Rust vs kombu (riportalo).

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

