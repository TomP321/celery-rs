# 02b — Redis transport

Phase 2 of `PLAN.md`, part b. General objective of the phase: async transports in Rust **interoperable** with kombu (a message published by Rust is consumed by identical kombu, and vice versa; the state on the broker is what kombu expects).

Prerequisite: 02a completed (API in `crates/celery-transport`). **Do not change the common API** (`message.rs`, `transport.rs`, `url.rs`, `routing.rs`): If you need a change, stop and explain it in the final message. **The contract to be respected is in the module doc of `crates/celery-transport/src/transport.rs`** (semantics of ack/reject/requeue, prefetch and `ConsumerControl` — `set_prefetch`, `add_queue`, `cancel_queue`, `cancel` —, `get`, passive declare, `ConnectionLost`). The memory transport (`src/memory.rs`) is the reference implementation of the contract: your Rust tests should cover the same cases as his. Your code goes into `crates/celery-transport/src/redis/` plus the `redis://`/`rediss://` branch of `connect`. Another agent is doing AMQP in parallel: do not touch `src/amqp/`.

## References (read only; `K=.venv-ref/lib/python3.13/site-packages/kombu`)

- `$K/transport/redis.py` — **the Redis data model specification**. In particular: `Channel._put`, `_put_fanout`, `_get`, `_brpop_start/_brpop_read`, `_q_for_pri`, `priority`, `_queue_bind`, `_new_queue`, `_get_queue_expire`, `_maybe_update_queues_expire`, `_refresh_bindings`, `sweep_bindings`, `REFRESH_BINDING`, `SWEEP_BINDINGS`, `_delete`, `_purge`, `_size`, `get_table`, `_do_restore_message`, `_restore`, `_get_publish_topic(s)`, `_subscribe`, `_handle_message`, `_receive_one`, `QoS.append/ack/reject/restore_visible/restore_by_tag`, `Mutex`, `PublishBatch`, `MultiChannelPoller` (round-robin queues, BRPOP timeout), `Transport` (URL options and `transport_options`: `visibility_timeout`, `sep`, `priority_steps`, `queue_order_strategy`, `fanout_prefix`, `fanout_patterns`, `global_keyprefix`, `unacked_key`, `unacked_index_key`, `unacked_mutex_key`, `unacked_mutex_expire`, `health_check_interval`, `socket_timeout`, …).
- `$K/transport/virtual/base.py` and `virtual/exchange.py` — message envelope (`Channel.prepare_message`, `encode_body`, `Message`), direct/topic/fanout virtual routing (`DirectExchange`, `TopicExchange`, `FanoutExchange`), `QoS` (prefetch, `can_consume`), delivery tag, `basic_publish`, `basic_get`, `basic_ack`, `basic_reject`, `basic_recover`.
- `$K/transport/memory.py` — in-memory transport.
- `$K/transport/pyamqp.py`, `$K/messaging.py`, `$K/entity.py` (`Exchange`, `Queue`, `queue_arguments`, `binding_arguments`, `delivery_mode`, `expires`, `message_ttl`, `max_priority`) and the `amqp` library (`.venv-ref/lib/python3.13/site-packages/amqp/serialization.py` for AMQP header tables).
- `celery/celery/app/amqp.py::send_task_message` — which exchange/routing key/declare does Celery use.


### Redis

Replicates the kombu data model **exactly**: lists per queue with priority suffixes (`sep` = `\x06\x16`, `priority_steps` = `[0, 3, 6, 9]`, priority reversed as in `_get_message_priority(reverse=False)`), JSON envelope with the same keys and order (`body` base64 with `body_encoding`, `content-encoding`, `content-type`, `headers`, `properties` with `delivery_info`, `delivery_tag` uuid4, …), together `_kombu.binding.<exchange>` with members `routing_key<sep>pattern<sep>queue` (+ permanent set and deadline zset where kombu uses them), fanout via `PUBLISH` on topics `/{db}.<exchange>` (with `fanout_prefix`/`fanout_patterns`), hash `unacked` + zset `unacked_index` + mutex `unacked_mutex` with the same script/semantics, resetting unconfirmed messages after `visibility_timeout` and at closing, `global_keyprefix`. Consume with `BRPOP` on all queues (round-robin order like `queue_order_strategy`) on a dedicated connection, publish on a connection pool. Use the `redis` crate (already in workspace) in async mode with multiplexing for publish.


## Test (gate)

1. `cargo test -p celery-transport` (Redis on `CELERY_RS_TEST_REDIS`, default `redis://127.0.0.1:6390`).
2. `crates/celery-py/tests/test_transport_redis.py` with `.venv-ref`:
   - Rust publish → kombu consumer and kombu publish → Rust consumer: body, headers, properties, delivery_info equal to those of a message published by kombu with the same parameters;
   - priority, multiple queues, direct/topic/fanout routing, broadcast to multiple consumers;
   - ack, reject without requeue, reject with requeue (returns to head like kombu), prefetch;
   - after publish and after a consumption without ack the **key state** (content of lists, hash, set, zset, normalizing delivery tag and timestamp) is identical between Rust and kombu; an unconfirmed message from Rust is restored by a kombu consumer after `visibility_timeout` and vice versa;
- command sequence with `MONITOR` (use `tools/compat/celery_compat/wire.py::RedisCapture` and `Normalizer`) between Rust and kombu for declare+bind, simple publish, with priority, fanout. Differences allowed only for connection management and pipeline/MULTI commands with identical semantics, each listed in the test with the reason.
3. Informational benchmark publish/s and consume/s Rust vs kombu (report it).

## Acceptance criteria

```
cargo build --release && cargo clippy --all-targets -- -D warnings && cargo test
VIRTUAL_ENV=.venv-ref .venv-ref/bin/maturin develop --release -m crates/celery-py/Cargo.toml
.venv-ref/bin/python -m pytest crates/celery-py/tests -q
```
All green, including spec tests 01. No tests skipped to pass the suite.


## Out of scope

`celery/` Python shell, result backend, workers, SQS and other transports, Redis Sentinel/Cluster (leaves an extension point), dynamic credentials/reauth.
