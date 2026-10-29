# 02c — AMQP Transport 0.9.1

Phase 2 of `PLAN.md`, part c. General objective of the phase: async transports in Rust **interoperable** with kombu (a message published by Rust is consumed by identical kombu, and vice versa; the state on the broker is what kombu expects).

Prerequisite: 02a completed (API in `crates/celery-transport`). **Do not change the common API** (`message.rs`, `transport.rs`, `url.rs`, `routing.rs`): If you need a change, stop and explain it in the final message. **The contract to be respected is in the module doc of `crates/celery-transport/src/transport.rs`** (semantics of ack/reject/requeue, prefetch and `ConsumerControl` — `set_prefetch`, `add_queue`, `cancel_queue`, `cancel` —, `get`, passive declare, `ConnectionLost`). The memory transport (`src/memory.rs`) is the reference implementation of the contract: your Rust tests should cover the same cases as his. Your code goes into `crates/celery-transport/src/amqp/` plus the `amqp://`/`amqps://`/`pyamqp://` branch of `connect`. Another agent is doing Redis in parallel - don't touch `src/redis/`.

## References (read-only; `K=.venv-ref/lib/python3.13/site-packages/kombu`)

- `$K/transport/redis.py` — **the Redis data model specification**. In particular: `Channel._put`, `_put_fanout`, `_get`, `_brpop_start/_brpop_read`, `_q_for_pri`, `priority`, `_queue_bind`, `_new_queue`, `_get_queue_expire`, `_maybe_update_queues_expire`, `_refresh_bindings`, `sweep_bindings`, `REFRESH_BINDING`, `SWEEP_BINDINGS`, `_delete`, `_purge`, `_size`, `get_table`, `_do_restore_message`, `_restore`, `_get_publish_topic(s)`, `_subscribe`, `_handle_message`, `_receive_one`, `QoS.append/ack/reject/restore_visible/restore_by_tag`, `Mutex`, `PublishBatch`, `MultiChannelPoller` (round-robin queues, BRPOP timeout), `Transport` (URL options and `transport_options`: `visibility_timeout`, `sep`, `priority_steps`, `queue_order_strategy`, `fanout_prefix`, `fanout_patterns`, `global_keyprefix`, `unacked_key`, `unacked_index_key`, `unacked_mutex_key`, `unacked_mutex_expire`, `health_check_interval`, `socket_timeout`, …).
- `$K/transport/virtual/base.py` and `virtual/exchange.py` — message envelope (`Channel.prepare_message`, `encode_body`, `Message`), direct/topic/fanout virtual routing (`DirectExchange`, `TopicExchange`, `FanoutExchange`), `QoS` (prefetch, `can_consume`), delivery tag, `basic_publish`, `basic_get`, `basic_ack`, `basic_reject`, `basic_recover`.
- `$K/transport/memory.py` — in-memory transport.
- `$K/transport/pyamqp.py`, `$K/messaging.py`, `$K/entity.py` (`Exchange`, `Queue`, `queue_arguments`, `binding_arguments`, `delivery_mode`, `expires`, `message_ttl`, `max_priority`) and the `amqp` library (`.venv-ref/lib/python3.13/site-packages/amqp/serialization.py` for AMQP header tables).
- `celery/celery/app/amqp.py::send_task_message` — which exchange/routing key/declare does Celery use.


### AMQP 0.9.1

Use `lapin`. Declarations (exchange direct/topic/fanout/headers, queues with topics `x-max-priority`, `x-expires`, `x-message-ttl`, `x-queue-type` quorum), `basic.qos` prefetch, optional publisher confirms, properties (`content_type`, `content_encoding`, `delivery_mode`, `priority`, `correlation_id`, `reply_to`, `expiration`, `headers`). Header tables must be decoded by py-amqp into the same Python types that kombu produces (e.g. lists → list, `None` → None, integers → int, strings → str).


## Test (gate)

1. `cargo test -p celery-transport` (RabbitMQ over `CELERY_RS_TEST_AMQP`, default `amqp://guest:guest@127.0.0.1:5673//`).
2. `crates/celery-py/tests/test_transport_amqp.py` with `.venv-ref`:
   - Rust publish → kombu consumer and kombu publish → Rust consumer: body, headers (with Python types decoded by py-amqp), properties, delivery_info the same;
   - exchange direct/topic/fanout/headers, queues with `x-max-priority` and mixed priorities, `x-expires`, `x-message-ttl`, `x-queue-type: quorum`, broadcast to multiple consumers;
   - ack, reject with/without requeue, prefetch (`basic.qos`), publisher confirms;
   - reconnection: the broker closes the connection (management API, port 15673) and the Rust consumer resumes without losing confirmed messages.
3. Comparison with the firehose tracer (uses `tools/compat/celery_compat/wire.py::RabbitCapture` and `Normalizer`): the same publishes made by kombu and Rust produce identical records.
4. Informational benchmark publish/s and consume/s Rust vs kombu (report it).

## Acceptance criteria

```
cargo build --release && cargo clippy --all-targets -- -D warnings && cargo test
VIRTUAL_ENV=.venv-ref .venv-ref/bin/maturin develop --release -m crates/celery-py/Cargo.toml
.venv-ref/bin/python -m pytest crates/celery-py/tests -q
```
All green, including spec tests 01. No tests skipped to pass the suite.


## Out of scope

`celery/` Python shell, result backend, workers, SQS and other transports, Redis Sentinel/Cluster (leaves an extension point), dynamic credentials/reauth.
