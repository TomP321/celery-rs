# 05a — Worker: Broker consumer and main loop in Rust

Phase 5 of `PLAN.md` (architecture: **Python master + embedded Rust core**). The master of the worker remains the Python code of the reference (`WorkController`, bootsteps, `Consumer`, strategies, `Request`, signals): this spec replaces **only** the kombu/Hub part of the consumer — read connection, task consumer, QoS/prefetch, ack/reject, `asynloop` — with the Rust core. Milestone: `celery worker -P solo` on Redis/AMQP complete and indistinguishable from the reference. In parallel another agent writes the Rust process pool (spec 05b): **do not touch** `celery/concurrency/` nor `crates/celery-worker/src/pool*`. The shared Waker already exists: `crates/celery-worker/src/wake.rs` and `_celery_rs.Waker` (do not change its interface).

## References

- `python/celery/worker/loops.py` (`asynloop`, `synloop`, AMQP heartbeat), `worker/consumer/consumer.py` (`Consumer`, `connect`, `connection_for_read`, `ensure_connected`, `on_connection_error_*`, `restart_count`, `on_ready`, `_restore_prefetch_count_after_connection_restart`, `_maybe_restore_prefetch`, `on_close`, `on_decode_error`, `on_unknown_message`, `on_unknown_task`, `on_invalid_task`, `create_task_handler`, `update_strategies`, `add_task_queue`/`cancel_task_queue`), `worker/consumer/tasks.py` (`Tasks`: `TaskConsumer`, `QoS`, `qos_global`), `worker/consumer/connection.py`, `worker/strategy.py`, `worker/request.py` (use `message.headers/body/content_type/content_encoding/delivery_info/properties`, `message.ack/reject/requeue/ack_log_error/reject_log_error`, `message.delivery_tag`).
- kombu: `common.QoS` (`increment_eventually`, `decrement_eventually`, `set`, `update`), `Consumer`, `Message`, `Connection.drain_events`, `asynchronous.hub.Hub` (timer), `asynchronous.timer`.
- Already done: `crates/celery-transport` (Transport, ConsumerControl, ack/reject, prefetch, `consume_no_ack`), `crates/celery-py/src/transport.rs`, `python/celery/backends/_rs_rpc.py` (kombu compatible `RsMessage`: reuse it by moving it to a common module, for example `python/celery/_rs_message.py`, without changing its behavior for `rpc://`), `python/celery/app/_rs_producer.py` (`FastConnection`, `fast_connection`, fork, `stats()`).

## Project

1. **`crates/celery-worker/src/consumer.rs`**: consumer of the worker above `celery-transport`, on its own tokio task: multiple queues, dynamic prefetch (`set_prefetch(count, global)`), add/cancel hot queue, deliveries in a limited queue (from prefetch) + `Waker::notify()`, ack/reject/requeue for delivery tag (also from thread other than loop), connection lost event (once) to trigger Celery reconnection. No body copy: `Bytes` up to Python (`bytes` created only once). Unit tests + tests on real Redis/RabbitMQ (under `tools/dev/with-brokers`).
2. **Binding** `_celery_rs.WorkerConsumer` (`crates/celery-py/src/worker.rs`, next to `Waker`): `consume(queues)`, `add_queue`, `cancel_queue`, `set_prefetch`, `drain(max)` non-blocking (delivery list), `ack/reject/requeue(tag)`, `close`, accept a `Waker`. GIL released for all I/O.
3. **Python shell** `python/celery/worker/_rs_consumer.py`:
   - `RsConnection`: the subset of `kombu.Connection` used by `Consumer` and the bootsteps (`connection_errors`, `channel_errors`, `recoverable_*`, `transport.driver_type`, `drain_events(timeout)`, `heartbeat_check`, `get_heartbeat_interval`, `supports_heartbeats`, `ensure_connection`, `collect`, `close`/`release`, `as_uri`, `info`, `qos_semantics_matches_spec`, `default_channel`/`channel()` lazy on kombu only if someone asks for them — see point 5);
   - `RsTaskConsumer`: subset of `kombu.Consumer` (`consume`, `cancel`, `add_queue`, `cancel_by_queue`, `consuming_from`, `queues`, `qos(prefetch_count, apply_global)`, `on_message`, `on_decode_error`, `accept`), returned by `app.amqp.TaskConsumer` when the connection is `RsConnection`;
   - messages: `RsMessage` common (Reference point) with `ack/reject/requeue/ack_log_error/
     reject_log_error`, `acknowledged`, `delivery_tag`, `payload`/`decode()` lazy;
   - `rsloop(obj, connection, consumer, blueprint, hub, qos, heartbeat, clock, hbrate)`: same semantics as `asynloop` (order of `on_ready`, `did_start_ok`, `maybe_shutdown`, `qos.update`, heartbeat errors, exit on `blueprint.state`), but the loop sleeps in `Waker.wait(timeout)` with timeout = next deadline of `hub.timer` (kombu Python timer: ETA, countdown, heartbeat of events remain there) and then clear the deliveries by calling `consumer.on_message(message)`. Other fds registered on the hub (if any) must continue to work - document how you manage them.
4. **Activation**: The Rust consumer is used when the read connection is eligible (same rules as `use_fast_producer`: redis/amqp, no failover/unknown options), the pool is `solo` (in this spec; the Rust pool will come with 05b/05c), and `CELERY_RS_WORKER` is not `0`. Otherwise everything remains as in the reference. The choice point must respect the upstream test mocks (`t/unit/worker`): `Consumer.loop`, `connection_for_read`, `amqp.TaskConsumer`.
5. **Off the Rust path in this spec** (they remain kombu, on a lazy kombu connection opened only if needed): remote control/pidbox, mingle, gossip, read events. They must continue to work with the Rust worker: `celery inspect ping/active/stats`, `control
   add_consumer/cancel_consumer/rate_limit/revoke` against a `-P solo` worker rs. If the kombu pidbox needs to be drained, integrate it into the loop (for example by draining its timeout kombu connection 0 on every spin, or logging its fd) and document it. **Worker events** (`worker-online/heartbeat/offline`, `task-received/started/
   succeeded/failed/...` with `-E`) must exit the Rust producer (`RsProducer`/`FastConnection`).
6. Reconnection: connection lost → same path as reference (`on_connection_error_after_connected`, `restart_count`, unconfirmed messages re-delivered by broker, prefetch restored), same logs. Shutdown warm (SIGTERM), cold (SIGQUIT), soft shutdown (`worker_soft_shutdown_timeout`): as the reference.
7. No changes to signatures, names or public defaults; no new settings in `app.conf`.

## Gate (all mandatory, real outcome in final message)

```
cargo build --release && cargo clippy --all-targets -- -D warnings
tools/dev/with-brokers cargo test
tools/dev/build-package && tools/dev/build-native
tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q
tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q
./compat api && ./compat wire && ./compat upstream --venv .venv-rs   # 0 / 0 / 0 regressions
```
Additionally `python/tests_rs/test_worker_consumer.py` (`.venv-rs`), worker **rs** `celery worker -P solo` started as a subprocess, **reference** client (`.venv-ref`) and vice versa rs client:
- the worker actually uses the Rust consumer (flag/counter exposed, for example in `inspect stats` or in a test state file) on Redis and AMQP;
- simple tasks, large args/kwargs, retry (`self.retry` with countdown), ETA/countdown, `expires`, `acks_late` + `reject_on_worker_lost`, `task_acks_on_failure_or_timeout`, priority (Redis priority steps, AMQP `x-max-priority`), rate limit, revoke before of execution, multi-queue routing and hot `-Q`, `add_consumer`/`cancel_consumer`, `inspect ping/active/reserved/
  scheduled/stats/registered`, events with `-E` captured by a `EventReceiver` of the reference (same fields/types as the reference worker), effective prefetch (`worker_prefetch_multiplier`, verified with full queues), broker killed during work (`CLIENT KILL`, DELETE connection via management API) → reconnection and no task lost, warm/cold shutdown with task in progress (same results as ack/requeue of the reference), `CELERY_RS_WORKER=0` returns to the reference;
- ref vs rs comparison of the same scenario: same messages on broker (ack/requeue), same results in the backend, same events.
- Benchmark: `bench/bench.py` with `-P solo` for `.venv-ref` and `.venv-rs` (Redis and AMQP): end-to-end throughput, p50/p99 latency, idle and peak worker PSS. Report the numbers.

## Out of scope

Process pool (05b), pidbox/mingle/gossip in Rust, beat, autoscale, `-P threads/eventlet/gevent`.
