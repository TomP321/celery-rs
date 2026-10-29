# 03b — Publishing tasks in Rust (kombu-compatible producer)

Phase 3 of `PLAN.md`. `python/celery/` is today a copy of the source of Celery 5.7.0rc1 (phase 3a): the gates are all green and **must remain so**. This spec replaces the hot publish task path (`apply_async` → `send_task` → `amqp.send_task_message` → `producer.publish`) with the native Rust core: `_celery_rs.json_dumps` serialization, `_celery_rs.compress` compression, native `saferepr`, publish via `celery-transport` (Redis/AMQP), without changing either the public API or the traffic on the broker.

## References

- `python/celery/app/base.py`: `send_task`, `producer_or_acquire`, `_acquire_producer`, `producer_pool`, `connection_for_write`, `_after_fork`/`on_after_fork` (line ~1470), `pool`.
- `python/celery/app/amqp.py`: `as_task_v2`, `_create_task_sender`/`send_task_message`, `producer_pool`, `default_exchange`, `queues`, `Queues`.
- `python/celery/events/dispatcher.py::EventDispatcher.publish` (event `task-sent` published with the task producer and a `kombu.Exchange`).
- `python/celery/backends/redis.py::on_task_call`, `python/celery/backends/rpc.py::on_task_call` (use `producer.channel` from kombu).
- kombu: `messaging.py::Producer` (Full API: `publish`, `declare`, `maybe_declare`, `revive`, `channel`, `connection`, `exchange`, `routing_key`, `serializer`, `compression`, `auto_declare`, `on_return`, `__enter__/__exit__`, `release`), `common.py::maybe_declare` (connection declaration cache), `connection.py::Connection.ensure`/`ensure_connection` (retry policy), `pools.py::ProducerPool`.
- `crates/celery-py/src/transport.rs` (`_celery_rs.Connection`), `crates/celery-transport`.

## Project

1. **`RsProducer`** (Python, `python/celery/app/_rs_producer.py`) with the same interface as `kombu.Producer` used by Celery and users. `.connection` is the normal lazy `kombu.Connection` (no socket until needed; used for metadata like `transport.driver_type` and by those accessing `.channel`), `.channel` creates the kombu channel only if someone requests it (e.g. backend `rpc://`). `publish(...)` takes the same arguments as `kombu.Producer.publish` (exchange as string or `kombu.Exchange`, `declare` with kombu entities to convert to cached Rust declarations like `maybe_declare`, `retry`/`retry_policy`, `headers`, `priority`, `expiration`, `delivery_mode`, `serializer`, `compression`, `content_type`/`content_encoding` already set, `**properties`), produces the same message and publishes it with `_celery_rs.Connection`. Native `json` serializer (with kombu's `default`); other serializers via `kombu.serialization.dumps`.
2. **Pool**: `AMQP.producer_pool` returns a pool of `RsProducer` with the same interface as `kombu.pools.ProducerPool` (`acquire(block, timeout)`, `release`, `limit`, `connections`, `LimitExceeded` as kombu) when fast path is active; otherwise the original kombu pool. One `_celery_rs.Connection` per process per URL, thread safe (GIL is released during I/O).
3. **When fast path is active**: Broker transport `redis`/`rediss`/`amqp`/`amqps`/ `pyamqp` and no unsupported options (multi-URL failover, `broker_use_ssl` with unmapped options, transport options unknown to Rust transport). `memory://` stays on kombu until the Rust worker (phase 5): the current worker is kombu and would not see the state of the Rust memory transport. To disable it, use the `CELERY_RS_PRODUCER=0` environment variable and not an option of `app.conf`: a new setting would change the surface checked by `compat api`.
4. **Retry**: same semantics as `kombu.Connection.ensure` with Celery's `retry_policy` (`max_retries`, `interval_start`, `interval_step`, `interval_max`, errback), same final exceptions (`kombu.exceptions.OperationalError`) and same messages.
5. **Fork safety**: after `fork()` (prefork pool, `multiprocessing`) the child must not use the parent's socket or tokio runtime. Use `os.register_at_fork(after_in_child=...)` and existing `app._after_fork` mechanism to discard Rust connections/pools in child; in Rust code the runtime must be recreated lazy in the new process. Testing with `os.fork()` and with real prefork pool.
6. **`saferepr`**: in `as_task_v2` use `_celery_rs.saferepr` (identical, already verified). `celery.utils.saferepr` remains the original Python module (its tests exercise it).
7. No changes to signatures, names or public defaults.

## Gate (all mandatory, real outcome in final message)

```
cargo build --release && cargo clippy --all-targets -- -D warnings && cargo test
tools/dev/build-package        # full celery-rs package in .venv-rs
tools/dev/build-native         # only _celery_rs in .venv-ref (beside the reference)
.venv-ref/bin/python -m pytest crates/celery-py/tests -q
./compat api                      # 0 differences
./compat wire                     # 0 differing scenarios (venv-rs now publishes with Rust!)
./compat upstream --venv .venv-rs # 0 regressions
```
Furthermore:
- `python/tests_rs/test_producer.py` (executed with `.venv-rs`): confirms that in `compat wire` scenarios the publish really goes through the Rust path (e.g. internal counter/flag), retry test with unreachable broker (same exception and same number of kombu attempts), fork safety, concurrent publish from 16 thread, `CELERY_RS_PRODUCER=0` back to kombu.
- Benchmark: `bench/bench.py --venv .venv-rs` with Redis and AMQP brokers. Report `client_publish_per_s` and `client_maxrss_kb` against the baseline in `PLAN.md` (Redis 1.720/s, AMQP 2.915/s). The worker used by the bench is still the Python one: it is expected.

## Out of scope

Result backend, worker, events other than `task-sent`, control/broadcast (`app.control` remains on kombu), `memory://`.
