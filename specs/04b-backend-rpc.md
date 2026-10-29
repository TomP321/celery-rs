# 04b — Result backend `rpc://` on Rust transport

Phase 4 of `PLAN.md`. The `rpc://` backend publishes the results with the app producer (formerly Rust from the 3b phase) and consumes them client-side with a `kombu.Consumer` on a separate kombu connection. This spec brings **the client side** to the Rust core (response consumption, `get_task_meta` via basic.get, response queue declaration) leaving the logic of `python/celery/backends/rpc.py` and `asynchronous.py` unchanged. In parallel another agent works on the Redis backend (spec 04a): do not touch `backends/redis.py` nor `backends/base.py`.

## References

- `python/celery/backends/rpc.py`: `ResultConsumer` (`start`, `drain_events`, `_reconnect`, `reconnect_on_error`, `stop`, `on_after_fork`, `consume_from`, `cancel_for`, `on_state_change`), `RPCBackend` (`on_task_call` — today `producer.channel` opens a **kombu** channel also with `RsProducer`: it must be eliminated —, `destination_for`, `on_reply_declare`, `store_result`, `on_out_of_band_result`, `get_task_meta`, `_slurp_from_queue`, `_set_cache_by_message`, `binding`, `oid`, `_after_fork`).
- `python/celery/backends/asynchronous.py`: `BaseResultConsumer`, `Drainer` (default/eventlet/gevent), `AsyncBackendMixin` (`iter_native`, `wait_for_pending`, `_wait_for_pending`, `add_pending_result`).
- kombu: `Consumer` (`add_queue`, `consume`, `cancel_by_queue`, `consuming_from`, `queues`, `accept`, `no_ack`), `Message` (`payload`, `decode`, `properties`, `headers`, `delivery_info`, `content_type`, `ack`, `reject`, `requeue`), `Connection.drain_events(timeout)` → `socket.timeout`.
- `crates/celery-py/src/transport.rs` (`Connection.consume`/stream/`get`/`ack`, `declare_queue`), `python/celery/app/_rs_producer.py` (`FastConnection`, `fast_connection`, `RsProducer.maybe_declare`, fork, `stats()`).

## Project

1. **`RsResultConsumer`** (`python/celery/backends/_rs_rpc.py`), subclass of `rpc.ResultConsumer` with the same interface: use a dedicated `_celery_rs.Connection` (not the shared one of the producer: it must be able to block in `drain_events`), consumer on multiple response queues with `no_ack`, `add_queue`/`cancel_by_queue` without recreating the consumer (transport `ConsumerControl`), `drain_events(timeout)` with the same kombu timeout exception (`socket.timeout`/`TimeoutError`), and GIL released. For each delivery it constructs a message object compatible with `kombu.Message` for the attributes used by Celery and user callbacks (`payload` decoded with `_celery_rs.json_loads` if JSON, otherwise kombu; `accept` respected with the same `ContentDisallowed`; `properties['correlation_id']`, `headers`, `delivery_info`, `ack()`/`reject()` on the Rust delivery tag). `_reconnect` and `reconnect_on_error` retain semantics (same logs, re-subscribing queues), with Rust connection errors in the `_connection_errors` tuple.
2. **`get_task_meta`/`_slurp_from_queue`**: basic.get on the Rust transport with the same algorithm (backlog_limit, `BacklogLimitExceeded`, ack/requeue as today, cache `_out_of_band`).
3. **`on_task_call`**: if the producer is a `RsProducer`, declare the response queue with `producer.maybe_declare(..., retry=True)` (no kombu channel); otherwise original code. Verify with a test that publishing a task with rpc backend **does not open** kombu connections (RabbitMQ management API: only one connection `lapin` for the producer + one for the result consumer, none `py-amqp`).
4. **Activation**: `RPCBackend.ResultConsumer` becomes `RsResultConsumer` when app connection is fast path eligible (`_rs_producer.fast_connection(app)` not `None`, AMQP only: `rpc://` with Redis broker remains kombu in this spec) and `CELERY_RS_BACKEND` it is not `0`. Upstream tests that replace `Consumer`/connections with mocks must remain on the original branch: decide the activation at a point that those mocks respect (for example in `start` only if `self.Consumer is kombu.Consumer` and `self.app.connection` has not been replaced).
5. Fork safety like 03b (`register_at_fork`, `on_after_fork`). No changes to signatures, names or public defaults; no new settings in `app.conf`.
6. The AMQP Rust transport: if needed, add what is missing in `celery-transport` (multi-queue `no_ack` consumer with dynamic add/cancel, basic.get) with tests in `crates/celery-transport/tests/amqp.rs`.

## Gate (all mandatory, real outcome in final message)

```
cargo build --release && cargo clippy --all-targets -- -D warnings && cargo test
tools/dev/build-package
tools/dev/build-native
.venv-ref/bin/python -m pytest crates/celery-py/tests -q
.venv-rs/bin/python -m pytest python/tests_rs -q
./compat api                      # 0 differences
./compat wire                     # 0 differing scenarios (includes chord/group on rpc)
./compat upstream --venv .venv-rs # 0 regressions
```
Also `python/tests_rs/test_backend_rpc.py` (`.venv-rs`, RabbitMQ on 5673), with real reference worker (`.venv-ref/bin/celery worker -P solo`) and rs client:
- the consumer used is `RsResultConsumer` (counter/flag) and no py-amqp connection is open;
- `AsyncResult.get(timeout)`, `propagate`, results of multiple tasks in flight, `ResultSet.join_native`, `iter_native`, group, intermediate states (`update_state` PROGRESS → `on_message`), `forget`, `get_task_meta` with backlog (multiple messages for the same task) and `BacklogLimitExceeded`, timeout → same `TimeoutError` as Celery;
- connection closed via management API during `get()` → reconnection and result received;
- `os.fork()` and billiards: the child receives his results;
- `CELERY_RS_BACKEND=0` returns to consumer kombu.
- Benchmark in `bench/bench_result.py` (if other agent creates it, just add `--backend rpc` option; otherwise create it): `apply_async().get()` p50/p99 latency and throughput of `join_native` on 1000 results, RSS client; `.venv-ref` vs. `.venv-rs`.

## Out of scope

Redis backend (04a), `rpc://` on Redis broker, events, control/broadcast, the worker.
