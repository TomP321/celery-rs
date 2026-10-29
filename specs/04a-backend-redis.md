# 04a — Result Redis backend with Rust client

Phase 4 of `PLAN.md`. `python/celery/backends/redis.py` remains the reference code (chord logic, retry, `ResultConsumer`, Sentinel): **the backend logic is not rewritten**. You replace the redis-py *client* with a Rust client that implements exactly the subset of the redis-py API used by the backend, so `store_result`, `get_task_meta`, `AsyncResult.get()` (pubsub), chord counter/join pass from the Rust core without changing behavior or traffic on the server. In parallel another agent works on `rpc://` (spec 04b): do not touch `backends/rpc.py`.

## References

- `python/celery/backends/redis.py`: `ResultConsumer` (pubsub, `_reconnect_pubsub`, `drain_events`, `_socket_readable`, `on_after_fork`), `RedisBackend` (`get/mget/set/_set/delete/incr/expire`, `on_chord_part_return`, `_new_chord_return`, `_restore_deps`, `set_chord_size`, `ensure`, `exception_safe_to_retry`, `connparams`, `_params_from_url`, `_create_client`, `client`, `ConnectionPool`), `SentinelBackend`.
- `python/celery/backends/base.py`: `encode`/`decode`/`decode_result`/`_get_result_meta`, `BaseKeyValueStoreBackend.get_many`/`_mget_to_results`.
- redis-py (in `.venv-ref`): `Redis`, `Pipeline` (transaction=True → MULTI/EXEC; chainable commands, `execute()` → result list, `__enter__/__exit__` → `reset`), `PubSub` (`subscribe/unsubscribe/get_message(timeout, ignore_subscribe_messages)/close/connection/
  connection_pool/on_connect/channels`), `ConnectionPool.reset`, `redis.exceptions`.
- `crates/celery-transport/src/redis/` (redis crate connection already in use), `crates/celery-py/src/transport.rs` (shared Tokyo runtime, safety fork: `runtime()`/`after_fork`), `python/celery/app/_rs_producer.py` (fast path scheme, counters `stats()`, `register_at_fork`).

## Project

1. **Crate `celery-backend`** (`crates/celery-backend/src/redis.rs`): Redis async client (redis crate, multiplexed) with: GET/MGET/SET (EX)/SETEX/DEL/INCR/EXPIRE/PUBLISH/ZADD/ZCOUNT/ ZRANGE/RPUSH/LLEN/LRANGE/… how much does it use `redis.py`; command buffer pipeline executed in a single round trip (MULTI/EXEC when `transaction=True`, like redis-py); pubsub on dedicated connection with limited message queue and `get_message(timeout)`. Values like `bytes` (zero useless copies: `Bytes`), never `serde_json::Value`. Unit testing + testing against real Redis.
2. **Binding PyO3** (`crates/celery-py/src/backend_redis.rs`): `_celery_rs.RedisClient`, `RedisPipeline`, `RedisPubSub`; GIL released during I/O; `transport.rs` shared runtime (same fork mechanism).
3. **Shim Python** `python/celery/backends/_rs_redis.py`: `RsRedis`, `RsPipeline`, `RsPubSub`, `RsConnectionPool` with the same redis-py signatures/returns for the methods used (e.g. `get` → `bytes|None`, `incr` → `int`, `pipeline().execute()` → list with redis-py types, `zadd(name, mapping)`, `set(name, value, ex=None)`, `pubsub(ignore_subscribe_messages=True)`, `get_message` → dict `{'type','pattern','channel','data'}` as redis-py). The exceptions are the **redis-py** classes (`redis.exceptions.ConnectionError`, `TimeoutError`, `ResponseError`, …), so `connection_errors`, `ensure`, `exception_safe_to_retry` and `_reconnect_pubsub` remain unchanged. If the backend accesses uncovered redis-py internals (e.g. `_pubsub.connection.register_connect_callback`, `connection._sock` for `_socket_readable`), provide a minimum equivalent on the shim or make `ResultConsumer` capable of using the shim with a dedicated branch **only if** it is the Rust instance; the redis-py branch remains identical.
4. **Activation**: in `RedisBackend._create_client` use `RsRedis` when `self._get_client() is redis.StrictRedis` of the real module (upstream tests replace `Backend.redis` with a fake module: we stick with that there), the backend is `RedisBackend` (not `SentinelBackend`), scheme `redis`/`rediss`/`socket` supported, no unmapped connection parameters (`connparams` unknown, `credential_provider`, ssl with unsupported options, `retry_on_timeout`… → redis-py). Deactivation: env `CELERY_RS_BACKEND=0` (no new settings in `app.conf`). Map `socket_timeout`, `socket_connect_timeout`, `socket_keepalive`, `health_check_interval`, `client_name`, `max_connections`, `db`, `password`/`username`.
5. **Result JSON**: in `base.py`, `encode`/`decode` use `_celery_rs.json_dumps`/ `json_loads` when the serializer is `json` and the content type is kombu (same output, same exceptions; `accept` respected). The other serializers remain with kombu. This applies to all backends (including `cache+memory://`).
6. **Identical traffic**: The sequence of commands sent to Redis must match redis-py (same order, MULTI/EXEC where redis-py uses them). Connection handshake: redis-py sends `CLIENT SETINFO`/`HELLO` etc.; if `./compat wire` reports differences **only** in the handshake, stop and report them in the final message instead of mimicking the library name.
7. Fork safety: connections discarded in child (`register_at_fork` + `on_after_fork` already in backend), lazy recreated runtime. No changes to signatures, names or public defaults.

## Gate (all mandatory, real outcome in final message)

```
cargo build --release && cargo clippy --all-targets -- -D warnings && cargo test
tools/dev/build-package
tools/dev/build-native
.venv-ref/bin/python -m pytest crates/celery-py/tests -q
.venv-rs/bin/python -m pytest python/tests_rs -q
./compat api                      # 0 differences
./compat wire                     # 0 differing scenarios
./compat upstream --venv .venv-rs # 0 regressions
```
Also `python/tests_rs/test_backend_redis.py` (`.venv-rs`, Real Redis on 6390):
- the backend really uses `RsRedis` (counter/flag) in real scenarios, and redis-py when `CELERY_RS_BACKEND=0` or with Sentinel;
- differential ref vs rs: same values in Redis (`celery-task-meta-*`, chord `.j`/`.t`/`.s`) and same sequence of commands (MONITOR) for `store_result`, `get`, `forget`, group/chord with real worker of the reference (`.venv-ref` `celery worker -P solo`) and client rs, and vice versa;
- `AsyncResult.get(timeout)` via pubsub, `propagate`, `disable_sync_subtasks`, `ResultSet.join` and `join_native`, `iter_native`, timeout → same `TimeoutError`;
- connection killed (`CLIENT KILL`) during `get()` → reconnection as redis-py;
- forks (os.fork and billiard) and 16 concurrent threads on `store_result`/`get`.
- New `bench/bench_result.py` benchmark: `apply_async().get()` p50/p99 latency, `get_many`/`join` throughput on 1000 results, RSS client; `.venv-ref` vs `.venv-rs` with reference workers. Report the numbers.

## Out of scope

`rpc://` (spec 04b), Sentinel, cluster, other backends (database, memcached cache, …), the worker.
