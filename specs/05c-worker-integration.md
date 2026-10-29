# 05c — Worker: `-P prefork` integration + traffic parity on broker

Phase 5 of `PLAN.md`. Already in `main`: Rust consumer and loop for `-P solo` (05a, `python/celery/worker/_rs_consumer.py`, `crates/celery-worker/src/consumer.rs`) and Rust pool with zygote (05b, `python/celery/concurrency/rs_prefork.py`, `_rs_zygote.py`, `_rs_child.py`, `crates/celery-worker/src/pool/`). This spec combines them: `celery worker` with the default pool (`prefork`) uses consumers, loops **and** Rust pools, with behavior and traffic on the broker identical to the reference. It is the last part of phase 5 before optimization.

## New verification tool (already in `main`)

`./compat wire-worker [--pool solo|prefork] [--broker redis amqp] [-s scenario]`: starts the same scenario with the reference worker and with that of celery-rs (always the reference client), captures the traffic on the broker and compares results, events, ack/redelivery, declarations and broadcasts (excluding polling and heartbeat). It's ref-vs-ref stable. Report in `tools/compat/out/wire-worker-report.md`. Today (main):
- `--pool solo`, AMQP: 0 differences on 10 scenarios;
- `--pool solo`, Redis: 10 different scenarios, **all for periodic maintenance of kombu on the kombu connection used by pidbox/gossip** (renewal of bindings with TTL and cleaning: `EVALSHA ef7a…`/`ZADD _kombu.binding_deadlines GT`/`SADD _kombu.binding.*`, `EVALSHA 9a2b…`, `TIME`): in the rs worker they run fewer times. See `kombu/transport/redis.py`: `MultiChannelPoller.maybe_refresh_binding_expiry`, `Transport.register_with_event_loop` (timer `call_repeatedly(1, ...)`), `BINDING_SWEEP_INTERVAL`, `MIN_BINDING_TTL`. Without renewal, after `MIN_BINDING_TTL` (1 h) another worker can wipe out the pidbox binding and the remote control stops working: it's a bug to be fixed, not normalized.

## Project

1. **Kombu maintenance**: Find why the periodic timers recorded by the kombu transport on the hub are not running as in `asynloop` (frequency and number of executions) and fix `rsloop`. Gate: `./compat wire-worker --pool solo` → 0 differences on Redis and AMQP. Add a test that speeds up times (for example monkeypatch of `BINDING_SWEEP_INTERVAL` and `MIN_BINDING_TTL` in the test worker) and verify that the renewal occurs repeatedly.
2. **Wake up without polling**: Today `rsloop` drains the hub every 0,25 s when it has no timer, so control commands have up to 250 ms of delay and the idle worker wakes up 4 times per second. Add to the `Waker` an fd (eventfd on Linux, pipe elsewhere) readable when notified (`Waker.fileno()`, without changing `notify`/`wait`) and make `rsloop` sleep in the hub poller (epoll) with the Waker fd registered and timeout = next timer: zero delay for pidbox/gossip, no unnecessary awakenings. Measure idle worker wakeups/s (before/after).
3. **`-P prefork` → Rust pool**: when the Rust consumer is active (same conditions as 05a, now also with `prefork` pool), `celery worker` uses `RsTaskPool` instead of `concurrency.prefork.TaskPool`; pool events arrive in the loop from `Waker`. The Rust pool is **only** used together with the Rust consumer (never with kombu's `asynloop`); `CELERY_RS_WORKER=0` back to full reference. They remain identical: `--concurrency`, `--autoscale` (Autoscaler with `grow/shrink`), `--max-tasks-per-child`, `--max-memory-per-child`, `--time-limit`/ `--soft-time-limit`, `revoke(terminate=True, signal=...)`, `inspect stats` (`pool` with the same keys), `worker_process_init`/`worker_process_shutdown`, `worker_ready`, `worker_shutting_down`, warm/cold/soft shutdown with tasks in progress (same outcomes as ack/requeue, `worker_cancel_long_running_tasks_on_connection_loss`), reconnection to the broker with tasks running in the children. The upstream tests `t/unit/concurrency` and `t/unit/worker` must continue to exercise the reference (billiard) code, as today.
4. **Zygote lost**: today the pool remains childless. Required behavior: clear `critical` log and worker shutdown with exit code ≠ 0 (so systemd/supervisor restarts it), in-flight job with `WorkerLostError` and messages redelivered according to `acks_late`/`reject_on_worker_lost` as in the reference. Tested with `os.kill(zygote_pid, SIGKILL)`.
5. No changes to signatures, names or public defaults; no new settings in `app.conf`.

## Gate (all mandatory, real outcome in final message)

```
cargo build --release && cargo clippy --all-targets -- -D warnings
tools/dev/with-brokers cargo test
tools/dev/build-package && tools/dev/build-native
tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q
tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q
./compat api && ./compat wire && ./compat upstream --venv .venv-rs   # 0 / 0 / 0 regressions
./compat wire-worker --pool solo                                       # 0 differences
./compat wire-worker --pool prefork                                    # 0 differences
```
Also `python/tests_rs/test_worker_prefork.py` (`.venv-rs`), worker rs `celery worker` (default pool, `-c 4`) as subprocess with reference client: same scenarios as `test_worker_consumer.py` repeated with prefork, plus time limit, `--max-tasks-per-child`, `--max-memory-per-child`, `--autoscale=4,1`, revoke with terminate, child killed during a task, zygote killed, warm/cold shutdown with long tasks in children, connection to broker lost with task in progress. Each scenario compared with the reference worker (same outcomes).
- `bench/bench.py --pool prefork --concurrency 4` benchmarks for `.venv-ref` and `.venv-rs`, Redis and AMQP: end-to-end throughput, p50/p99 latency, **idle and peak worker tree PSS** (baseline in `PLAN.md`: Redis 146/151 MB, AMQP 145/149 MB), wakeups/s from idle. Report the numbers.

## Out of scope

Pidbox/mingle/gossip in Rust, memory optimization (step 8), beat, `-P threads`, eventlet/gevent.
