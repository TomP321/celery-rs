# 08a — Worker: pidbox, mingle and Rust transport gossip (no kombu in master)

Phase 8 of `PLAN.md` (optimization), first part. From 05a the master of the worker with Rust consumer (`python/celery/worker/_rs_consumer.py`) still opens a lazy **kombu** connection only for remote control (pidbox), mingle, gossip and event reading: for this it loads `kombu.pidbox`, `kombu.connection`, the kombu transport and redis-py/py-amqp, and keeps their fd and timers alive on the hub (see Redis binding maintenance fixed in 05c). Objective: the master no longer uses kombu to talk to the broker; everything goes through the Rust transport, with identical traffic.

Starting measurement (`-P solo`, Redis backend, idle master PSS; Celery ref vs celery-rs):

| brokers | ref | rs today | modules uploaded by rs master today |
|---|---|---|---|
| Redis | 54 MB | 50 MB | `redis`, `kombu.transport.redis`, `kombu.pidbox`, `kombu.connection` |
| AMQP | 54 MB | 41 MB | `amqp`, `kombu.transport.pyamqp`, `kombu.pidbox`, `kombu.connection` |

## References

- `celery/celery/worker/pidbox.py` (`Pidbox`, `gPidbox`: `Node`, `listen`, `on_message`, `reset`, `shutdown`, `_close_channel`), `worker/consumer/control.py`, `worker/control.py` (`Panel`, commands and responses), `worker/consumer/mingle.py` (`hello`, `sync_with_node`, `on_clock_event`, `on_revoked_received`), `worker/consumer/gossip.py` (`Receiver`, `get_consumers`, `on_message`, `election`, `on_elect`, `on_node_join/leave/lost`, `clock`), `worker/consumer/events.py`, `worker/consumer/heart.py`, `worker/consumer/consumer.py` (`connection_for_read/write`, `on_close`, reconnection).
- kombu: `pidbox.py` (`Node.listen`, `handle_message`, `dispatch`, `reply`, `_publish_reply`, `reply_exchange`, queue names `{hostname}.{mailbox}.pidbox`, `exclusive`, `auto_delete`, `expires`/`message_ttl`, `oid`), `transport/redis.py` (fanout via PUBLISH/PSUBSCRIBE with `fanout_prefix`/`fanout_patterns`, TTL binding and periodic maintenance), `messaging.py`.
- Already in `main`: `crates/celery-transport/src/control.rs` and `python/celery/app/_rs_control.py` (07: client side of the mailbox, same format of messages and reply queues), `crates/celery-worker/src/consumer.rs` (05a), `_rs_message.py`, `Waker` with fd (05c).

## Project

1. **Pidbox in Rust core**: Node queue consumer (`{hostname}.celery.pidbox` and fanout bindings like kombu, on Redis and AMQP) whose messages arrive at the loop from `Waker`; the reference code (`Pidbox.on_message` → `Node.handle_message` → `Panel`) remains the one that decides and responds; the response starts from the Rust producer towards `reply.celery.pidbox` with identical `routing_key`/ `correlation_id`/`ticket`. `reset()`/`shutdown()`/reconnection as in the reference.
2. Rust transport gossip: `worker.#` event queue (names, `auto_delete`, `expires`, `message_ttl` as `Receiver`), same Python handlers (`on_message`, election, clock, join/leave/lost). Outgoing events already pass through the Rust producer.
3. **Mingle** without kombu connection: `hello` broadcast and response collection via the 07 Rust control client (today `mingle` passes `connection=` and therefore falls back to kombu), with the same responses and the same effect (synchronized revocations and clocks).
4. **Redis maintenance**: if the pidbox/gossip TTL bindings pass to the Rust transport, the periodic renewal (`REFRESH_BINDING`/sweep, `_kombu.binding_deadlines`) must also be done from Rust with the same traffic (gate `wire-worker` on Redis).
5. **Result**: With the Rust consumer active the master does not import `kombu.connection`, `kombu.pidbox`, `kombu.transport.*`, `redis`, `amqp` (unless the results backend or the user loads them) and does not register fd kombu on the hub. `CELERY_RS_WORKER=0` back to full reference. `-P solo` and `-P prefork` (Rust pool) both.
6. No changes to names, signatures, defaults or log texts; no new settings.

## Tests required

- `python/tests_rs/test_worker_control.py` (`.venv-rs`, worker rs as subprocess, client and second worker of the reference where needed, each outcome compared with the worker of the reference): all commands `inspect`/`control` (including `revoke` with terminate, `rate_limit`, `time_limit`, `add_consumer`/`cancel_consumer`, `pool_grow/shrink`, `autoscale`, `shutdown`, `election`, `heartbeat`, `query_task`), destination/pattern, mingle with worker ref↔rs in both directions (revocations and synchronized clocks), gossip (`on_node_join/leave/lost` with a worker ref entering and exiting), reconnection to the broker (pidbox responds again), Redis and AMQP, `-P solo` and `-P prefork`.
- Master modules: a task `-P solo` (and a probe in the master for prefork, for example via `CELERY_RS_WORKER_STATE`) checks for the absence of the modules listed in point 5.
- Rust unit tests for the pidbox/gossip consumer (fanout bindings, code names, TTL).

## Benchmarks

`bench/bench.py --pool solo` and `--pool prefork --concurrency 4`, Redis and AMQP: master/tree idle and peak PSS, throughput and latency (should not deteriorate), wake up/s from idle; and latency of `inspect ping` to the worker (p50 of 200 calls with `limit=1`). Report ref vs rs before/after. Target: Master PSS `-P solo` Redis ≤ 41 MB, no degradation elsewhere.

## Gate (all mandatory, real outcome in final message)

```
cargo build --release && cargo clippy --all-targets -- -D warnings
tools/dev/with-brokers cargo test
tools/dev/build-package && tools/dev/build-native
tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q
tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q
./compat api && ./compat wire && ./compat upstream --venv .venv-rs   # 0 / 0 / 0 regressions
./compat wire-worker --pool solo && ./compat wire-worker --pool prefork   # 0 / 0 (including two_workers)
./compat wire-beat && ./compat cli                                      # 0 / 0
```

## Out of scope

mimalloc and general memory profiling (08b), rpc/beat latencies (08c), embedded beats.
