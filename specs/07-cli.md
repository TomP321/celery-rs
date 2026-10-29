# 07 — CLI: identical output, remote control client in Rust core, lightweight boot

Phase 7 of `PLAN.md`. `python/celery/bin/` is the copy of the reference (click) and `./compat cli` (already in `main`) gives today **0 differences on 190 commands**: `--help` of all commands, usage errors and exit codes, `call`/`result`/`purge`, `status`/`inspect`/`control` against a reference worker, `multi show/names/get/expand`, `amqp`, `list bindings`, `graph`, `report`, `shell`. So the behavior is already there. This spec keeps that gate at 0 and works on costs, which today are **worse** than the reference:

| command (max RSS, `/usr/bin/time`) | ref | rs |
|---|---|---|
| `celery --help` | 46,0 MB | 48,5 MB |
| `celery -A app status` (no workers) | 55,5 MB | 59,2 MB |

## References

- `celery/celery/bin/*.py` (`base.py`: `CeleryCommand`, `CLIContext`, `CeleryOption`, error management and exit code `EX_*`; `control.py`, `call.py`, `result.py`, `purge.py`, `multi.py`).
- `celery/celery/app/control.py` (`Control.broadcast`, `Inspect._request`, `ping`, `flatten_reply`, `Control.mailbox` with `reply_queue_ttl`/`reply_queue_expires`/`producer_pool`).
- kombu `pidbox.py` (`Mailbox._broadcast`, `_publish`, `_collect`, `get_reply_queue`, `reply_exchange`, `oid`, `serializer`, `accept`, `limit`, `callback`, `timeout`).
- `python/celery/app/_rs_producer.py` (3b), `python/celery/_rs_message.py`, `python/celery/backends/_rs_rpc.py` (4b: consume from queue with Rust transport), `crates/celery-transport` (Redis: pub/sub fanout `/0.celery.pidbox` and code; AMQP: fanout/direct).

## Project

1. **Remote control client in the Rust core.** `Control.broadcast(..., reply=True/False)` (so `inspect`, `control`, `status`, `app.control.ping()`, `revoke`, `rate_limit`, ...) publishes to the mailbox and collects the answers with the Rust transport when the transport is suitable (same conditions and producer deactivation variable: `CELERY_RS_PRODUCER=0` → kombu), without opening kombu connections or importing redis-py/py-amqp. Identical to the reference: exchange and response queue (name `oid`, `reply_queue_ttl`/`expires`, auto-delete), serializer/accept, order and form of responses, `limit`, `callback` called on each response, `timeout` (wait exactly like kombu: until timeout if `limit` is not reached), `destination`, `pattern`/`matcher`, `connection=`/`channel=` passed by the user (in that case you remain on kombu). Gate: `./compat wire` (client scenarios `control_*`) and `./compat cli` to 0. `Control.Mailbox`/`self.mailbox` remain available with the same interface (a user can still use them directly: you stay on kombu there).
2. **Light boot.** `celery --help`, `celery <cmd> --help`, usage errors, and `celery multi` should not load `_celery_rs` nor kombu transport/redis-py/py-amqp if the reference does not load them; commands that talk to the broker only load the Rust transport. Use lazy imports without changing public names (module attributes remain accessible: `celery.bin.base`, `celery.app.control.Mailbox`, etc.). Test with tests that read `sys.modules` after each command (launch the CLI with `-X importtime` or a test `sitecustomize`, do not change the code for the tests).
3. **`celery call`/`purge`/`result`** already use Rust producer/backend: check that `purge` uses the Rust transport (same count and message as `Purged N messages from M known task queues.`) and that `result` does not open unnecessary kombu connections.
4. **`celery multi`** real: `start`/`stop`/`stopwait`/`restart`/`kill` of 2 nodes with rs workers (`--detach`, pidfile and logfile in a temporary dir), same output and exit code as the reference (same commands with worker ref); `celery worker --detach` and `celery beat --detach`.
5. No changes to signatures, names, defaults or texts; no new settings.

## Tests required

- `python/tests_rs/test_cli.py` (`.venv-rs`, each case compared to `.venv-ref`): point 4, `celery events --dump` for 2 s with a worker executing a task (same event types), `purge` without `-f` with response `no`/`yes` on stdin, `control shutdown` (last case), `inspect` with 2 worker (order and `limit`), `control` with `-d` on multiple nodes, broker unreachable (same error and exit code), imports present in `sys.modules` for `--help`, `status`, `call`.
- Rust unit tests for response collection (timeout, limit, other's responses `ticket` discarded as in kombu).

## Benchmarks

New `bench/bench_cli.py` (ref vs rs, Redis and AMQP, median of 10 runs): time and max RSS of `--help`, `call`, `result`, `purge -f`, `status` (no workers), `inspect ping` with 1 worker and with `--timeout 0.2`, and of `app.control.inspect().ping()` 1000 times in a process (p50 latency for call with `limit=1`, final RSS). Targets: RSS rs ≤ ref for `--help`, at least −10% for `status`/`inspect ping`/`call`; start time rs ≤ ref.

## Gate (all mandatory, real outcome in final message)

```
cargo build --release && cargo clippy --all-targets -- -D warnings
tools/dev/with-brokers cargo test
tools/dev/build-package && tools/dev/build-native
tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q
tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q
./compat api && ./compat wire && ./compat upstream --venv .venv-rs   # 0 / 0 / 0 regressions
./compat wire-worker --pool prefork && ./compat wire-beat              # 0 / 0
./compat cli                                                           # 0 differences
```
Plus the numbers of `bench/bench_cli.py` (ref vs rs) in the final message.

## Out of scope

Worker side pidbox (phase 8), `celery events` with curses, `celery shell` with IPython/bpython, `celery migrate`, `celery upgrade`, `celery logtool`.
