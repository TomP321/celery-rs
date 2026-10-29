# 08c — Latencies and real worker capacity, without increasing RAM

Project priorities: **Worker RAM and beats** first; here latency and throughput are improved **only with changes that do not increase the PSS** (worker c10 idle/load, beat, client): each change reports the PSS before/after; if it grows beyond the noise (> 0,5 MB) it should be discarded or motivated with a large performance gain. Read `AGENTS.md` (also "Known Failures"), `PLAN.md`, `specs/08d-worker-c10.md`. Constraints unchanged: API, behavior and traffic on the broker identical to the reference (`compat wire*`), real kombu objects, no rewritten names, `test_kombu_identity.py` untouchable, `celery/`, `celery/t/`, `tools/compat/` do not change.

## 1. Missing measurements (before optimizing)

- **Real c10 worker capacity**: Today `bench/bench.py` measures throughput while a single client publishes, and with celery-rs the queue empties at the same rate as publish (client limit, not worker limit). Add to `bench/bench.py` a mode `--prefill`: publish N tasks with the worker stopped, then start the worker and measure the emptying time (worker tasks/s) and the peak PSS. Measure ref vs rs, c4 and c10, Redis and AMQP, `--task pure` and `bench`.
- **Realistic app**: variant of the bench task with `import redis` **at the top of the task module** (as in a real app, therefore shared via zygote) instead of inside the function: `--task bench-toplevel`. Measure PSS c10 ref vs rs (should be close to `pure`).

## 2. Latencies to be improved

| measure | ref | rs today |
|---|---|---|
| rpc backend, `delay().get()` p50 | 3,25 ms | 3,40 ms |
| `inspect ping` in-process to AMQP worker | — | +1,5 ms compared to ref (phase 07) |
| `inspect ping` worker AMQP prefork, p50 | 9,72 ms | 10,66 ms |
| `delay().get()` Redis c10 p50 | 3,01 ms | 1,94 ms |
| `delay().get()` AMQP c10 p50 | 3,28 ms | 2,66 ms |

Objectives: rpc and `inspect ping` AMQP **not worse than reference** (better if ≥ 20% faster); c10 capacity with `--prefill` the highest possible; p99 no worse than the reference. Profile first (where the time goes: client, broker, master, IPC, child, response) and report the breakdown. Examples to check with numbers: waits/polls with timeouts in the response path, Nagle/flush on AMQP sockets, ack batching, loop wake-up, repeated serializations, prefetch with c10.

## Test

Test in `python/tests_rs/` for any change in behavior; no existing tests weakened.

## Gate

`tools/dev/gates` (all green except for the "Known Failures" of `AGENTS.md`). Intermediate checks with `tools/dev/gates --only <nome>` or individual tests under `tools/dev/with-brokers`.

## Work Rules (model quota limited)

Run long commands **once and blocking**, not in the background with periodic checks: each check costs one model call. Don't investigate "Known Failures". Short final report: before/after tables (latencies, capacities `--prefill`, PSS c10 `pure`/`bench`/ `bench-toplevel`, beats), what you changed and why, discarded variants with numbers, gates with real outcome, what remains open. Commit to the branch (`08c: …`).
