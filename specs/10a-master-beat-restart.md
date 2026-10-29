# 10a — RAM: worker master, beat idle, jump on broker restart

Project priority: **Worker RAM and beats** compared to the original Celery (then performance). Phases 0–9 are closed (`PLAN.md`). Three memory points remain. Read `AGENTS.md`, `PLAN.md`, `specs/08b-memory.md`, `specs/08f-beat-memory.md`, `specs/09c-memory-growth.md`. Constraints unchanged: API, behavior and traffic identical to the reference; **real kombu/billiard objects, no rewritten names, no replacement classes**; `test_kombu_identity.py`, `celery/`, `celery/t/`, `tools/compat/` untouchables.

## Status (main @a28e412)

| | ref | rs | objective |
|---|---|---|---|
| master worker prefork c10, idle | 27,6 MB | 18,3 MB | **≤ 13 MB** |
| zygote, idle | — | 6,1 MB (with preload 9b: ~11 MB) | don't grow up |
| beat 3 entries, idle | 43–51 MB | 26–28 MB | **≤ 22 MB** |
| jump to broker restart (soak c10 Redis, total) | +7 MB | +14 MB | **≤ +5 MB** |

Known breakdown (08b, 08f): in master and beat the Rust heap is < 0,2 MB; Python heaps (imported modules) and mapped libraries dominate; beat load ~364 modules.

## Work

1. **Master**: list modules and cost (`python -X importtime`, `tracemalloc`, `sys.modules`) of the master with Rust pool; everything that is only needed by the children (trace, canvas, result, backend...) must be loaded into the zygote, not into the master; what is of no use to anyone should only be loaded when used (honest lazy imports, PEP 562). Verify that the zygote is not holding memory that its children do not use.
2. **Beat**: same analysis on the `celery beat` process (Redis and AMQP); target ≤ 22 MB with 3 entries.
3. **Broker restart**: What is loaded or allocated in master, zygote and children on reconnect (errors, retry, rebuilding connections/channels/consumers, buffers) compared to the reference; upload it before fork or avoid it. Targeted test: worker c10 idle or with little load, PSS per process before and after a `docker restart` of the brokers in **your** lane.
4. Each change with a test in `python/tests_rs` (modules absent from master/beat, no growth above threshold on restart) and before/after measurements (`bench/bench.py --pool prefork
   --concurrency 10 --task pure`, `bench/bench_beat.py`, `bench/bench_client.py`): no growth of PSS elsewhere, throughput and latency within noise.

## Check

Your short measures (≤ 5 minutes each, blocking). **Do not** run `tools/dev/gates`, soak, or `./compat integration` - the orchestrator runs them after your commit.

## Work Rules (model quota limited)

Blocking commands, no waiting messages or progress checks; no background commands (they die with the session). Short final report: before/after table (master, zygote, children, beat, client, jump on restart), modules removed/moved, discarded variants with numbers, tests performed, what remains. Commit to the branch (`10a: …`).
