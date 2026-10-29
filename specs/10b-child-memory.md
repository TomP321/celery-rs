# 10b — Prefork children RAM under load

Project priority: **take up as little RAM as possible** (speed only matters if it doesn't cost memory). With many processes the RAM of a worker is almost all in the children: a celery-rs child weighs ~6 MB idle but rises to 11–15 MB after load (`bench/results/scale-*.json`, `final-c10-*.json`), versus ~28 MB of the reference. Objective: **child after load ≤ 9 MB** with `--task pure` and ≤ 12 MB with `--task bench-toplevel` (c10 and c50), without increasing master, zygote, beat or client.

Read `AGENTS.md`, `PLAN.md`, `specs/08d-worker-c10.md`, `specs/09b-soak-fuzz.md`, `specs/09c-memory-growth.md`, `specs/10a-master-beat-restart.md`. Constraints unchanged: API, behavior and traffic identical to the reference; real kombu/billiard items, no rewritten names; `test_kombu_identity.py`, `celery/`, `celery/t/`, `tools/compat/` untouchables; no visible changes to the task code (GC configuration seen from the task, user `sys.modules`, logging, signals identical to the reference).

## Diagnosis

For a child, from the fork to the first N tasks (1, 10, 100, 1000) and then at full speed: `Private_Dirty`, `Shared_*` (smaps for mapping: heap, pymalloc arenas, libraries, stack, anonymous mmap), `tracemalloc` (via `CELERY_RS_MEMORY_PROBE`), Rust heap (feature `memory-probe`). Separate:
- **copy-on-write eroded**: zygote pages written by the child (refcount and GC fields of inherited objects, modified dicts/caches, modules that mutate on first use);
- **lazy initializations on first task** (trace paths, serialization, backend, logging, `re`/`functools` cache, `linecache`): move them to the **zygote before fork** with an internal warm-up that does not execute user tasks or produce external effects (no messages, signals, logs or writes to the backend);
- **remaining allocations** (pymalloc/glibc arenas not returned, oversized IPC/Rust buffers, objects held between tasks). Reports a "cause → MB per child" table.

## Corrections

Just by the numbers: warming in zygote, `gc.freeze()` after warming, IPC/Rust buffers scaled or released, releasing arenas after spikes, fewer live objects per child. Each fix with a test in `python/tests_rs` (child's PSS/Private_Dirty above threshold after N tasks; no visible warming effect).

## Check

Your short, blocking measures (≤ 5 minutes): `bench/bench.py --pool prefork --concurrency 10 e 50
--task pure|bench-toplevel --prefill`, `bench/fleet.py --budget-mb 2048`, before/after. **Do not** run `tools/dev/gates`, soak, or `./compat integration` - the orchestrator does them.

## Work Rules (model quota limited)

Blocking commands, no waiting messages or progress checks, no background commands. Short final report: cause table → MB, fixes, before/after measurements (child, total c10/c50, workers in 8 GB estimated), variants discarded, tests performed. Commit to the branch (`10b: …`).
