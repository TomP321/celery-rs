# 10c — RAM: every MB of the c10 worker and beats

Owner's top priority: **occupy as little RAM as possible** (speed only matters if it remains better than reference). State after 10b (`bench/fleet.py`, 8 GB): worker `-c 10` rs **77,4 MB idle, 85,2 MB under load** (ref 300 MB); 105 worker in 8 GB idle, 96 under load (ref 27). For worker: master ~18 MB, zygote ~8 MB, children 5,9–8 MB (c50/c10) dependent `pure`, ~9–11 MB with `bench-toplevel`; beat ~28 MB with 3 entries.

Targets: worker c10 **≤ 65 MB idle and ≤ 72 MB under load** (`fleet.py`), master ≤ 15 MB, beat ≤ 24 MB, without increasing client memory and with p50 latency below the reference.

Read `AGENTS.md`, `PLAN.md` (phase 10), `specs/10a-master-beat-restart.md`, `specs/10b-child-memory.md`. Constraints unchanged: Identical API, behavior and traffic; real kombu/billiard items, no rewritten names; `test_kombu_identity.py`, `celery/`, `celery/t/`, `tools/compat/` untouchables; no visible effect on task code.

## 1. Balance sheet (before correcting)

"Where does each MB go" table for master, zygote, child (idle and after load) and beat: private vs shared (`smaps` for mapping), Python heap per module (tracemalloc, `sys.modules`, size of code objects and docstrings), free pymalloc/glibc arenas, mapped libraries, thread stack, Rust heap (`memory-probe`). Indicate for each entry whether it is shared among children, between different workers, or private.

## 2. Experiments to measure (adopt only if bench confirms)

- **Modules in the master and in the zygote**: what each one really needs; move into the zygote what is only needed by the children, remove from the master what is only needed by the zygote, use proper lazy imports.
- **Zygote**: private memory of the zygote that the children do not use (can you free it before forking?).
- **Allocator**: tunable glibcs set by the worker for itself (`M_TRIM_THRESHOLD`, `M_MMAP_THRESHOLD`, `M_ARENA_MAX` for Rust threads), `PYTHONMALLOC`, releasing arenas after startup and after spikes; only if invisible to user code.
- **Threads**: Number and size of Rust/Python thread stacks in master, zygote, children, and beats (smaller stacks where safe).
- **Docstrings/annotations/code objects** not necessary at runtime: only if the behavior remains identical (public `__doc__` must remain the same as the reference: nothing `-OO`).
- **Beat**: Modules and structures left after 08f/10a.

## Test

One test in `python/tests_rs` for each change (PSS thresholds per process, modules absent, no visible effect). No existing tests weakened.

## Check

Your short, blocking measures (≤ 5 minutes each): `bench/fleet.py --budget-mb 2048` (rs, before and after), `bench/bench.py --pool prefork --concurrency 10 --task pure|bench-toplevel --prefill`, `bench/bench_beat.py`, `bench/bench_client.py`. **Do not** run `tools/dev/gates`, soak, or `./compat integration` - the orchestrator runs them after your commit.

## Work Rules (model quota limited)

Blocking commands, no waiting messages or progress checks, no background commands. Final short report: balance sheet, experiments with numbers (adopted and discarded), before/after measurements (c10 workers idle and under load, workers in estimated 8 GB, master, zygote, child, beat, client, latency), tests performed. Commit to the branch (`10c: …`).
