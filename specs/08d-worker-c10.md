# 08d — Prefork worker with concurrency 10: minimum RAM, maximum performance

Project owner's goal: **a `-P prefork -c 10` worker that uses as little RAM as possible than the original Celery and performs as well as possible**. It is the main metric of the project: each choice of this spec is judged on RAM savings (MB and %) and on throughput compared to the reference with 10 processes.

Start from branch `agent/08b-memory` (job 08b not yet merged into `main`). Read `AGENTS.md`, `PLAN.md`, `specs/08b-memory.md`, `specs/08b-review.md`, `specs/08b-review2.md`. Constraints unchanged: API and behavior identical to the reference; **real kombu/billiard objects, no rewritten `__module__`/`__qualname__`/`__name__`, no replacement classes** (`python/tests_rs/test_kombu_identity.py` does not change); `celery/`, `celery/t/`, `tools/compat/` are not changed; test with broker always under `tools/dev/with-brokers`.

## Measure (do it first and last)

`bench/bench.py --pool prefork --concurrency 10 --n 30000` for `.venv-ref` and `.venv-rs`, Redis and AMQP (`BENCH_BROKER=amqp://guest:guest@127.0.0.1:5673//`), with `--task bench` and `--task pure`. Report in the table, ref vs rs, with **savings in MB and %**:
- Total shaft PSS: idle, peak, after load;
- PSS per process (master, zygote, each child): idle and after load; Private_Dirty per child;
- end-to-end task/s and p50/p99 latency;
- wake up/s from idle (optional). Repeat each measurement at least 2 times (variance). Reference with 4 processes (08b): ref 145,7 MB idle, rs 72,3 MB; child rs 11,2 MB idle, 23,5 MB after loading with bench task (redis-py imported from task), 12,9 MB with `--task pure`; ref ~31 MB for son.

## Where to earn (check with numbers, only adopt what the bench confirms)

1. **Private memory per child** (with 10 children dominates everything):
   - everything that the child imports/allocates to the first task must be done in the zygote before the fork and `gc.freeze()` (user task modules: `app.loader.import_default_modules()` is already there; modules imported *by* tasks at first use, e.g. redis-py in the bench: measures whether an explicit preload of the modules already present in the master - as in fact the reference does, which has everything in the master - reduces the total PSS);
- copy-on-write: check how many inherited pages become private after N tasks (refcount, GC, allocator); `gc.freeze()` after warm-up, GC thresholds, `PYTHONMALLOC`/arenas: only if invisible to the user's code (same `gc.get_threshold()`/`gc.isenabled()` as the reference seen by the task);
   - child buffers/structures (IPC, `_rs_child`): size and growth.
2. **Master and zygote**: what the supervisor does not need outside the zygote; the zygote does not have to keep memories that its children do not use.
3. **Throughput with children 10**: profile the master (dispatch, IPC, ack) at c10; check that throughput grows from c4 to c10 (ref on Redis does not scale: 852 → 914 alone to c4). Prefetch and send/ack batch only if the traffic on the broker remains identical (`compat wire-worker`).
4. **Growth over time**: after 30000 task the children's PSS must not continue to rise (no unlimited growth); `max_tasks_per_child`/`max_memory_per_child` as the reference.

## Test

- Test in `python/tests_rs/` for each change in behavior (e.g. preload in the zygote, GC configuration seen from the task, no growth of PSS beyond a threshold after N tasks).
- No existing tests weakened.

## Gate (real outcome in the final message)

```
cargo build --release && cargo clippy --all-targets -- -D warnings
tools/dev/with-brokers cargo test
tools/dev/build-package && tools/dev/build-native
tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q
tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q
./compat api && ./compat wire && ./compat upstream --venv .venv-rs
./compat wire-worker --pool solo && ./compat wire-worker --pool prefork
./compat wire-beat && ./compat cli
```
If you kill worker processes during testing, make sure they don't get orphaned connected to brokers (spoofing others' gates).

## Final message

Table c10 ref vs rs (Redis/AMQP, bench/pure) with RAM savings in MB and %, task/s, latency; what did you change and why; tested and discarded variants with numbers; gate with real outcome; what remains open. Commit to the branch (`08d: …`).
