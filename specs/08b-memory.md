# 08b — Memory: import diet, prefork children, beat, allocator

Phase 8 of `PLAN.md` (optimization), second part. 08a removed kombu from the worker's master; here we reduce memory in all other processes (client, prefork children, beat) and evaluate the allocator. Each intervention enters **only if the bench confirms it** and the gates remain green.

## Starting measurements (main @ 303a59a, PSS from `/proc/<pid>/smaps_rollup`)

**Client** (`bench/bench_client.py`, new: `import celery` → app+task → `delay()` → `get()`):

| step | ref | rs |
|---|---|---|
| app + tasks | 24,2 MB, 353 modules | 23,9 MB, 342 modules |
| after `get()` | 35,2 MB, 522 modules | 30,6 MB, 383 modules |

rs loads again: `kombu.connection`, `kombu.serialization` (→ `yaml`, `msgpack`), `kombu.transport`, `amqp` (→ `ssl`, `decimal`), `billiard` (→ `multiprocessing`, `pickle`, `cffi`), `click`, `greenlet`, `importlib.metadata` (→ `email`, `zipfile`), `dateutil`, `tzlocal`. Chains found with `python -X importtime`:
- `celery.utils` → `kombu.utils.objects` → `kombu/utils/__init__` → `kombu.utils.compat` → `importlib.metadata`, `kombu.exceptions` → `amqp`, `billiard.util` → `billiard`, `cffi`;
- `celery._state` → `celery.utils.threads` → `greenlet`;
- `celery.app.base` → `click.exceptions` and `dateutil.parser`;
- `celery.signals` → `celery.utils.time` → `tzlocal`;
- `celery.worker` (uploaded by app importer) → `celery.bootsteps` → `kombu.common` → `kombu.entity` → `kombu.serialization` (→ `yaml`, `msgpack`) and `kombu.connection`; `celery.worker.state` → `shelve`.

**Prefork c4 worker, Redis** (`bench/bench.py`, 20 000 `noop`; PSS per process):

| process | idle | after loading |
|---|---|---|
| master | 20,9 MB (anon 11,7, file 9,2), 7 thread | 19,8 MB |
| zygote | 8,9 MB | 11,4 MB |
| every child | 11,7 MB (Private_Dirty 7,3) | 25,1 MB (Private_Dirty 20,5) |
| total | 76,5 MB | 131,8 MB (ref: 149 → 154 MB) |

The growth is all in the children (+13 MB each): at the first task the child imports 144 modules, including `kombu.transport.redis`, `kombu.transport.virtual*`, `kombu.utils.scheduling/text/time` (understand who is asking for them: with transport and the Rust backend they are not needed) and redis-py (`redis`, `asyncio`, `cryptography`, `opentelemetry`, `email`, `hmac`…), which however is imported from the bench task (`benchapp.counter()`: it is user code). Ref child grows by 16 MB without new modules (copy-on-write broken by refcount/GC); the rs child already has `gc.freeze()` (41k frozen objects).

**Worker only, Redis:** 37,0 Idle MB → 46,0 Peak MB (ref 49,7 → 50,0).

**Beat** (`bench/bench_beat.py`, 3 entries): ref Redis 51,3 / AMQP 43,2 MB; rs Redis 45,7 / AMQP 45,7 MB (on AMQP rs is **above** the reference).

## Project

1. **Client import diet** (the user process: `import celery`, `Celery(...)`, `@app.task`, `delay`/`apply_async`, `AsyncResult.get`, canvas). With active Rust producers/backends you must not load: `amqp`, `billiard`, `multiprocessing`, `cffi`, `click`, `greenlet`, `yaml`, `msgpack`, `ssl`, `importlib.metadata`, `dateutil`, `kombu.connection`, `kombu.transport.*`, `kombu.serialization`, `kombu.entity`, `redis` (unless the user uses them or a function that really requires them: serializer `yaml`/`pickle`/`msgpack`, `CELERY_RS_*=0`, brokers/backends not supported by Rust core, eventlet/gevent, CLI). Techniques: Lazy imports at point of use, PEP 562 (`__getattr__`/`__dir__` of module) for re-exported public names, replacing kombu utilities used only for convenience (`cached_property`, `symbol_by_name`, `maybe_list`…) with local equivalents **only where the reference does not expose them as APIs** (public objects remain the same objects: if `celery.utils.X is kombu.utils.X` in the reference, it must remain so). `celery.worker` should not be loaded by those who only import the app. The kombu serializer registry must remain equivalent when used (same `content_type`, same registered encoders/decoders, same behavior as `app.conf.accept_content`, `register_*`/`disable_untrusted_serializers`).
2. **Prefork and zygote children**: find and remove what imports `kombu.transport.redis`, `kombu.transport.virtual*` and the other celery/kombu modules loaded at the first task in the child; everything that the child uses for each task must be imported into the **zygote before `gc.freeze()`** (so it is shared in copy-on-write), not at the first task. Measure with a probe (task that reads `sys.modules` and `smaps_rollup`, see `bench/bench.py` and the new field `pss_after_per_process_kb`). Also evaluate, numbers in hand, `gc.freeze()` after the warm-up of the first import in the child and the deactivation/thresholds of the cyclic GC only if identical for the user (no changes to `gc.get_threshold()` visible to the task code, unless the reference does so). The bench task imports redis-py itself: don't count that cost as "ours", but report it separately (PSS of children with a task that doesn't import anything, for example a `--task pure` variant of `bench.py` that counts completions via result backend or events).
3. **Worker master**: Break down prefork master 20,9 MBs (Python heap vs Rust allocations vs mapped libraries, via `smaps` for mapping, `tracemalloc`, allocator counters) and shrink them: Unneeded Python modules in master (same dot diet 1), number of tokio/blocking threads (each thread carries a glibc arena) and size of buffers. PLAN target: master < 12 MB; minimum target of this spec ≤ 16 MB in prefork and ≤ 30 MB in `-P solo` Redis.
4. **Allocator**: Try `mimalloc` (already in `[workspace.dependencies]`) as `#[global_allocator]` of the `celery._rs` module and, separately, the limit of glibc arenas for Rust threads (e.g. `mallopt(M_ARENA_MAX)` for the worker process only, if measurable and without effects on user code). Adopt only what reduces idle/peak PSS without degrading throughput/latency beyond noise; also report the numbers of the discarded variants. Beware of fork (zygote/children): allocator must be safe after `fork()`.
5. **Beat**: Same diet as imports in the `celery beat` process (Redis and AMQP), with PSS at 3 entries **below** the reference on both brokers (target ≤ 38 MB).
6. No changes to names, signatures, defaults, log texts, or observable behavior; no new public settings. `CELERY_RS_PRODUCER=0`, `CELERY_RS_BACKEND=0`, `CELERY_RS_WORKER=0` keep returning to the reference path (loading kombu is fine there).

## Tests required

- `python/tests_rs/test_import_diet.py` (`.venv-rs`, subprocesses with clean interpreter): for clients (Redis and AMQP: import, app+task, `delay`, `get`, `group`/`chord`), worker master (`-P solo` and prefork, via `CELERY_RS_WORKER_STATE` or a task/probe), prefork child after first task, `celery beat`: list of absent prohibited modules. And on the contrary: with serializer `yaml` / `pickle` / `msgpack`, with `CELERY_RS_PRODUCER=0`, with `celery.utils.X`/`kombu` used by the user, everything works as in the reference (identical messages and results).
- For each lazy name: the same object as the reference (`is`), `dir()` of the same module, pickle/`__reduce__` unchanged where applicable.
- Test Rust for any runtime/buffer/allocator changes.

## Benchmark (reports ref vs rs, before/after, Redis and AMQP)

- `bench/bench_client.py` — target: PSS after `get()` ≤ 20 MB (ref 35), no modules banned.
- `bench/bench.py --pool solo` and `--pool prefork --concurrency 4` (also `--n 20000`): PSS idle, peak, after, per process; throughput and latency must not worsen. Goals: prefork c4 Redis peak ≤ 100 MB with the bench task (now 125), ≤ 60 MB with `--task pure`.
- `bench/bench_beat.py` — PSS to 3 and 1000 entries, both brokers.
- `bench/bench_cli.py` — CLI times should not deteriorate.

## Gate (all mandatory, real outcome in final message)

```
cargo build --release && cargo clippy --all-targets -- -D warnings
tools/dev/with-brokers cargo test
tools/dev/build-package && tools/dev/build-native
tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q
tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q
./compat api && ./compat wire && ./compat upstream --venv .venv-rs   # 0 / 0 / 0 regressions
./compat wire-worker --pool solo && ./compat wire-worker --pool prefork   # 0 / 0
./compat wire-beat && ./compat cli                                      # 0 / 0
```
After each change to the imports run at least `pyflakes` on the touched files and `CELERY_RS_PRODUCER=0` / `CELERY_RS_BACKEND=0` on the relevant test group: in phases 07 and 08a the lazy imports left names used without import in uncovered branches (`NameError` at runtime).

## Out of scope

Beat rpc/ping/tick latencies (08c), beat embedded via zygote (09).
