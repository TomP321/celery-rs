# 11c — `-c 10` worker RAM: immortal objects and consumption under load

Read `AGENTS.md`, `PLAN.md` (phase 10), `docs/DIFFERENCES.md`. Priority: `-c 10` prefork worker RAM, **both idle and under load**; throughput must not deteriorate more than 5%.

## Orchestrator diagnosis (already measured, Python 3.13, Redis, task `add`)

Tools in `bench/memprobe/` (`breakdown.py`: PSS for process and mapping; `loadpeak.py`: PSS peak during 30.000 task; `sd.py`: pages written after fork via soft-dirty → object types). Usage: `tools/dev/with-brokers .venv-rs/bin/python bench/memprobe/loadpeak.py
$PWD/.venv-rs 10 $PWD/bench/memprobe 30000`.

| | rest | peak load | after loading |
|---|---|---|---|
| original | 307 MB | 307 | 307 |
| celery-rs | 67,5 | 74,6 (children 55,6) | 76 |

- Each child has ~3 private MB, all dirty in the first second (fork + `process_initializer`), then +0,6–0,8 MB with load. The touched pages are mostly `code` objects (adaptive interpreter counters), refcounts of `str`/`type`/`tuple`/`function`, `dict`.
- **Successful experiment** (temporary patch, already removed): make immortal (PEP 683, refcount to 32 low bits = `0xFFFFFFFF`) all objects reachable from `gc.get_objects()` + direct referents, **in the master immediately before the zygote fork** and again **in the zygote before `gc.freeze()`**, excluding objects whose type has a finalizer (`__del__`/`tp_finalize`) and instances of `_celery_rs` types: rest 69,6 → 53,3 MB, after 2000 task 76,1 → 56,1 MB (master 13,4 → 10,9; child 5,7 → 4,1).
- It must be done **without large temporary structures** (a `set` of visits in Python made the zygote inherit a bloated heap: worse result). The immortal refcount itself acts as a marker of "already visited". After the visit: `_release_task_memory(True)` before the fork.
- Immortalizing only in the zygote is counterproductive (breaks sharing with the master: +12 MB).

## Work required

1. **Native immortalization** in `celery-py` (no `ctypes`): internal function of `_celery_rs` that receives the list of `gc.get_objects()` and marks objects and referents (`tp_traverse` via `PyType_GetSlot`; also descend into containers not tracked by the GC, e.g. tuples). Conditions: CPython ≥ 3.12, non-free-threaded build, 64-bit little-endian; otherwise no-op (3.10/3.11 remain as today). Skip already immortal items, those with `tp_finalize`/`tp_del`, and types of `_celery_rs`. `// SAFETY:` on why the refcount writing is correct (PEP layout 683 for version; check on 3.12, 3.13 and, if available with `uv python`, 3.14: for 3.14 use the correct immortal initial value). Called in the master before the zygote fork and in the zygote before `gc.freeze()`. Can be deactivated with `CELERY_RS_IMMORTALIZE=0`. Document in `docs/DIFFERENCES.md` what changes (existing objects at fork never freed: no `__del__` for those without type finalizer, weakref/`WeakValueDictionary`/weak receivers of non-decaying signals, huge `sys.getrefcount`) and why it is accepted.
2. **Under load**: measure with `loadpeak.py` and `sd.py` (adapt it to count the pages that a child dirty between task 10 and task 2000) what grows during the execution of the tasks in the children and in the master, and reduce it. Leads to verify with measurements, not to apply blindly: pymalloc allocations scattered in inherited partially free pools (fill/compact them in the zygote before freezing so the child's new allocations end up in their own arenas), avoidable per-task objects in the path `_rs_child` → trace → READY (buffer, pickle, frame), master buffer for message/prefetch. Also repeat with arguments from 16 KB (add an option to `loadpeak.py`).
3. Tests: `python/tests_rs/` — item marked immortal after call, items with finalizer and of `_celery_rs` not touched, objects created afterward remain normal, no-op with `CELERY_RS_IMMORTALIZE=0` and on unsupported versions; a test that measures the private pages of a forked child (with margin). Rust unit tests where possible.
## Targets (`-c 10`, Redis, task `add`, Python 3.13)

Rest ≤ 53 MB, peak under load ≤ 58 MB, post load ≤ 56 MB, task/s ≥ 95% of today (~2800). Also reports numbers with arguments from 16 KB and on RabbitMQ.

## Check

`cargo build --release`, `cargo clippy --all-targets -- -D warnings`, `cargo test` (`tools/dev/with-brokers`), `tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q`, `./compat api`. **Do not run `tools/dev/gates`, `./compat upstream|integration` or soak** - the orchestrator does them. Blocking commands, no waiting messages, no pushes. Commit to branch (`11c: …`); final report with before/after table and actual measurements.
