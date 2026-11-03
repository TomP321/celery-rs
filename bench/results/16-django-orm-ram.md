# 16 — Django ORM child RAM

Python 3.11, Redis, PostgreSQL, prefork c10. Measurements use this worktree's
wheel and `tools/dev/with-brokers tools/e2e/bench.sh` (3000 tasks, three alternating
reference/rs rounds). PSS is in MiB. Differences within 2 MiB are noise.

## Attribution method

`bench/memprobe/django_orm.py` runs 2000 jobs in one real Rust pool child.
It samples startup, task 1, task 10, and task 2000 without tracemalloc.
Separate processes isolate traced add, raw psycopg connection, Django connection,
SQL compilation without execution, and traced ORM count. The connection controls
keep the connection open, matching the project's 60-second connection lifetime.

The probe clears Linux soft-dirty bits before task 1 and again after it.
It intersects written pages with initially resident shared anonymous/heap pages
that become exclusive. This measures inherited CoW, including refcounts,
interpreter specialization, and writes to inherited containers. It cannot
identify refcount writes alone. Object types come from `sd.py` after sampling;
a type on a written page does not establish what wrote that page.

Controls overlap and must not be added as independent allocations. PSS in a
one-child probe has a different sharing divisor from c10; private bytes and
page counts explain costs per child, while the full benchmark checks the target.
GC and malloc trimming run only after the final unperturbed sample.

## Split before changes

Environment: CPython 3.11.15, Django 5.2.19, psycopg 3.3.7 (binary).
The three-round median ORM peak is 234.1 MiB, against 157.0 for Django add.

Per-child private growth from startup to task 1, KiB:

| Control | Private growth | Inherited anonymous/heap CoW | Other private growth |
|---|---:|---:|---:|
| Traced add | 580 | 336 | 244 |
| Raw psycopg connection | 4244 | 3792 | 452 |
| Django connection | 5672 | 5136 | 536 |
| Compile only | 1612 | 1356 | 256 |
| Traced ORM count | 7480 | 6876 | 604 |

A differential split of the ORM first task is: driver/native connection 4244 KiB,
additional Django connection state 1428 KiB, and ORM/tracing 1808 KiB.
Within the total, 6876 KiB (92%) is measured inherited CoW, and 604 KiB is
other private growth. These are two views of the same memory, not additive
categories. Raw-driver CoW alone is 3792 KiB; it is not all Django model state.
The glibc live-allocation increase is only 375 KiB for ORM (400 KiB raw driver).

Between tasks 1 and 2000, ORM adds 640 KiB private and 466 KiB PSS:
384 KiB is newly copied inherited anonymous/heap pages, leaving 256 KiB of
other private growth. Glibc live bytes do not grow in this interval; Python
allocated-block count grows by 3748. No additional named model/wrapper cache
keys appear after task 1. A full GC collects zero objects and increases PSS
by 39 KiB; subsequent malloc trimming recovers zero KiB. Allocator retention
is therefore not an explanation for tens of MiB per worker in this probe.

The first task adds `_forward_fields_map` and `managers_map` on `Item._meta`,
and `isolation_level`, `pg_version`, `timezone`, `timezone_name`, and
`wrap_database_errors` on the wrapper. No new ops/features cached-property
keys appear. Compile-only also builds `concrete_fields` and `fields`.
Django lazy state is included in the compile/connection controls; cache-key
counts alone cannot give its exact byte size or separate it from refcount CoW.

Written pages between tasks 1 and 2000 are dominated by strings (719 pages),
code objects (459), dicts (193), functions (166), types (133), and tuples (124);
89 pages cannot be assigned to a GC-visible object. These are overlapping
page contents, not exclusive causes. Shared library mappings gain about
3 MiB of resident clean code (mostly bundled libcrypto); those pages are
shared across children and must not be multiplied by ten.

In the full c10 benchmark, the median idle-to-peak jump is 119.5 MiB for ORM
and 42.9 MiB for add. The incremental ORM cost is therefore 76.6 MiB. Scaling
the controls' private first-task costs by ten gives about 41.4 MiB driver,
13.9 MiB additional Django connection setup, and 17.7 MiB ORM/tracing. The
remaining roughly 3.6 MiB reflects differences in initialization, sharing,
and later tasks; it is not attributed to a cache or to refcounts alone.

The result explains the scale: roughly 7–8 MiB of additional private state
per ORM child, predominantly inherited CoW, rather than a growing cache or
100 MiB of live query results. The direct probe already touches the generic
path during initialization, so the full c10 benchmark remains authoritative
for the total startup-to-load jump.

## Attempts

1. Full GC followed by malloc trimming after task 2000: zero recovered KiB;
   discarded. The child already trims after each task. No GC-policy change.
2. Preload only Django's argument-keyed psycopg adapter templates for
   `(True, UTC)` and `(False, None)` in the zygote. Experimental ORM peaks:
   229.7, 228.3, 227.9 MiB (median 228.3, −5.8 MiB / 2.5%). ORM throughput:
   536, 546, 563 tasks/s (median 546; reference median 430). Django add median
   peak 158.6 MiB versus baseline 157.0 (within noise); FastAPI 120.3 versus
   121.0 (within noise). Discarded after the compatibility check below.

The experiment inserted calls to `get_adapters_template(True, timezone.utc)`
and `get_adapters_template(False, None)` after the prepared initializer in the
installed Python 3.11 zygote. A source implementation with a private fixup hook
was prepared, then removed.
No connection or query was opened in the zygote. Fixed cache keys handle changed
timezones/settings, but do not handle changed global psycopg adapters.

Compatibility rejection, reproduced without a database connection: warm
`get_adapters_template(True, timezone.utc)`; register a custom text loader with
`psycopg.adapters.register_loader('text', CustomLoader)`; request the same template.
Its `get_loader(25, Format.TEXT)` does not return `CustomLoader`. Clear the cache
and request it again, and it does. A user registering adapters in
`worker_process_init` would therefore get different query results after warming.
No production code or documented behavior difference is retained.

Reproduce the rejection in `target/e2e/venv-rs/bin/python`:

```python
from datetime import timezone
from psycopg import adapters
from psycopg.adapt import Loader
from psycopg.pq import Format
from django.db.backends.postgresql.psycopg_any import get_adapters_template

class CustomLoader(Loader):
    def load(self, data):
        return 'custom'

get_adapters_template.cache_clear()
get_adapters_template(True, timezone.utc)
adapters.register_loader('text', CustomLoader)
assert get_adapters_template(True, timezone.utc).get_loader(25, Format.TEXT) is not CustomLoader
get_adapters_template.cache_clear()
assert get_adapters_template(True, timezone.utc).get_loader(25, Format.TEXT) is CustomLoader
```

The measured preloading step is below the 3% stopping threshold. Further exploratory
changes stop here as requested. The 190 MiB target has not been reached.
Warming arbitrary task tracers or model queries would execute user signals or
custom model/field code, so those were not attempted. Native-driver CoW and
Python 3.11 refcount/specialization writes remain the main unresolved costs.

Raw data: `16-before.jsonl`, `16-adapters.jsonl`, `16-attribution.json`.

## Verification notes

The first benchmark attempt exited 127 because this worktree had no Python 3.11
e2e environments. `tools/e2e/setup` created them. A fresh `e2e_bench` database
also had to be created: the benchmark's first scenario is FastAPI and its
one-time creation flag is consumed before Django. No harness was modified.
All recorded rounds subsequently completed. The initial ORM diagnostic failed
before that database existed; only successful reruns appear in the raw data.

The speculative adapter warm-up and its source hook were removed before the
final package build. Only the diagnostic probe, its test, and measurements
remain. There are no new dependencies or changes to observable worker behavior.

Verified commands (all exit 0 unless noted):

| Command | Actual result |
|---|---|
| `cargo build --release` | Passed |
| `cargo clippy --all-targets -- -D warnings` | Passed |
| `tools/dev/with-brokers cargo test` | 252 passed, 4 ignored |
| `tools/dev/build-package` | Installed the full editable package in `.venv-rs` |
| `tools/e2e/setup` | Built and installed Python 3.11 rs/ref environments |
| `tools/dev/with-brokers tools/e2e/run --no-setup --only django-redis-redis,django-redis-db,fastapi-redis-redis` | 144 passed, 0 unexpected failures |
| `tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs/test_prepared_initializer.py python/tests_rs/test_fork_memory.py python/tests_rs/test_worker_prefork.py python/tests_rs/test_django_memory_probe.py -q` | 28 passed in 176.40 s |
| `./compat api` | 0 differences in 137 modules plus settings/signals/CLI |

The e2e runner reports its existing known reference failure for
`django-redis-db: celery call` (45/46 checks in that scenario), and exits 0.
No full gates, upstream/integration suites, soak, or push were run.
No Italian text was introduced.

Probe command, repeated for `add`, `raw`, `connect`, `compile`, and `orm`:

```sh
tools/dev/with-brokers target/e2e/venv-rs/bin/python bench/memprobe/django_orm.py orm
```

## Final before/after

Three-round medians. The final worker implementation is unchanged.

| Project/task | Reference idle/peak before → after | rs idle/peak before → after | Reference task/s before → after | rs task/s before → after |
|---|---|---|---|---|
| fastapi add | 271.5/289.1 → 271.8/287.0 | 84.5/121.0 → 84.7/119.8 | 626 → 645 | 2131 → 2093 |
| django add | 399.9/415.3 → 400.2/415.1 | 114.1/157.0 → 114.3/157.3 | 615 → 624 | 1970 → 1967 |
| django orm | 400.0/467.8 → 400.6/467.5 | 114.6/234.1 → 114.4/234.2 | 430 → 435 | 551 → 549 |

Django ORM peak remains 234.2 MiB, above the 190 MiB target. Its 0.1 MiB
difference from baseline is noise. Django add and FastAPI peaks also remain
within 2 MiB of baseline. All rs throughput medians exceed the reference.

`tools/dev/with-brokers tools/e2e/bench.sh` passed before and after (18/18
measurements each). The adapter experiment used the same command with
`E2E_VENVS=rs` (9/9 measurements). Raw final data: `16-after.jsonl`.

## Python 3.13

```sh
tools/dev/with-brokers .venv-rs/bin/python bench/memprobe/loadpeak.py "$PWD/.venv-rs" 10 "$PWD/bench/memprobe" 10000
```

| Installation | Idle before/after | Peak before/after | task/s before/after |
|---|---:|---:|---:|
| Wheel / wheel | 50.9 / 49.2 | 56.6 / 56.1 | 2682 / 2529 |

No measurable PSS regression. A first final run used the editable installation
and measured idle 46.0 / peak 52.2 MiB, 2711 tasks/s. That is not a comparable
installation, so it is not counted as a memory improvement. The final wheel
was installed temporarily for the table, then `tools/dev/build-package` restored
the editable package successfully. Raw results: `16-python313.txt`.

## Retained changes and limits

- `bench/memprobe/django_orm.py`: staged CoW, allocation, cache, and mapping diagnostics.
- `python/tests_rs/test_django_memory_probe.py`: validates shared-to-private page attribution in a separate process.
- This report and `16-before.jsonl`, `16-after.jsonl`, `16-adapters.jsonl`,
  `16-attribution.json`, `16-python313.txt`: measured evidence.

The 190 MiB target remains open. The safe result of this task is the measured
split and a reproducible reason to reject the small warm-up, with no worker
behavior change. Exact refcount-only bytes and exact live sizes of Django's
lazy caches remain unresolved; page accounting gives bounds and controls,
not an exclusive allocation traceback. Further attempts stopped at the
specified sub-3% threshold, before the 45-minute exploration limit.

## Python 3.13 on the same projects (orchestrator, `tools/e2e/bench.sh`, three alternating runs)

| | reference idle / peak | celery-rs idle / peak | task/s ref → rs |
|---|---:|---:|---:|
| FastAPI, `add` | 288 / 298 MB | 63 / 85 MB | 633 → 2017 |
| Django, `add` | 422 / 434 MB | 88 / 116 MB | 628 → 1945 |
| Django, ORM `count()` | 422 / 479 MB | 88 / 165 MB | 445 → 551 |

Immortal objects remove most of the inherited copy-on-write: the ORM peak drops from 234 to 165 MB with no code change.
