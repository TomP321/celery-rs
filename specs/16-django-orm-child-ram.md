# 16 — Child RAM with a real Django project using the ORM

Read `AGENTS.md`, `docs/DIFFERENCES.md`, `bench/results/11d-ram-over-speed.md`. Priority: RAM of a prefork worker `-c 10` in a real project,
on **Python 3.11** (user production). The floor is the reference: never below its task/s.

## Measured (`tools/dev/with-brokers tools/e2e/bench.sh`, Redis, Postgres, three alternating runs)

| | reference idle / peak | celery-rs idle / peak | task/s ref → rs |
|---|---:|---:|---:|
| FastAPI, `add` | 272 / 289 MB | 85 / 120 MB | 650 → 2100 |
| Django, `add` | 401 / 418 MB | 114 / 157 MB | 700 → 2050 |
| Django, ORM `count()` | 401 / 468 MB | 114 / 234 MB | 450 → 540 |

Under ORM load each child reaches ~20 MB PSS (~15 MB private), against ~8 MB for `add` in the same project. Master 28 MB, zygote 16 MB. No module is imported
in a child after `worker_process_init`: lazy imports are not the cause. The growth is not yet explained.

## Work

1. **Explain the +100 MB first**, with numbers: per child, between task 1 and task 2000, how much is inherited pages dirtied by refcounts (`bench/memprobe/sd.py`, soft-dirty → object types), Django's
   lazily built state (model `_meta` caches, compiled regexes, `connection.ops`/features, `cached_property`), the psycopg/libpq connection, and allocator retention. Report the split before changing anything.
2. Reduce it. Ideas to measure, keep only what lowers PSS:
   - Pre-build deterministic per-process state in the zygote, in the Django fixup (`python/celery/fixups/django.py`) or in `prepare_process_initializer`. **Never open a database connection in the
     zygote or master**; no user-visible change: signals still fire in the child, no ORM query is executed, nothing that depends on settings a user may change after fork.
   - Warm the generic task path once in the zygote (`trace`, serializers, the known ~5–8 MB).
   - GC timing around the first tasks, `malloc_trim` when a burst ends, arena settings.
3. Do not change observable behaviour; anything observable goes in `docs/DIFFERENCES.md`. Django-specific code stays in the fixup and costs nothing for non-Django projects.

Target: Django ORM peak ≤ 190 MB (−20%), Django `add` and FastAPI no worse, task/s ≥ the reference in each row. Python 3.13 no worse (`E2E_PYTHON=3.13 tools/e2e/setup` in a separate checkout of `target/e2e` is not needed: use `.venv-rs` with `bench/memprobe/loadpeak.py`).
Stop when a further step gains < 3% of the peak, or at 45 minutes, whichever comes first, and report what is left. If the target cannot be reached, say so with the split from step 1: that is an acceptable result.

## Verification
`cargo build --release`, `cargo clippy --all-targets -- -D warnings`, `tools/dev/build-package`, `tools/e2e/setup`, `tools/dev/with-brokers tools/e2e/run --no-setup --only django-redis-redis,django-redis-db,fastapi-redis-redis`,
`tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs/test_prepared_initializer.py python/tests_rs/test_fork_memory.py python/tests_rs/test_worker_prefork.py -q`, `./compat api`. Add unit tests for what you add. Measure with
`tools/dev/with-brokers tools/e2e/bench.sh` before and after, same worktree; RAM differences under ±2 MB are noise, do not claim them.

Do not run `tools/dev/gates`, full `./compat upstream|integration` or soak: the orchestrator does. Codex quota is limited: stay on this list. Blocking commands, no push. Commit as `16: …`, with `bench/results/16-django-orm-ram.md`: the split, each attempt including discarded ones with numbers, before/after table.
