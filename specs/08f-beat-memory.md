# 08f — `celery beat` (and `worker -B`) RAM at minimum

Project priority: **worker and beat RAM compared to the original Celery**; performance comes later. This spec is dedicated to beat. Read `AGENTS.md`, `PLAN.md`, `specs/06-beat.md`, `specs/08b-memory.md` (honest lazy import decisions), and `specs/08b-review.md`. Constraints unchanged: API, behavior, broker traffic and schedule file format (`celerybeat-schedule`, shelf) identical to the reference; **real kombu/billiard objects, no rewritten `__module__`/`__qualname__`/`__name__`, no replacement classes** (`python/tests_rs/test_kombu_identity.py` does not change); `celery/`, `celery/t/`, `tools/compat/` are not changed.

## Starting measurement (main @ 8ca8eae, `bench/bench_beat.py`, PSS after startup)

| | ref | rs |
|---|---|---|
| 3 entries, Redis | 50,8 MB | 31,7 MB |
| 3 entries, AMQP | 43,0 MB | 31,6 MB |
| 1000 entries, Redis | 58,1 MB | 42,6 MB |
| 1000 entries, AMQP | 50,2 MB | 42,9 MB |
| send/s Redis / AMQP | 433 / 470 | 481 / 561 |
| `Scheduler.tick` 1000 entries | 1130 µs | ~1167 µs |

rs grows by **~11 KB per entry** (31,7 → 42,6 MB). The beat process at 3 entries still weighs 31,7 MB.

## Objectives

- beat 3 entries ≤ **24 MB** on Redis and AMQP;
- growth per item ≤ **3 KB** (1000 items ≤ base + 3 MB); also fits 10 000 entries;
- PSS stable over time: after 30 minutes of sending (or accelerated equivalent) no growth;
- `worker -B` (embedded beat, now threaded): measure the PSS of the worker with and without `-B` (`-P solo` and prefork c10) and reduce the additional cost;
- send/s and ticks don't get worse.

## Where to earn (measure first, only adopt what the bench confirms)

1. **Breakdown** of the beat process PSS: loaded Python modules (list and cost, `python -X importtime`, `tracemalloc`), Python heap, mapped libraries, Rust heap. Report the before/after table.
2. **Import**: what beat loads without using it (CLI, workers, canvas, backend, pygments, clicks beyond necessary, etc.) → honest lazy imports as in 08b.
3. **By entry**: `ScheduleEntry` and the `schedule`/`crontab`/`solar` objects hold many Python attributes; the Rust core already has crontab (`crates/celery-beat`). Consider keeping the schedule state (next deadline, expanded crontab fields) in compact Rust structures, with `ScheduleEntry`/`crontab` Python remaining identical public objects (same attributes, `repr`, pickles, comparisons) but without duplicating data; `__slots__` only where the reference does not observably expose `__dict__`. Check the cost per entry of: entry, schedule, shelf (`PersistentScheduler` in-memory cache), `options`/`kwargs` dictionaries.
4. **Persistence**: `PersistentScheduler` synchronizes the shelf; check that it does not keep duplicate copies of entries in memory (module cache `dbm`/`shelve` with `writeback`).
5. **`-B`**: the embedded beat shares the process with the worker; what beat loads weighs on the master. Consider starting from the zygote as a separate process only if it reduces the total PSS.

## Test

- `python/tests_rs/test_beat.py` (existing) remains green; add test for each change: attributes/`repr`/pickle/equality of `ScheduleEntry` and `crontab` identical to the reference, schedule file readable by ref and rs in both directions, no growth of PSS beyond a threshold.
- `./compat wire-beat` 0 differences.

## Gate

`tools/dev/gates` (all green; command runs everything in parallel across lanes in ~10 minutes). For intermediate tests use `tools/dev/gates --only <nome>` or individual tests under `tools/dev/with-brokers`.

## Work Rules (model quota limited)

Run long commands **once and in a blocking manner** (just one command waiting for the end), not in the background with periodic checks: each check costs one model call. Final short report: ref vs rs before/after table (3, 1000, 10 000 entries; Redis/AMQP; `-B`), PSS breakdown, what you changed and why, discarded variants with numbers, gates with real outcome, what remains open. Commit to the branch (`08f: …`).
