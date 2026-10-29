# 06 — Beat: Rust publishing, crontab in core, persistence compatible

Phase 6 of `PLAN.md`. Today `python/celery/beat.py`, `schedules.py` and `apps/beat.py` are copies of the reference: `celery beat` works but publishes with a `kombu.Producer` (redis-py/py-amqp) and calculates crontabs in Python. Target: `celery beat` of celery-rs with identical behavior, broker messages and schedule files to the reference, but with less memory and less CPU per tick.

## References

- `celery/celery/beat.py`: `ScheduleEntry` (`is_due`, `_next_instance`, `update`, `editable_fields`, `__reduce__`), `Scheduler` (`tick`, `populate_heap`, `apply_entry`, `apply_async`, `_do_send`, `send_task`, `should_sync`/`sync`, `_ensure_connected` with the log `beat: Connection error: %s. Trying again in %s seconds...`, `connection`, `producer`, `Producer`, `adjust`, `_when`, `merge_inplace`, `install_default_entries`, `close`), `PersistentScheduler` (shelve, `_store`, keys `entries`/`__version__`/`tz`/`utc_enabled`, reset the db if version/tz changes, `_destroy_open_corrupted_schedule`), `Service` (`start`, `sync`, `stop`, `get_scheduler`), `EmbeddedService`.
- `celery/celery/schedules.py`: `schedule`, `crontab_parser` (messages of `ParseException` and `ValueError`), `crontab` (`_expand_cronspec`, `remaining_delta`, `_delta_to_next`, `remaining_estimate`, `is_due` with `beat_cron_starting_deadline`, `__reduce__`, `__setstate__`, `__eq__`, `from_string`), `solar`.
- `celery/celery/utils/time.py`: `remaining`, `ffwd`, `maybe_make_aware`, `localize`, DST.
- `python/celery/app/_rs_producer.py` (phase 3b): `FastConnection`, `RsProducer`, activation conditions (`CELERY_RS_PRODUCER`), error mapping.
- Upstream tests: `celery/t/unit/app/test_beat.py`, `celery/t/unit/app/test_schedules.py`, `celery/t/unit/bin/test_beat.py`, `celery/t/unit/apps/test_beat.py`.

## Project

1. **Publishing with Rust producer.** `Scheduler.connection`/`Scheduler.producer` use `FastConnection`/`RsProducer` of phase 3b when transport is suitable (same conditions as `use_fast_producer`, `CELERY_RS_PRODUCER=0` back to kombu) **and** `Producer` has not been redefined by a subclass (django-celery-beat, redbeat and the like must continue to work with the kombu path). They remain identical: `_ensure_connected` (retry, `broker_connection_max_retries`, log text), publication error in `apply_entry` (log `Message Error: ...` and continuing beat), reconnection after lost broker, closure in `close()`. The beat process no longer needs to import redis-py nor py-amqp to the Rust path (check this with `sys.modules` in a test). Gate: `./compat wire-beat` → 0 differences.
2. **Crontab in Rust core.** New module `crates/celery-beat` (+ binding in `celery-py`):
   - field parser with the exact semantics of `crontab_parser` (range, step, `*/n`, wrap `fr > to`, names of days/months, negative numbers, same error messages and same exception classes on the Python side);
   - calculation of the next instant of the **calendar** part of `remaining_delta` and `_delta_to_next` (fields as bitmask: minutes `u64`, hours/days/months in integers), which returns the same arguments as `ffwd` as the reference (including the `RuntimeError` after 2000 attempts). Time zones, DST, `maybe_make_aware`, and `remaining()` remain the reference Python code: Rust does not handle tz and does not add tz dependencies.
   - Public API unchanged: `crontab.minute`/`hour`/`day_of_week`/`day_of_month`/`month_of_year` `set` of `int`, `_orig_*` remain unchanged, `__reduce__`/`__setstate__`/`__eq__`/`repr` identical, so pickle and shelf remain compatible in both directions.
   - `schedule` (range) and `solar` remain Python.
3. **Persistence compatible.** `PersistentScheduler` remains on `shelve` with the same content. Test: the file written by beat ref is read by beat rs (same entries, `last_run_at`, `total_run_count`, no reset) and vice versa; corrupt db → same reference recovery.
4. **Memory and CPU of the beat process.** Reduce imports to the minimum necessary on the Rust path (no changes to public names: attributes remain accessible, at most loaded lazily). Measurement with a new `bench/bench_beat.py` (venv ref and rs, Redis and AMQP): PSS of the idle beat process with 3 entries and with 1000 entries (500 interval 1 s + 500 mixed crontab), CPU per tick (`Scheduler.tick` with 1000 entries, µs), sustained send/s, and `crontab.remaining_estimate`/`is_due` (ns/call) microbench. Report ref vs rs numbers. Objective: PSS rs < ref, `remaining_estimate` at least 5× faster.
5. No changes to signatures, names, public defaults or log texts; no new settings.

## Tests required

- `crates/celery-beat`: Unit tests of parser and calendar calculation (leap years, month 29/30/31, `day_of_week` + `day_of_month` together, months without valid days → `RuntimeError`).
- `crates/celery-py/tests/test_cron.py` (`.venv-ref`, differential in the same process: `celery.schedules` of the reference vs functions `_celery_rs`): ≥ 200.000 random cases with fixed seed on parser (valid and invalid strings, same errors) and on `remaining_delta`/ `remaining_estimate`/`is_due` with time zones `UTC`, `Europe/Rome`, `America/New_York`, `Australia/Lord_Howe`, `Asia/Kolkata`, `enable_utc` true/false, `last_run_at` naive and aware in a different time zone, moments around DST changes, `beat_cron_starting_deadline`. Zero differences.
- `python/tests_rs/test_beat.py` (`.venv-rs`): `celery beat` as a subprocess with a reference worker: interval, crontab (one entry that fires per minute: slow test allowed, < 75 s), `relative=True`, `options` (queue, priority, expires, headers), non-existent task, broker unreachable at startup then available (use test-controlled TCP proxy, **don't** stop shared brokers), broker lost during execution, ref↔rs schedule file, `-S` custom scheduler with `Producer` redefined (kombu path), `CELERY_RS_PRODUCER=0`, embedded beat (`celery worker -B`, with the rs worker). Each scenario compared to reference beats.

## Gate (all mandatory, real outcome in final message)

```
cargo build --release && cargo clippy --all-targets -- -D warnings
tools/dev/with-brokers cargo test
tools/dev/build-package && tools/dev/build-native
tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q
tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q
./compat api && ./compat wire && ./compat upstream --venv .venv-rs   # 0 / 0 / 0 regressions
./compat wire-worker --pool prefork                                    # 0 differences
./compat wire-beat                                                     # 0 differences
```
Also, in the final message, the numbers of `bench/bench_beat.py` (ref vs rs).

## Out of scope

CLI (`celery beat` remains the reference click one, phase 7), django-celery-beat, `solar` in Rust, worker optimizations.
