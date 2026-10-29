# 09a — Hardening: original Celery integration suite (`t/integration`) on celery-rs

Phase 9 of `PLAN.md`. Phase output gate: **`t/integration` complete**, performed with celery-rs, with the same results as the reference. Project priority unchanged: worker and beat RAM, then performance; no correction can increase the PSS above the noise (> 0,5 MB) without motivating it. Read `AGENTS.md` (also "Known Failures"), `PLAN.md`. Constraints unchanged: Identical API, behavior and traffic; real kombu items, no rewritten names; `test_kombu_identity.py`, `celery/`, `celery/t/` untouchables. This spec authorizes you to **add** the `integration` subcommand to `tools/compat/` (without modifying the existing ones).

## 1. `./compat integration`

Runs `celery/t/integration` (copies into `tools/compat/out/integration-src` as `upstream` does, never modifying the original) with `.venv-ref` and with `.venv-rs`, on the lanes of `tools/dev/with-brokers`, in two configurations:
- `TEST_BROKER=redis://…` `TEST_BACKEND=redis://…`
- `TEST_BROKER=pyamqp://…` (lane AMQP) `TEST_BACKEND=redis://…`

Test result (pass/fail/skip/error) for ref and rs; **regression = fails in rs and passes in ref**; unstable tests repeat (like `upstream`) before being counted. Report in `tools/compat/out/integration-report.md` (+ JSON) and summary line like the other gates. Tests that require absent services (database, memcached, Django, security with certificates...) skip or fail in the same way in ref and rs: they are not regressions; list them. Add `integration` to `tools/dev/gates`.

## 2. Corrections

Bring regressions to **0**. Expected areas are the worker embedded in the tests (`celery.contrib.testing.worker.start_worker`, pooled threaded worker `solo`), canvas/chord, `test_connection_recovery.py` (broker restarting: to restart the broker only use containers in **your** lane), `test_inspect.py`, `test_beat.py`, leak tests (`test_mem_leak_in_exception_handling.py`, `test_events_state_memory_leak.py`, `test_backend_leak_in_fixture_teardown.py`). For each fix: test in `python/tests_rs/` which covers it, and verify that the PSS of the c10 and beat worker does not grow (`bench/bench.py --pool prefork
--concurrency 10 --task pure`, `bench/bench_beat.py`).

## Gate

`tools/dev/gates` (all green except for "Known Failures"), including the new `integration` with 0 regressions in both configurations.
## Work Rules (model quota limited)

Long commands **once and blocking**, no periodic progress checks. Don't investigate "Known Failures". Short final report: integration results table ref vs rs by configuration (passed/failed/skipped, regressions before/after), corrections made and why, PSS before/after, gate with real outcome, what remains open. Commit to the branch (`09a: …`).
