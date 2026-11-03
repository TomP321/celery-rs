# 15 — Retry state race and third-party `_store_result` signatures

Read `AGENTS.md` and `specs/13-english-style.md`. Two bugs found with real Django/FastAPI projects. Fix only
these. No RAM or throughput regression; wire traffic stays identical.

## 1. A finished retry can end up stuck in `RETRY` (celery-rs only)

`Task.retry` publishes the retry message and raises; `handle_retry` in `app/trace.py` stores `RETRY` afterwards.
The Rust pool delivers so fast that the new attempt stores `SUCCESS` first, then `RETRY` overwrites it (orchestrator
hypothesis from log timing: confirm it first, with evidence). `.get()` then times out and `AsyncResult.state`
stays `RETRY`.

Measured (Python 3.11, `-c 2`/`-c 3`, fresh worker, first retry in that worker, `self.retry` with delay 0, Redis
broker): Postgres + MongoDB 22/24 stuck on celery-rs, 0/24 on the reference. Redis backend 0/12 on both. SQLite,
S3 and DynamoDB also stuck in one run. Warm workers and a 1 s countdown: 0.

Required invariant: **a retried task's later terminal state is never overwritten by its own `RETRY`**, for every
backend including third-party ones, with no window left open. A check-then-set guard alone leaves a window: not
enough. Preferred fix: store `RETRY` before publishing the retry message in `Task.retry` when running in a worker,
and have `handle_retry` not store it a second time (same store conditions as `handle_retry`; nothing for eager or
direct calls; `throw=False` still works). `AsyncResult.result`, `.traceback`, `task_retry`, `on_retry` and the log line
stay as they are wherever possible. Propose something else only if it is demonstrably better. Anything observable that
changes (for example the stored traceback, or the state left when the publish fails and `Reject` is raised) goes in
`docs/DIFFERENCES.md`.

Regression test in `python/tests_rs/`, deterministic and without external services: a backend whose `_store_result`
sleeps ~300 ms when storing `RETRY`, running under a real prefork worker. It must fail before the fix (show it) and
pass after. Cover the `solo` pool too.

## 2. `django-celery-results` stores nothing (reference has this too)

`Backend.store_result` calls `self._store_result(..., state=state)` by keyword. `django_celery_results`
2.6.0 declares `_store_result(self, task_id, result, status, traceback=None, request=None, using=None)`, so every store
raises `TypeError` and no result is ever saved. Pass `task_id`, `result`, `state` positionally so any third-party backend
naming that parameter differently works, keeping `BackendStoreError`'s `task_id`/`state` context in
`_ensure_retryable`. This intentionally departs from the reference: add it to `docs/DIFFERENCES.md`. Add a test with a
backend using the `status` name. Do not add `django-celery-results` as a dependency.

## Verification
`cargo build --release`, `cargo clippy --all-targets -- -D warnings`, `tools/dev/build-package`,
`tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs/<new test file> python/tests_rs/test_backend_redis.py python/tests_rs/test_worker_prefork.py -q`,
`./compat api`, plus the Celery unit tests for the code you touched (`celery/t/unit/backends/test_base.py`,
`celery/t/unit/tasks/`): the reference tests are read-only, and a test that fails because of the new order must be reported to
the orchestrator, not edited.

Do not run `tools/dev/gates`, full `./compat upstream|integration` or soak: the orchestrator does. Codex quota is limited:
stay on this list, about 30 minutes. Blocking commands, no push. Commit as `15: …`. Report the evidence for the cause, what changed,
commands with actual results, and open issues.
