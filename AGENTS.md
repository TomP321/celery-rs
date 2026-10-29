# celery-rs — agent rules

You are working on celery-rs: Celery rewritten with a Rust core and an identical Python API. Read `PLAN.md`.

## Structure

- `celery/` — **Celery 5.7.0rc1 reference source. READ ONLY. Never modify it.**
  It is the specification: when behavior is unclear, the reference is correct.
- `crates/` — Rust workspace (`celery-proto`, `celery-transport`, `celery-backend`, `celery-worker`,
  `celery-beat`, `celery-py` = PyO3 module `celery._rs`).
- `python/celery/` — drop-in Python shell (same structure as `celery/celery/`).
- `tools/compat/` — conformance tools (`./compat api|upstream`). **Do not modify them** or
  the tests in `celery/t/` to make them pass.
- `specs/` — one spec per task. Implement only what your assigned spec requests.

## Environment

- Venvs: `.venv-ref` (original Celery), `.venv-rs` (celery-rs). Python 3.13.
- Test lanes: `tools/dev/lanes up|down|status`; configure the count with
  `CELERY_RS_LANES` (default 4). `tools/dev/with-brokers command ...` assigns an exclusive,
  reentrant lane and cleans up processes and connections afterward. It exports
  `CELERY_RS_LANE`, `CELERY_RS_REDIS_URL` (without a database), `CELERY_RS_AMQP_URL`,
  `CELERY_RS_RABBIT_API` (with `/api`). Without overrides, addresses stay on lane 0.
  Each subsequent lane increments every port by 1.
  `tools/dev/gates` runs the gates; `--only tests_rs` selects all Python groups.
  Other names match the final table. Logs are in `target/gates/<name>.log`,
  with permanent copies in a subdirectory per invocation.
- Test brokers: Redis `127.0.0.1:6390`, RabbitMQ `127.0.0.1:5673` (guest/guest).
  Start: `docker compose -f infra/docker-compose.yml up -d`.
  Brokers are shared with other agents: run tests using Redis/RabbitMQ under a lock:
  `tools/dev/with-brokers cargo test`, `tools/dev/with-brokers .venv-…/bin/python -m pytest …`
  (`./compat wire` acquires it automatically; the lock is reentrant). This also applies to
  benchmarks and manual probes. Use unique queue/key names; FLUSHALL/FLUSHDB only under the lock.
- Rust: `cargo build --release`, `cargo test`, `cargo clippy --all-targets -- -D warnings`.
- Python: `tools/dev/build-native` installs only `_celery_rs` in `.venv-ref` (differential tests);
  `tools/dev/build-package` installs the full celery-rs package in `.venv-rs`.
  **Do not run `maturin develop -m crates/celery-py/Cargo.toml` from the repository root**:
  it would install the celery-rs `celery` package into `.venv-ref`, overwriting the reference.

## Code rules

- Memory and performance are requirements: avoid unnecessary clones/allocations; prefer
  `bytes::Bytes` and borrows (`&str`) over copied `String`/`Vec`. Do not use `serde_json::Value`
  as an intermediate structure in hot paths. Bound data structures; no unlimited growth.
- No `unwrap()`/`expect()` in library code (tests only). Use `thiserror` for errors.
- Use `unsafe` only when essential, with a `// SAFETY:` comment explaining why it is sound.
- Dependencies: use `[workspace.dependencies]`; justify any new dependency in the final message.
- Comments and docs in English, minimal, explaining only *why* (style rules: `specs/13-english-style.md`).
  Names and APIs match the reference.
- Every Rust module has unit tests. Behavior that can be checked against the reference has a
  test comparing its output with `.venv-ref`.

## Before declaring completion

1. `cargo build --release` and `cargo clippy --all-targets -- -D warnings` pass cleanly.
2. `cargo test` passes.
3. The verification commands in the spec pass.
4. Report what changed, files touched, commands run and their actual results, and open issues.
   Never claim that unrun tests passed.

## Known failures (report only; do not investigate)

- `t/unit/contrib/test_pytest.py::test_pytest_celery_marker_registration` (`./compat upstream`):
  also fails with reference Celery; caused by the pytest environment, not the code.
