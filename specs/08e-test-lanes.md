# 08e — Parallel gate: separate brokers for "lane"

Test infrastructure, no changes to celery-rs code. Today all tests using brokers pass through a single Redis `:6390` / RabbitMQ `:5673` (+ management `:15673`) pair protected by a lock (`tools/dev/with-brokers`): the complete tour of the gates lasts ~60 minutes, almost all in a row. Machine: 24 core, 117 GB RAM. Objective: **complete gate tour ≤ 20 minutes** with the same outcome as today, and the possibility for multiple agents/worktrees to work in parallel without disturbing each other.

Read `AGENTS.md`. This spec explicitly authorizes you to modify `tools/compat/` and `tools/dev/` **only** to make broker addresses configurable (no changes to comparison logic, cases, normalizations, or thresholds). `celery/` and `celery/t/` remain untouchable.

## Project

1. **Lanes**: `infra/docker-compose.yml` parameterized (or one file per lane) for N lanes (default 4, configurable), each with its own Redis and RabbitMQ on its own ports. The 0 lane remains on the current ports (`6390`, `5673`, `15673`), so those who do not use the lanes do not change anything. Start/Stop: `tools/dev/lanes up|down|status`.
2. **`tools/dev/with-brokers`**: takes exclusively **a free lane** (non-blocking flock on one file per lane, wait if all occupied), exports the lane environment variables and executes the command; returning as today (within a command already under lock, reuse the same lane). Variables (unique names, documented in `AGENTS.md`): `CELERY_RS_LANE`, `CELERY_RS_REDIS_URL` (e.g. `redis://127.0.0.1:6390`, without db), `CELERY_RS_AMQP_URL` (e.g. `amqp://guest:guest@127.0.0.1:5673//`), `CELERY_RS_RABBIT_API` (e.g. `http://127.0.0.1:15673/api`).
3. **All points that have fixed ports today** read these variables with default on the 0 lane: `python/tests_rs/*`, `crates/*/tests` and Rust tests, `crates/celery-py/tests`, `tools/compat/*`, `bench/*`. No port `6390`/`5673`/`15673` must remain written anywhere other than as default (check with `rg`).
4. **`tools/dev/gates`**: executes all gates (list below) **in parallel on the lanes**, each with its own log in `target/gates/<nome>.log`, and at the end prints a name / outcome / duration / summary line table (passes, differences, regressions) and ends with code ≠ 0 if a gate fails. `tests_rs` must be divided into several groups (by row, balanced by duration) that run in different lanes. Option `--only nome,...` for a subset. It is **a single blocking command**: whoever launches it waits for the end without having to check the progress.
5. **Cleanup**: at the end of each gate (even if stopped with Ctrl-C/SIGTERM) no child worker process remains alive or connected to the lane brokers (dedicated process group and kill of the entire group). Before using a lane, `with-brokers` checks that there are no residual AMQP connections or orphaned exclusive queues and, if there are, it closes them (via lane management API) and writes it to the log.

## Gates managed by `tools/dev/gates`

```
cargo build --release ; cargo clippy --all-targets -- -D warnings ; cargo test
tools/dev/build-package ; tools/dev/build-native
.venv-ref/bin/python -m pytest crates/celery-py/tests -q
.venv-rs/bin/python -m pytest python/tests_rs -q        (in groups across multiple lanes)
./compat api ; ./compat wire ; ./compat upstream --venv .venv-rs
./compat wire-worker --pool solo ; ./compat wire-worker --pool prefork
./compat wire-beat ; ./compat cli
```
build/clippy/build-package/build-native first (in sequence), then everything else in parallel.

## Check

- `tools/dev/lanes up` then `tools/dev/gates`: same results as today (note: in `main` a `test_worker_pool.py::test_worker_process_signals[prefork]` failure is known with segfault of the billiard children in the complete suite, already present before: report it, do not correct it here) and total duration ≤ 20 minutes; the table shows.
- Two `tools/dev/gates --only tests_rs`s launched at the same time from two shells do not interfere.
- Ctrl-C halfway through `tools/dev/gates`: no orphan processes (`ps`) or residual connections.

## Work Rules (model quota limited)

Run long commands **once and in a blocking manner** (just one command waiting for the end, with appropriate timeout), not in the background with periodic progress checks: each check costs one model call. Short final report: what you did, files touched, gate table with real outcomes and durations, what remains open. Commit to the branch (`08e: …`).
