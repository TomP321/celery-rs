# 09b — Hardening: stable memory over time, parser fuzz, segfault billiard

Last spec of the phase 9. Project priority: **Worker and beat RAM** (stable and minimum), then performance. Read `AGENTS.md` (also "Known Failures"), `PLAN.md`. Constraints unchanged: API, behavior and traffic identical to the reference; real kombu items, no rewritten names; `test_kombu_identity.py`, `celery/`, `celery/t/` untouchables.

## 1. Memory stable over time (soak)

New `bench/soak.py`: workers `-P prefork -c 10` (Redis and AMQP) and `celery beat` (1000 entries, of which many are due every second) for a configurable duration (`--minutes`, default 60), with continuous and varied load: task `pure`, tasks failing, retry, `countdown`/ETA, revokes, chord/group, large results (1 MB), periodic `inspect`/`control`, restart lane broker once halfway through. Sample each 30 s the PSS of each process (master, zygote, children, beat) and save a series in `bench/results/soak-*.json`. Criterion: After warm-up (first 5 minutes) the PSS slope of each process is **< 1 MB/hour** and no child exceeds its PSS at 5 minutes by more than 3 MB; open fd and constant threads. Run ref and rs for comparison (60 minutes each, in parallel on different lanes). If you find growths, find the cause and correct it (with testing).

## 2. Message parser fuzz

The Rust core decodes messages from the broker (Celery v1/v2 protocol, header, JSON/pickle-free body, compression, Kombu envelope for Redis, AMQP properties). Add fuzz targets (`cargo fuzz` with libFuzzer, or `proptest`/`arbitrary` if `cargo fuzz` is not available: justify the dependency in the final message) for the parsing entry points in `crates/celery-proto` and `crates/celery-transport`. Run each target ≥ 10 minutes; no panic, no proportional allocation to unbounded message declared lengths (defenses against hostile messages: explicit bounds), no infinite loop. Each crash found becomes a regression test. A malformed message must produce the same outcome as the reference (reject/log), never the crash of the worker.

## 3. Segfault billiard in the full suite

`python/tests_rs/test_worker_pool.py::test_worker_process_signals[prefork]` (see "Known Failures"): In the full suite the children of the billiard pool (reference path) die with SIGSEGV during GC. Find the cause (probable: fork of a pytest process that has live Rust threads/state left from previous tests; `celery._rs` object finalizers inherited) and fix it in celery-rs code (fork safety: `register_at_fork`, orphaned Rust objects after fork should not be touched by finalizers). Then remove the entry from "Known Failures" in `AGENTS.md`.

## Gate

`tools/dev/gates` (all green except for the remaining "Known Failures"), plus the soaks and fuzzes above.

## Work Rules (model quota limited)

- Long commands (soak, fuzz, gate) **in blocking mode**, only once. **Do not write waiting messages or check the progress**: each message is a call to the model.
- If a command lasts longer than the limit of your tool, launch it in the background with output to file and **end the turn** by writing in the report which files to read: the orchestrator will resume you when it is finished.
- Do not investigate "Known Failures" other than point 3. Final report short: PSS plots/table over time (slope per process, ref vs rs), fuzz target with duration and outcomes, segfault cause and fix, gate with real outcome, what remains open. Commit to the branch (`09b: …`).
