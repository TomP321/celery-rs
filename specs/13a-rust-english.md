# 13a — Rust crates in English, minimal style

Read `specs/13-english-style.md` (mandatory rules) and `AGENTS.md`.

Scope: every file in `crates/` (all six crates, including tests, benches and `build.rs`).
Translate and rewrite comments, rustdoc, error/log messages and test names; apply the Rust rules
of the style guide. `celery-transport` has the most text (~520 comment lines): do it first.

Constraints: no public API or PyO3 name changes (Python imports `_celery_rs.*`); Python tests may
match on some error strings — grep `python/` and `tools/` for any message you change and keep
them consistent (only within `python/tests_rs` and tools; do not edit `python/celery` here, that is
13b — if a Python file asserts a Rust message, list it in the report instead).
