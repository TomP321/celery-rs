# 14 — Native Rust memory management instead of C idioms

Read `AGENTS.md` and `specs/13-english-style.md`. Goal: resources and memory are managed through
ownership, borrowing, RAII and safe std APIs, not C-style code. No behaviour change: same public
API, wire format, CLI. RAM must not grow; task/s must stay above the reference (Redis `-c 10`: 834).

The orchestrator already audited the code. `celery-proto`, `celery-transport` and `celery-beat` have
no `unsafe`, and hot paths already use `Bytes`/`CompactString`: do not rewrite those. Fix these:

1. **Pointer laundering** — `crates/celery-py/src/compress.rs` (2x) and `task.rs` (1x) cast a slice to
   `usize` and rebuild it with `from_raw_parts` to cross `py.detach`. `&[u8]` is `Send`: borrow it in
   the closure and delete the `unsafe`.
2. **`wake.rs`** — raw fd fields, manual `libc::close` in `Drop`, `libc::read/write/pipe/fcntl`.
   Hold an `OwnedFd`/`File` so `Drop` is automatic; use `std::io::pipe` and `Read`/`Write`. Only
   `eventfd` creation may stay as one small `unsafe`.
3. **`pool/spawn.rs`** — replace `socketpair` + `from_raw_fd` with `UnixStream::pair()`, `dup` with
   `try_clone`, `RawFd` parameters with `BorrowedFd`/`OwnedFd`. `SCM_RIGHTS` has no stable safe std API:
   keep it in one small helper with a precise `// SAFETY:` comment.
4. **Hand-rolled once cells** — `fds.rs` (`FDS`) and `celery-py/src/transport.rs` (`RUNTIME`) duplicate
   `AtomicPtr` + `Box::into_raw`/`from_raw`. Build one fork-aware generic (`PerProcess<T>`), with the
   `unsafe` confined to it and tests, and use it in both. It must not lock an inherited mutex after
   fork. Keep the intentional per-fork leak, documented.
5. **`libc` in `supervisor.rs`, `celery-py/src/worker.rs`** — wrap what remains (`kill`, `waitpid`, `prctl`)
   in small safe functions returning `io::Result`, in one module.
6. **`celery-py/src/json.rs`** — raw `ffi` on Python objects: use the safe PyO3 API wherever a benchmark
   shows no measurable cost (compare before/after on the JSON path); keep justified `ffi` only with a
   comment saying why.
7. **Avoidable owned data** — review `.clone()` in `redis/mod.rs` (38) and `pool/supervisor.rs` (14) and
   `.to_string()` outside error paths and tests. Use borrows, `Cow`, `Arc<str>`, `Box<[T]>` where it
   removes copies in per-message paths. Skip cold paths and anything that hurts readability.

Leave as they are, with accurate comments: `immortal.rs` (inherent to PEP 683), `malloc_trim` (trims the
glibc heap shared with CPython: Rust's allocator API cannot do it), `memory_probe.rs` (diagnostics),
`ForkSafe`'s `ManuallyDrop` and `mem::forget` of inherited connections (dropping them after fork aborts).

Add `#![forbid(unsafe_code)]` to every crate left with none, and `#![deny(unsafe_op_in_unsafe_fn)]` to the
others.

## Verification
`cargo build --release`, `cargo clippy --all-targets -- -D warnings`, `tools/dev/with-brokers cargo test`,
`tools/dev/build-package`, `tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q`
(run the full suite **once, at the end**), `./compat api`, and
`tools/dev/with-brokers .venv-rs/bin/python bench/memprobe/loadpeak.py $PWD/.venv-rs 10 $PWD/bench/memprobe 30000`
before and after.

Do not run `tools/dev/gates`, `./compat upstream|integration` or soak. Codex quota is limited: stay on this
list, no exploration beyond it, about 45 minutes. Blocking commands, no push. Commit as `14: …`. Report:
`unsafe` count per crate before/after, RAM and task/s before/after, what you left and why.
