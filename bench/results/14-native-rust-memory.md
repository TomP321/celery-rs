# Native Rust memory management

Python 3.13.9, Rust 1.99.1, Redis prefork concurrency 10. Measurements use
`tools/dev/with-brokers` and the same worktree virtualenv before and after.

## Changes

- Compression borrows immutable Python byte slices across `Python::detach`.
- Wake descriptors use `File` ownership and safe I/O. Other Unix platforms use
  `std::io::pipe`, with one outstanding byte under the existing state lock;
  this avoids needing a nonblocking setter that std pipes do not expose.
- Zygote spawning uses `UnixStream::pair`, borrowed descriptors and owned clones.
  The Python integer descriptor is first duplicated by Python's `os.dup`, then
  transferred to `OwnedFd`; invalid integers never become borrowed Rust descriptors.
  SCM_RIGHTS stays in one helper, with an aligned ancillary buffer.
- `PerProcess<T>` replaces both pointer-based once cells. It publishes without
  inherited locks and intentionally leaks published entries across fork/reset.
  Unpublished contenders are dropped normally.
- Process syscalls live behind safe `io::Result` functions in `process.rs`.
- JSON type checks use safe PyO3 APIs. Integer FFI stays because safe conversions
  measured slower; dictionary iteration stays because PyO3 panics on callback
  mutation. Type flags stay native to bypass metaclass overrides.
- Redis single-queue publishing moves the envelope rather than copying it.
  Consumer BRPOP borrows its connection; control collection borrows reply/ticket.
  Other Redis clones own configuration, map entries, snapshots, or shared handles.
  Supervisor clones retain shared `Bytes`, `Arc` or sender ownership; tests account
  for the remaining occurrences. These do not copy message bodies. Delivery-tag
  `Display` formatting stays on the shared numeric/UUID path rather than adding
  variant-specific `Cow` branching; Python API string returns stay owned.
  Connection setup and declaration strings are cold paths.
- Unsafe-free crates forbid unsafe code; the other crates deny implicit unsafe
  operations in unsafe functions.

## Unsafe counts

Counts are whole-word `unsafe` occurrences in `crates/*/src/**/*.rs`, including
unit-test code and unsafe function declarations, across all platform cfgs.

| Crate | Before | After |
|---|---:|---:|
| celery-proto | 0 | 0 |
| celery-transport | 0 | 0 |
| celery-beat | 0 | 0 |
| celery-backend | 1 | 1 |
| celery-worker | 27 | 16 |
| celery-py | 39 | 29 |

PEP 683 immortalization, glibc heap trimming, memory diagnostics, and inherited
connection leaks remain intentional. Rust allocator APIs cannot trim the heap
shared with CPython. Tokio's descriptor registration requires its unsafe API;
the owned socket guarantees descriptor validity. The Python descriptor ownership
transfer also requires one unsafe block. Each retained block states its invariant.

## JSON benchmark

`bench/memprobe/json_path.py` measures seven repetitions of 20,000 calls for
normal tasks, dictionaries, small integers, large integers, and JSON decoding.
Saved extension builds can be compared with `--extension-dir`.

A paired comparison of original JSON code against the fully safe candidate gave:

| Path | Original median ns | Safe candidate median ns |
|---|---:|---:|
| Task | 1,597 | 1,586 |
| Dictionary | 15,734 | 15,807 |
| Small integers | 3,930 | 3,930 |
| Large integers | 1,089 | 1,360 |
| Decode dictionary | 14,764 | 13,922 |
| Decode large integers | 1,035 | 1,135 |

Normal-path differences overlap run-to-run noise. Large integer encoding costs
25% more because safe extraction constructs an overflow exception; calling
Python `int(text, 10)` costs 10% more on decoding. Both integer FFI calls remain.

## Memory and throughput

Command (run before and after):

```sh
tools/dev/with-brokers .venv-rs/bin/python bench/memprobe/loadpeak.py "$PWD/.venv-rs" 10 "$PWD/bench/memprobe" 30000
```

| Metric | Before | After |
|---|---:|---:|
| Idle total PSS, MiB | 52.1 | 46.2 |
| Peak total PSS, MiB | 59.2 | 53.2 |
| Peak master PSS, MiB | 13.2 | 11.3 |
| Peak zygote PSS, MiB | 4.3 | 3.8 |
| Peak children PSS, MiB | 41.7 | 38.1 |
| After-load total PSS, MiB | 59.2 | 53.1 |
| Tasks/s | 2,932 | 2,893 |

Peak PSS decreased 10%; throughput changed -1.3% and remains above the required
reference floor of 834 task/s. These are individual runs, not confidence intervals.

**Correction by the orchestrator.** The single runs above overstate the change: the "before" run was
inflated. Same worktree, swapping only the native module, four alternating runs (`loadpeak.py`, 30,000 tasks):
before idle 46.6 / peak 52.6 MiB, after idle 46.7 / peak 52.9 MiB. The difference is within noise (±1 MiB).
This spec does not change RAM: its value is safer code, not memory.

The final JSON medians were task 1,588 ns, dictionary 14,822 ns, small integers
4,469 ns, large integers 1,050 ns, dictionary decoding 14,990 ns, and large integer
decoding 1,027 ns.

## Verification

| Command | Result |
|---|---|
| `cargo build --release` | Passed |
| `cargo clippy --all-targets -- -D warnings` | Passed |
| `tools/dev/with-brokers cargo test` | 252 passed, 4 ignored, 0 failed |
| `tools/dev/build-package` | Installed full package in `.venv-rs` |
| `tools/dev/build-native` | Installed extension beside reference in `.venv-ref` |
| `.venv-ref/bin/python -m pytest crates/celery-py/tests/test_differential.py -q` | 24 passed; includes JSON mutation, integer, compression and task-message comparisons |
| `./compat api` | 0 differences in 137 modules plus settings/signals/CLI |
| Broker-locked `loadpeak.py` command above | Passed before and after |
| `tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q` | 283 passed, 57 warnings in 964.64 s; full suite run once |
| `git diff --check` | Passed |

The initial benchmark attempt found stopped broker lanes and no installed package;
`tools/dev/lanes up` and the unchanged `tools/dev/build-package` established the
baseline. A new Python-binding unit test initially lacked interpreter initialization;
that test setup was corrected before the successful final Rust test run.
No gates, upstream/integration command, soak, or push was run. No dependencies added.
No Italian remains in changed code/comments. Runtime verification was on Linux;
the non-Linux pipe fallback was not executed on another platform. Its borrowed
std pipe I/O was compiled and exercised in a standalone Linux probe.

All acceptance checks passed. The Python warnings concern deprecated Redis `setex` and fork calls in
multithreaded test processes. No open implementation issues remain from this spec.

## Files touched

- `crates/{celery-proto,celery-transport,celery-backend,celery-worker,celery-beat,celery-py}/src/lib.rs`
- `crates/celery-worker/src/{per_process.rs,process.rs,fds.rs,wake.rs,pool/spawn.rs}`
- `crates/celery-py/src/{compress.rs,json.rs,task.rs,transport.rs,worker.rs}`
- `crates/celery-py/tests/test_differential.py`
- `crates/celery-transport/src/redis/mod.rs`
- `crates/celery-backend/src/redis.rs`
- `bench/memprobe/json_path.py`
- `bench/results/14-native-rust-memory.md`

Detailed command logs and saved benchmark builds are in `target/spec14/`.
