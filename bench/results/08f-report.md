# 08f — measured ratio

The spec thresholds **are not all reached**. Work reduces schedule growth and improves tick; the base ≤24 MB, the base limit+3 MB to 1000 entries on AMQP and a demonstrable reduction in the cost of `-B` remain.

PSS in MiB from `/proc/*/smaps`; all broker benchmarks were run under `tools/dev/with-brokers`. Shared libraries and external concurrency introduce noise: the negative deltas of `-B` in the baseline are not savings attributable to the code.

| Brokers | Voices | Ref | rs first | rs after |
|---|---:|---:|---:|---:|
| redis | 3 | 66.06 | 32.60 | 27.23 |
| redis | 1000 | 54.97 | 39.12 | 29.14 |
| redis | 10000 | 117.82 | 118.26 | 53.87 |
| amqp | 3 | 39.61 | 28.51 | 28.46 |
| amqp | 1000 | 46.42 | 39.68 | 33.79 |
| amqp | 10000 | 109.58 | 118.31 | 56.29 |

Between 1000 and 10000 entries the rs growth is of redis: 2.81 KiB/entry, amqp: 2.56 KiB/entry. The direct comparison 3→1000 also includes the fixed cost of the first publication (runtime and client paths), absent in the three items at an hourly interval.

| Brokers | Tick ref / before / after (µs) | Send ref / before / after (/s) |
|---|---:|---:|
| redis | 1239 / 1307 / 1049 | 388 / 435 / 674 |
| amqp | 1250 / 1328 / 1070 | 442 / 459 / 740 |

Worker: total PSS of the master and **all** descendants; each cell is without `-B` → with `-B` (MiB). The rs beat remains in thread. A new zygote process per beat has not been introduced: the data does not demonstrate that it would reduce the total PSS.

| Brokers / pools | Ref | rs first | rs after |
|---|---:|---:|---:|
| redis /only | 51.77 → 64.16 | 36.30 → 34.21 | 30.95 → 31.92 |
| redis / prefork c10 | 317.47 → 336.56 | 93.08 → 87.28 | 79.08 → 85.69 |
| amqp / only | 40.13 → 52.89 | 32.02 → 33.99 | 32.04 → 33.93 |
| amqp / prefork c10 | 254.85 → 266.01 | 83.64 → 83.36 | 84.46 → 87.80 |

Breakdown to 3 items. The Python heap is measured in a separate process with tracemalloc, so as not to distort the PSS. Anonymous pages include Python heaps, Rust heaps, stacks, and fragmentation: **they are not an isolated measure of the Rust heap**. The native column measures the extension's mappings, not its allocations.

| Broker / version | Anonymous MiB | MiB Extension | Other MiB mappings | Python heap plotted MiB | Forms |
|---|---:|---:|---:|---:|---:|
| redis / ref | 57.46 | 0.00 | 8.60 | 35.05 | 755 |
| redis /before | 23.63 | 2.28 | 6.70 | 16.79 | 366 |
| redis /final | 19.48 | 2.28 | 5.47 | 16.73 | 364 |
| amqp / ref | 31.25 | 0.00 | 8.36 | 27.55 | 656 |
| amqp /before | 19.55 | 2.28 | 6.68 | 16.79 | 366 |
| amqp / final | 19.49 | 2.28 | 6.69 | 16.73 | 364 |

Changes:

- `PersistentScheduler.sync` keeps only the entry map in the writeback cache after flush: heap and scheduler do not reread copies of the same pickle. Shelve format unchanged.
- `crontab` keeps expanded fields in Rust core until observed. Attributes and `vars()` materialize true, mutable sets; user-supplied sets retain identities. In-place changes are honored.
- Compact crontab comparison uses Rust fields. The common path avoids re-expanding the fields during the schedule comparison.
- The pidbox node is only imported when required: 366→364 modules in the rs beat, about 65 KiB of Python heap plotted less. The kombu objects remain the real ones.
- The schedule DEBUG log text is built only when the level is enabled.
Rejected GC + `malloc_trim`: 30499→30439 KiB in the initial test, only 60 KiB; no changes to the allocator adopted. No new Rust dependencies.

Targeted checks before the final gate: crontab differential (240000 cases) 4 passed; new 5 memory/API/persistence/pidbox tests passed; one hour accelerated, 10800 submissions, PSS growth among samples after warm-up <2 MiB. `pyflakes` clean on new files and beats; schedules maintains four pre-existing flags for internally unused public compatibility imports. `git diff --check` clean.

Benchmark commands:
```sh
tools/dev/with-brokers .venv-rs/bin/python bench/bench_beat_memory.py --venv .venv-rs --label before --source "$PWD/target/08f-baseline/python" --workers --trace
tools/dev/with-brokers .venv-ref/bin/python bench/bench_beat_memory.py --venv .venv-ref --label ref --workers --trace
tools/dev/with-brokers .venv-rs/bin/python bench/bench_beat_memory.py --venv .venv-rs --label final --workers --trace
```

The Python baseline is extracted from HEAD before modifications (no changes to the reference source). Complete JSON, module lists and tracemalloc statistics are in the `08f-{before,ref,final}.json` files; the import profiles are in `08f-importtime-{before,after}.txt`.

Final gate: `tools/dev/gates`, one full run after changes, 714,5 s; log in `target/gates/20261026-173646-2559402`.

| Check | Real outcome |
|---|---|
| cargo build --release | OK |
| cargo clippy --all-targets -- -D warnings | OK |
| build-package / build-native | OK / OK |
| cargo test under lock | 234 passed |
| crates/celery-py/tests | 81 passed |
| python/tests_rs (four groups) | 209 passed |
| compat api | 0 differences, 137 modules |
| compact wire | 0 differences, 40 scenarios |
| compat wire-worker solo / prefork | 0 / 0 differences, 22 scenarios each |
| compact wire-beat | 0 differences, 2 brokers |
| compat cli | 0 differences, 190 commands |
| compat upstream | exit 1: only known test_pytest_celery_marker_registration failure, ignored at user instruction; 3 unstable |

The gate as a whole therefore returns exit 1, and is not declared entirely green. No changes to `celery/`, `celery/t/`, `tools/compat/` or kombu identity tests. The spec remains partially open for the RAM targets indicated at the beginning. The Rust heap has not been isolated from other anonymous allocations: this limitation of the decomposition remains explicit.

A first importtime probe terminated with SIGBUS while the gate reinstalled the native extension; re-executed after the builds are concluded, it passes in both variants. It was not used for PSS measurements or gate tests.
