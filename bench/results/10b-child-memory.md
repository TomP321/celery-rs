# 10b — Prefork children RAM

Units: MiB (PSS/1024), as in the benchmarks. Redis; 10000 task, prefill, 20 latency samples; concurrency 10/50. Each measurement is finished within 300 s; batches after the first c10 also have explicit timeouts. The thresholds are also verified by the four new tests, on new worker instances.

## Before/after without profiler

| c/tasks | middle child before → after | maximum before → after | total before → after | master before → after | zygote before → after |
|---|---:|---:|---:|---:|---:|
| 10 / pure | 9.96 → 8.07 | 9.97 → 8.09 | 129.7 → 105.9 | 20.12 → 17.18 | 9.98 → 8.07 |
| 10 / bench-toplevel | 12.41 → 11.37 | 12.42 → 11.38 | 153.0 → 141.0 | 18.09 → 17.43 | 10.83 → 9.83 |
| 50 / pure | 6.47 → 5.39 | 6.96 → 5.88 | 349.7 → 291.6 | 19.56 → 16.93 | 6.50 → 4.97 |
| 50 / bench-toplevel | 10.14 → 9.00 | 10.56 → 9.39 | 536.3 → 473.5 | 20.43 → 15.87 | 8.75 → 7.62 |

| c/tasks | task/s before → after | p50 ms before → after | p99 ms before → after |
|---|---:|---:|---:|
| 10 / pure | 2749 → 3108 | 2.41 → 2.21 | 4.01 → 4.61 |
| 10 / bench-toplevel | 2751 → 2779 | 1.99 → 2.03 | 4.47 → 4.57 |
| 50 / pure | 2892 → 2977 | 3.70 → 4.32 | 4.25 → 4.86 |
| 50 / bench-toplevel | 2834 → 2847 | 4.24 → 3.69 | 4.90 → 4.35 |

Short samples on shared host: latency has variance, without a statistical conclusion on small differences.

## Causes and fixes

| cause | Private MiB per child c10 pure | intervention / evidence |
|---|---:|---|
| free glibc pages after initializer and warm-up | about 0,59 recovered | trim-only variant: Private_Dirty 6,89 → 6,31; PSS 9,96 → 9,17 |
| CFFI/prctl parser and inherited pages written during initializer | additional 1,08 recovered | native syscall: Private_Dirty 6,31 → 5,22; PSS 9,17 → 8,07 |
| CoW erosion and trace initialization after idle | about 1,68 residues | Private_Dirty final 5,22 vs. idle 3,54; no new modules in the path as well |
| new live Python objects, with active probe | ≤ 0,20 before; ≤ 0,16 after | tracemalloc to 2000 task, includes probe objects |
| Rust heap in child as well | 0,0043, constant | build memory-probe, 4424 live bytes and peak 4608 from fork to 2000 task |

The native route sets the same PR_SET_PDEATHSIG/SIGKILL as billiard, preserving its return even for an invalid signal. No kombu/billiard classes replaced. The CFFI parser is no longer preloaded into the zygote nor built by the initializer. libc was already in the workspace: direct dependency avoids parsers and temporary objects for just one syscall.

Free pages are returned after gc.freeze in the zygote, after initializer and after tasks 1/10/100/1000. GC, logging, task signals and user code remain unchanged. Existing large payload recovery remains active. No need for IPC buffer reductions: the Rust counter shows no increasing retention.

Discarded variants: trim only (9,17 MiB, still above threshold); Explicit GC (no recovery in the first experiment, with increase in private pages). No GC added to normal route.

## Diagnosis with profiler

30000 task pure, c10, feature memory-probe; average of ten children completing at least 2000 tasks. Full statistics for heap, anonymous mappings (including pymalloc arenas), libraries, and stacks are in 10b-diagnosis.json. The probe also saves per-process tracemalloc and malloc_info snapshots in /tmp/10b-diagnosis-v2-{before,after}.

| tasks per child | PSS before → after | Private_Dirty before → after | Python alive KiB before → after |
|---|---:|---:|---:|
| 0 | 7.99 → 8.17 | 5.21 → 5.17 | 16.6 → 16.6 |
| 1 | 12.67 → 10.89 | 10.17 → 8.38 | 137.5 → 109.4 |
| 10 | 13.65 → 11.28 | 11.15 → 8.77 | 181.7 → 138.1 |
| 100 | 13.93 → 11.52 | 11.43 → 9.02 | 190.9 → 153.6 |
| 1000 | 14.07 → 11.64 | 11.58 → 9.14 | 194.6 → 160.3 |
| 2000 | 14.16 → 12.74 | 11.67 → 10.23 | 199.7 → 163.4 |

The profiler alters the resident: snapshots, smaps reading and serialization leave temporary memory in the allocator; do not use this PSS for acceptance thresholds. In particular, sample 2000 follows 1000 tasks without forced trim, while samples 1/10/100/1000 follow a trim. No additional lazy import observed in children as well.

## Fleet and processes outside the pool

Fleet: budget 2048 MiB, c10, 10000 pure tasks completed in both cases. The numbers of workers under load are estimates from the average resident of the sample, as implemented by fleet.py.

| | before | after |
|---|---:|---:|
| worker idle within 2 GiB | 22 | 26 |
| workers under load estimated by 2 GiB | 20 | 23 |
| MiB per worker under load | 97.9 | 85.8 |
| workers under load estimated in 8 GiB | 83 | 95 |
| beat redis, 3 entries, MiB | 25.39 | 24.73 |
| beat redis, 1000 entries, MiB | 27.32 | 27.25 |
| beat amqp, 3 vocals, MiB | 24.55 | 24.54 |
| beat amqp, 1000 vocals, MiB | 28.55 | 27.77 |
| redis client after get, MiB | 22.01 | 22.24 |
| redis client, Pss_Anon MiB | 19.438 | 19.453 |
| amqp client after get, MiB | 22.55 | 22.45 |
| amqp client, Pss_Anon MiB | 19.523 | 19.523 |

Client: 333 modules in both cases; Redis anonymous difference of 16 KiB, total PSS +0,23 MiB versus +0,12 MiB of the concurrently measured reference; AMQP slightly down. No material increase observed outside the pool, with shared page noise.

## Checks

- cargo build --release: PASS.
- cargo clippy --all-targets -- -D warnings: PASS.
- tools/dev/with-brokers cargo tests: 243 PASS, 4 ignored by the suite.
- tools/dev/build-package and tools/dev/build-native: PASS, repeated on the final version.
- ./compat api: 0 differences on 137 modules and settings/signals/CLI.
- tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q: 81 PASS.
- tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs/{test_child_memory,test_memory_growth,test_worker_c10,test_zygote_logging_fds,test_master_beat_restart,test_kombu_identity}.py -q: 35 PASS.
- test_worker_pool.py -k "process_signals or zygote_preserves_user_gc_configuration": 4 PASS; test_worker_prefork.py::test_zygote_lost_shuts_down_with_failure: 1 PASS (always under with-brokers).
- Benchmark worker before/after: bench/bench.py --venv .venv-rs --pool prefork --concurrency {10,50} --task {pure,bench-toplevel} --prefill --n 10000 --latency-n 20; fleet: bench/fleet.py --venv .venv-rs --budget-mb 2048 --tasks 10000. All completed under lock; batches following the first with timeout 300 s; baseline fleet with --source /tmp/10b-baseline-source, rebuilt by HEAD.
- Final redo on installed build: test_child_memory.py, test_memory_growth.py and test_worker_c10.py, 15 PASS in 71,81 s. Verified that the installed build does not expose memory-probe counters.
- Additional check cargo fmt --check: report differences even in numerous unmodified files; no global formatting applied. git diff --check clean.

Tools/dev/gates, soak and compat integration not executed, as required by the spec: they remain with the orchestrator. No open memory policies.
Files changed: PyO3 bridge worker/lib/Cargo and Cargo.lock, celery-backend/redis.rs, platforms.py, concurrency/_rs_child.py/_rs_zygote.py/_rs_memory_probe.py, bench/fleet.py, test_child_memory.py and test_memory_growth.py, data, and this report. Reference source, tools/compat and test_kombu_identity.py unchanged.
