09c — diagnose and fix traceback loops (27 October 2026)

Corrections: `bench/soak.py` always chooses `.venv-<impl>/bin/python`, even without `--compare` and with a lock already acquired. In the child, after serialization and sending READY, the traceback frames and their already terminated callers are freed. No changes to GC thresholds/enablements, no new preloads or dependencies. Optional diagnostics via `CELERY_RS_MEMORY_PROBE=<directory>`: RSS/PSS/USS/Shared/Private, tracemalloc snapshot updated every 30 seconds and diff for traceback, Rust counters when compiled with `memory-probe`, XML of `malloc_info`. The normal path does not matter the diagnostics.

Cause: `ExceptionInfo.exception → traceback → frame trace_task → f_back fast_trace_task → R → ExceptionInfo`. Celery attempts cleanup while tracer is still running; also `traceback.clear_frames()` runs through `tb_next`, not `f_back`. The code location lists constructed by `billiard.einfo._Code` then remain in cycles until the GC, with allocations and fragmentation. An initial fix limited to `clear_frames()` still left `ExceptionInfo` referenced by `fast_trace_task`: verified with `gc.get_referrers`. The final correction also runs through `f_back`, stopping at the first frame still active.

The test on the real `fast_trace_task`, with GC disabled, failed before this last change and passes after: the weakrefs to `ExceptionInfo` are immediately emptied due to failure/retry/reject/ignore. Another test checks pickle byte identity before/after cleaning. Existing tests also compare Rust pool to billiard.

Cause table → MiB/h: breakdown of c10 baselines provided by the orchestrator, samples at 300 and 870 seconds; averages per child. The components are measures of pages, not an independent estimate of the cost of individual allocation sites.

| Observed component | Redis rs | AMQP rs |
|---|---:|---:|
| New Anonymous Pages/RSS (includes traceback, caching and fragmentation loops) | about 10,0 | about 16,4 |
| Shared_Dirty lost, indicative limit of COW erosion | about 1,8 | about 0,8 |
| Child's overall PSS | about 11,4 | about 17,0 |

The dominant problem is therefore new memory, not COW erosion. In the 2 minute Redis diagnostic test, the live Rust heap fluctuates between approximately 47 KiB and 2,09 MiB after boot (I/O buffer), without progressive growth; peak approximately 8,1 MiB. The Python diff mostly indicates `billiard/einfo.py:28` (`list(code.co_positions())`). In the AMQP test with diagnostic interventions, the GC at the last sample recovers approximately 1,55 MiB Python and `malloc_trim` approximately 7,37 MiB RSS; `malloc_info` records approximately 13,6 MiB free in arenas. This data includes tracemalloc/snapshot overhead - it is NOT natural slopes nor does it motivate a new trim or periodic GC in production.

Final A/B tests: c1, same mixed load as `soak_app`/`soak.load`, no beat, no tracemalloc, 240 seconds, restart on second 120, broker-exclusive lock. Before = same build with cleanup only disabled in runner; after = complete correction. Native compiled with `memory-probe` in both groups. Numbers are MiB, samples approximately 30 → 240 seconds. The four tests end with exit 0 (the runner checks the survival of the processes, does not certify the soak thresholds).

| Measurement | Redis first | Redis after | AMQP first | AMQP after |
|---|---:|---:|---:|---:|
| PSS son | 27,05 → 30,24 | 23,79 → 24,95 | 27,59 → 30,43 | 24,16 → 24,39 |
| RSS final child | 53,16 | 48,47 | 53,72 | 48,18 |
| USS Final Son | 18,68 | 13,65 | 19,01 | 13,20 |
| Growth PSS worker + zygote + child | +5,45 | +1,13 | +5,87 | +0,97 |
| Cycles Completed | 66 | 66 | 56 | 56 |
| Delta PSS total around 110 → 131/132 s, around restart | −1,60 | −1,64 | +0,86 | +0,98 |

The child growth reduction on this window is equivalent to approximately 35 MiB/h Redis and 45 MiB/h AMQP for c1; It's a short comparison that includes warm-up and reconnection, NOT a one-hour forecast. After correction AMQP tends to plateau, Redis retains residual growth. Previous trials from 120 seconds also showed reduction in final PSS, but incomplete cleanup did not resolve the caller loop.

Data held in `bench/results/09c-memory-growth.json`: role/process sets of the four tests from 240 s, loops, Python/Rust traces, malloc statistics. Local logs and details in `target/09c/`. Playable runner:

```
tools/dev/with-brokers .venv-rs/bin/python bench/memory_growth.py --broker redis --seconds 240 --output target/09c/new-redis
# Add --without-cleanup for A/B testing, --trace for diagnostics.
# --reclaim with --trace measures GC and malloc_trim separately; diagnostics only.

```

Checks performed: `cargo build --release` OK; `cargo clippy --all-targets -- -D warnings` OK; `tools/dev/with-brokers cargo test` OK, 240 tests passed and 4 fuzz skipped. Final Python suite under lock (`test_worker_pool.py`, `test_redis_memory.py`, `test_memory_growth.py`, `bench/test_soak.py`): 39 passed, 52 fork deprecation warning from multithreaded process. The exploratory and A/B tests are all blocking, at most 4 minutes each. No gate or soak for 15/60 minutes performed.

Restored and verified normal build with `tools/dev/build-package` (OK, `_allocator_stats` absent); the 10 diagnostic/soak tests pass even without features.

To be measured by orchestrator: after `tools/dev/build-package`, soak `--compare --minutes 15` for total PSS c10 and per child at 5 minutes/end, slopes relative to reference, delta around restart and load resumption; then long duration if the short comparison is satisfactory. Also check representative latency and throughput: only cycle parity is checked here, in addition to pool tests. The spec threshold on 60 minutes and the total jump c10 ≤3 MiB remain UNcertified.
