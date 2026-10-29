09b — Redis diagnosis and brief comparison (27 October 2026)

The jump after the reboot arises mainly from late imports of the error classes: `RedisBackend.connection_errors` loads redis-py, asyncio and kombu in the individual children. The zygote does not allocate: its RSS remains constant, but PSS increases when children edit shared pages. The second contribution is the free pages of the glibc arenas after the body copies from 1 MiB into the RESP buffers. In the 500 probe results, alive Python memory after GC grows by only 16 KiB, while `malloc_trim` recovers around 10 MiB of PSS.

Fix: Real exception graph is loaded into zygote before `gc.freeze()` and forks; after large SET/SETEX/PUBLISH the free pages are returned to glibc, even at the next command and at the end of the task, when the Python copies are no longer alive. The child also frees result, arguments, and IPC frames before waiting for the next job. Claiming is limited to Linux/glibc and payloads >=1 MiB; `libc` was already in the workspace dependencies.

Four rs/Redis soak runs for 12 minutes, blocking, with a restart at minute 6. Here is the comparison between the initial code and the final code, both with 220 cycles completed and load resumption verified. PSS in MiB; growth from sample to 5 minutes; last sample at 11,5 minutes.

| Measurement | Before | After |
|---|---:|---:|
| PSS final worker + zygote + 10 children, excluding beat | 414,61 | 253,67 |
| Median maximum growth of children | 19,51 | 1,43 |
| Worst maximum growth of a child | 20,24 | 3,81 |
| Zygote PSS maximum growth | 2,69 | 0,37 |
| RSS zygote | 32,1 constants | 42,6 constants |

Original series: [before](soak-20261027-040541-rs-redis-390ea448d0.json), [after](soak-20261027-045508-rs-redis-90758db30b.json); [table for PID, slopes, fd and threads](09b-redis-short-pss.md). The two intermediate tests and all logs of this diagnosis remain in `target/09b/`.
Isolated probe, 100 results from 1 MiB: same Python rs shell, previous binding of `.venv-ref` vs new binding; PSS increase **12,29 -> 2,30 MiB**. The pipeline test fails with the previous binding (+4,32 MiB) and passes with the new one (limit +3 MiB). Other regressions check Python encoding pages, the absence of new error imports into children, and the destruction of the last result while the child is inactive.

Final checks: `cargo build --release` and `cargo clippy --all-targets -- -D warnings` OK; `tools/dev/with-brokers cargo test`: **240 passed, 0 failed, 4 fuzzes ignored**; pytest locked on `test_backend_redis.py`, `test_worker_pool.py`, `test_redis_memory.py` and `bench/test_soak.py`: **49 passed**. No gate, long fuzz or soak for 60 minutes rerun.

**The spec threshold remains open:** even the short final soak comes out 1. Two children still exceed +3 MiB (+3,81 and +3,06); the OLS slopes of the children are 5,01–48,10 MiB/h, that of the zygote 3,19 MiB/h; some fds change upon reconnection. The main jump is correct, but this comparison does not certify stability <1 MiB/h. Imports/preloads and reclamation reduce memory; warm-ups, analyzes or thresholds for declaring green soak are not changed. The long verification remains with the user.
