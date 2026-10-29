# 10a — closing owner follow-up

Decision made following `ccd5bc3`: minimize RAM while maintaining performance above the reference. Follow-up requests are completed; I do not declare that the historical targets of 13 MiB for the master and 22 MiB for the beat have been reached.

`TaskPool.Pool` and `TaskPool.BlockingPool` resolve and preserve the true classes on first access. Two tests in clean interpreters exercise the class and the instance as first access respectively, verifying the identity with `AsynPool` and `billiard.pool.Pool`. The c10 worker probe verifies that `asynpool`, `billiard.pool`, and `billiard.queues` are absent from the master, even after reconnection. That leaves the billiard utilities required by real kombu items. The traditional path continues to use the original implementations. The bootstep Hub also avoids importing the billiard pool only to replace the ApplyResult lock when the Rust pool is selected. The clock lock and traditional path patch remain active.

Modified code: `Cargo.toml`, `python/celery/concurrency/prefork.py`, `python/celery/worker/components.py` and `python/tests_rs/test_master_beat_restart.py`; added reports and measurements. The list of modules observed in the masters is stored in `results/10a-owner-master-imports.json`.

## Final measurements

PSS idle in MiB; child = average of ten children. Before: Python and native module of `ccd5bc3`, copied to `/tmp/10a-owner-before/python`. After: Lazy pools and build releases at `opt-level="s"`, with **only `celery-beat` at `opt-level=3`**. No new dependencies or changes to `tools/compat`.

| Process | Before | After |
|---|---:|---:|
| master prefork c10 | 22,04 | 18,27 |
| zygote | 8,07 | 7,99 |
| middle son | 7,61 | 7,49 |
| total worker c10 | 106,17 | 101,18 |
| beat 3 Redis entries | 29,07 | 28,55 |
| beat 3 AMQP entries | 28,37 | 28,57 |
| client after get Redis | 26,23 | 26,00 |
| client after get AMQP | 26,57 | 26,28 |

The worker's savings is 4,99 MiB, approximately 4,7%. The AMQP beat instead measures **+0,20 MiB**: I don't advocate an improvement in every single line. The choice favors the overall reduction and that of masters, children, clients and Redis beats.

| Redis worker performance, pure tasks | Before | After | Measured reference |
|---|---:|---:|---:|
| p50 delay/get, ms | 2,120 | **2,012** | 2,483 |
| task/s end-to-end | 2419 | **2591** | 823 |

The p50 is under 3 ms and under that of the reference; the throughput is approximately three times that of the reference. Short samples: 3000 task and 200 latency measures.

Three compact builds were compared, all with lazy pools, before the Hub bootstep fix. The initial table also includes this correction.

| Variant | Masters | Zygote | Son | Total c10 | p50 ms |
|---|---:|---:|---:|---:|---:|
| only dependencies and bindings to s | 18,54 | 7,82 | 7,34 | 99,74 | 2,097 |
| also worker/transport/backend to s | 18,35 | 7,88 | 7,41 | 100,33 | 2,065 |
| all to s, crontab to 3 — adopted | 18,37 | 7,84 | 7,36 | 99,87 | 1,962 |

The difference of 0,13 MiB on the total c10 between the first and last variant is small; the latter further reduces the master, Redis client and the two beat measures compared to the former. The raw data are `results/*10a-owner-after.json`, `results/*10a-owner-io-s.json` and `results/*10a-owner-cron3.json` respectively; the worker after the Hub fix is in `results/10a-owner-selected.json`. Baseline and reference have suffixes `10a-owner-before` and `10a-owner-ref`.

## Commands and verification

Sequential measurements, without compiles or concurrent tests, all under `tools/dev/with-brokers`; each individual benchmark lasts less than five minutes:

```text
.venv-rs/bin/python bench/bench.py --venv .venv-rs --pool prefork --concurrency 10 --task pure --n 3000 --latency-n 200 --label <variante>
.venv-rs/bin/python bench/bench_beat.py --venv .venv-rs --label <variante>
.venv-rs/bin/python bench/bench_client.py --label <variante>
```

`--source /tmp/10a-owner-before/python` was added for the baseline; for the worker reference `--venv .venv-ref`. No gate or soak performed.

Checks completed in the adopted configuration:

- `cargo build --release` and `cargo clippy --all-targets -- -D warnings`: green.
- `tools/dev/with-brokers cargo test`: 242 passed, 4 ignored; the debug profile does not change with release settings.
- `tools/dev/build-package` and `tools/dev/build-native`: Completed.
- Test rs `test_master_beat_restart.py`, `test_kombu_identity.py`, `test_beat_memory.py`: **21 passed**, including the two brokers, class/instance accesses and the absence of pool modules in the real master.
- Upstream units `concurrency/test_prefork.py`, `apps/test_worker.py` and `worker/test_components.py`, with `-o pythonpath=python` and `CELERY_RS_WORKER=0 CELERY_RS_PRODUCER=0 CELERY_RS_BACKEND=0`: **97 passed**.
- `.venv-ref/bin/python -m pytest crates/celery-py/tests -q`, under lock: **81 passed**; JSON benchmark 625k/s against 247k/s of the reference, encode task v2 88k/s against 16k/s.
- `./compat api`: **0 counted differences** on 137 modules and settings/signals/CLI, using owner-added acceptances in `ccd5bc3`.
- `./compat wire-beat`: **0 differences** on Redis and AMQP.
- Pyflakes on modified Python files and `git diff --check`: green.

The first probe execution had identified two failures for the residual import of billiard.pool from the bootstep Hub; after the fix the entire group of 21 tests was rerun successfully.
