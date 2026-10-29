# 10a — result and limitations

`a9b80be` commit history report. The owner's subsequent decision and configuration is in [10a-owner-memory.md](10a-owner-memory.md).

At the end of that commit the spec **remained open**: the Redis jump is correct and verified; the absolute goals of master ≤13 MiB and beat ≤22 MiB are not achieved. No new addictions. `celery/`, upstream tests and code of `tools/compat/` are unchanged.

Measurements on Linux/Python 3.13, PSS in MiB (KiB/1024). Baseline: `e6ae45c`, sources extracted in `/tmp/10a-baseline/python` and native module of the initial build, without modifying the reference. Final measurements without competing compilations; each individual test lasts less than five minutes. For worker, baseline median of three trials and final median of two; the detail is in `results/10a-repeats.json`.

| PSS idle/skip | ref measured | rs first | final rs |
|---|---:|---:|---:|
| master prefork c10 | 26,75 | 19,09 | 18,78 |
| zygote | — | 7,93 | 7,86 |
| child c10, average | 28,13 | 7,48 | 7,39 |
| beat 3 Redis entries | 47,52 | 29,15 | 28,35 |
| beat 3 AMQP entries | 39,62 | 28,47 | 28,39 |
| client after get Redis | 33,77 | 26,30 | 26,27 |
| client after get AMQP | 33,68 | 26,46 | 26,48 |
| worst Redis restart, total c10 | +2,30 | +43,74 | +2,15 |
| worst AMQP restart, total c10 | +2,06 | not measured | +3,73 |

The reboot probe uses **all ten** children before each sampling, so the first use of a backend is not counted as a reconnection. The Redis baseline generates 30 task errors in the two reboots: tries the first GET on closed backend sockets, imports redis-py into the master when it receives the exception, and preserves sources/tracebacks in the children. This targeted probe amplifies the problem compared to the +14 MiB of the spec's soak; It's not a new soak.
In the final code no task errors, no new modules in the master and same PIDs before/after both reboots. Subsequent increments: Redis +2,15/+0,20 MiB, AMQP +2,04/+3,73 MiB. I'm under 5 MiB **for reboot**; the cumulative AMQP after two restarts is +5,76 MiB. Final data stored from tests in `results/10a-restart-final-{redis,amqp}.json`.

## Changes

- `apps/worker.py`: `cry` is the original object resolved with PEP 562; normal master doesn't import `celery.utils.debug` and psutil. `dir`, identity and pickle are checked against the reference.
- `concurrency/_rs_zygote.py`: Redis and asynchronous mixin load via the actual backend, avoiding holding them for memory/disabled backend.
- `concurrency/rs_prefork.py`: `TERM_SIGNAL` comes directly from billiard.common.
- `worker/_rs_consumer.py` and `backends/_rs_redis.py`: upon Redis reconnection, only inactive sockets of the native backend on the same host/port and with the same TLS type are discarded, even if the database is different. Independent backends, redis-py and custom clients remain on their path.
- Rust pool, PyO3 and `_rs_child.py` bindings: A dedicated IPC frame reaches all children before new tasks. It does not count as a job and does not change pool statistics. Busy children run it after the current task.
- `bench.py`, `bench_beat.py`, `bench_client.py`: `--source` allows isolated comparisons; the client also accepts `--label`. New `bench_restart.py` and eleven tests in `python/tests_rs/test_master_beat_restart.py`.

## Profiles, performance and discarded variants

Beat still loads 364 modules: Python heap plotted about 16,73 MiB, distinct from PSS because tracemalloc takes up its own memory. The Redis physical sample is divided into anonymous 19,49, libraries 5,29, Python 1,41 and Rust module 2,19 MiB. Complete profiles: `results/08f-10a-profile.json`, `results/10a-master-{before,final}.json`; `*-importtime.txt` describes bootstrap module imports, not an entire CLI session.
Throughput worker rs first 2439–2680 task/s, final 2452–2567; ref 842. p50 latency first 1,73–2,10 ms, final 1,96–2,04; ref 2,35. Tick beat Redis 1012→1025 µs, AMQP 1058→1044 µs; send(s) 650→689 and 777→738. Short samples show variability and overlapping ranges; they do not demonstrate an improvement in tail latencies. No measurable client growth.

Variants not adopted, exploratory measures in `results/10a-discarded.json`:

- traditional pool classes on lazy descriptors: six static API differences; master 18,81 MiB, substantially the same as the final. Restored the original classes.
- `opt-level=s` on everything: native module about 11→7,6 MiB, but simple crontab 3,66→4,04 µs in the first round; not adopted.
- binding to `s` only: modulus approximately 12 MiB, greater than baseline; discarded.
- bindings and dependencies to `s`: master 18,52 MiB in the first isolated test, p50 2,14 ms against 1,73 of the baseline of the same round. Insufficient benefit to change project profiles. All Cargo profiles are restored.

## Checks performed

- `cargo build --release`, `cargo clippy --all-targets -- -D warnings`: green.
- `tools/dev/with-brokers cargo test`: green, 242 tests passed, 4 ignored.
- `tools/dev/build-package`, `tools/dev/build-native`: completed, also in the final configuration at `opt-level=3`.
- `.venv-ref/bin/python -m pytest crates/celery-py/tests -q`, under lock: 81 passed.
- Targeted suite rs: worker_pool, worker_prefork, kombu_identity, beat, beat_memory, import_diet, master_beat_restart, backend_redis: 110 passed and a test collected before removing descriptors remained obsolete. After the update, reran the entire `test_master_beat_restart.py`: **11 passes**, including both brokers and no task errors.
- Upstream tests `t/unit/concurrency/test_prefork.py` and `t/unit/apps/test_worker.py`, `-o pythonpath=python`, with `CELERY_RS_WORKER=0 CELERY_RS_PRODUCER=0 CELERY_RS_BACKEND=0`: 83 passed.
- `./compat api`: **0 differences** on 137 modules and settings/signals/CLI.
- `./compat wire-beat`: **0 differences** on Redis and AMQP.
- Pyflakes on modified Python files and `git diff --check`: green.
- `cargo fmt --all -- --check`: not cleaned for pre-existing formatting in multiple crates; no global reformatting applied.

The reported benchmarks were run under `tools/dev/with-brokers`: `bench.py --pool prefork --concurrency 10 --task pure --n 3000 --latency-n 100|200`, `bench_beat.py`, `bench_client.py`, `bench_restart.py`, `bench_memory.py` and `bench_beat_memory.py --trace`. No global gate, soak or compact integration.

The absolute master and beat targets remain open: trace, canvas, result, backend and asynpool are still present in the master. The reduction in imports achieved is small; I do not consider all the criteria of the spec to be satisfied.
