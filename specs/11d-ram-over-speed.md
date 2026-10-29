# 11d — RAM before speed (worker `-c 10`, also Python 3.11)

Read `AGENTS.md`, `docs/DIFFERENCES.md`, `bench/results/11c-immortal-load.md`. Base: 11c (immortal objects). User rule: **all about saving memory; the basis is the original Celery.** Throughput can drop to that of the original Celery measured on this machine with the same measurement (`loadpeak.py` with `.venv-ref`: Redis `-c 10` ~820 task/s, RabbitMQ ~3350; remeasure it yourself at the beginning and use those numbers as the floor, not below). Every MB counts more than any task/s above the floor: always prefer the variant that uses less RAM. Counts the total PSS of the worker **idle and at peak load** (`bench/memprobe/loadpeak.py`, 30.000 tasks, integers and `--payload-bytes 16384`, Redis and RabbitMQ).

Starting point (11c, Python 3.13): rest ~49,5 MB, peak 53,7 (Redis integers) / 58,1 (RabbitMQ 16 KiB). User production runs on **Python 3.11**, where immortality is a no-op: also measure with 3.11 (`uv python install 3.11`, create `.venv-rs311` with adapted `tools/dev/build-package` or `uv pip install` of the wheel; do not touch `.venv-ref`) and reports before/after for 3.11 and 3.13.

## Tracks (each: measure before/after, only keep what reduces the PSS)

1. **Master**: less threaded tokio runtime (also `current_thread`) for consumer/pool/producer; `mallopt(M_ARENA_MAX, …)`/arena glibc for threads; smaller Rust thread stacks; initial and maximum capacities of buffers (socket read, prefetch, IPC frames towards children) reduced and returned when the load drops.
2. **Children under load**: return memory after each task or when switching to rest (`malloc_trim`/release empty pymalloc arenas) instead of just 1/10/100/1000 tasks; socketpair read/write buffers reused or released; fewer objects per task in the path `_rs_child` → trace → READY.
3. **Python 3.11** (without immortality): children's private pages after fork and `process_initializer` (see diagnosis in `bench/results/11c-immortal-load.md` and `bench/memprobe/sd.py`): reduce what the child touches, e.g. identical deterministic work in each child that can be moved in the zygote **only if the observable behavior remains that of the reference** (signals `worker_process_init` etc. always in the child); compaction of partially free pymalloc pools in the zygote before freezing if the measures justify it.
4. **Shared image inventory** (~28 MB of shared RSS that each worker pays once, plus private pages that arise by touching it): lists the modules in `sys.modules` of master, zygote and child with the estimated cost (anon + `.so` mapped, e.g. `_zstd`, `_brotli`, `_yaml`, `_cmsgpack`, `_decimal`, `libcrypto`/`ssl`, `click`, unused kombu transports, `redis-py` if the backend is Rust) and who imports them. Avoid imports that the worker does not use **only where the API remains identical** (lazy imports in the `python/celery` shell; kombu remains the real kombu, see `python/tests_rs/test_kombu_identity.py`). It also measures free heap inherited from the zygote (pymalloc/glibc) and fragmentation.
5. Any other ideas measured. Don't change observable semantics (prefetch, ack, number of processes, signals, CLI options) from the reference.

## Objectives

Python 3.13: rest ≤ 40 MB, peak ≤ 44 MB in all four scenarios. Python 3.11: ≥ 25% reduction in rest and peak compared to 11c over 3.11. These are minimums, not milestones - keep going until you find measurable reductions. Throughput ≥ floor.

## Check

`cargo build --release`, `cargo clippy --all-targets -- -D warnings`, `tools/dev/with-brokers cargo test`, `tools/dev/with-brokers .venv-rs/bin/python -m pytest
python/tests_rs -q`, `./compat api`. **Do not launch `tools/dev/gates`, upstream, integration or soak**: the orchestrator runs them. Blocking commands, no waiting messages, no pushes. Commit to branch (`11d: …`); report in `bench/results/11d-ram-over-speed.md` with before/after table for each scenario and version, and each track tested (even the discarded ones, with the number).
Note: the orchestrator is executing gates in parallel on the broker lanes: RAM measurements remain valid, throughput may be lower than actual (repeat the final task/s measurement twice and report the best one). Files in `bench/results/11d-before.log` from a previous aborted boot can be reused or overwritten.
