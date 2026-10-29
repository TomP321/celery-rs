# 09c — Memory growth over time: no worse than baseline

Project priority: **Worker and beat RAM**. After 09b the celery-rs c10 prefork worker uses much less memory than the reference, but **grows faster** under prolonged load (`bench/soak.py`, 60 minutes, varied load, broker restart at minute 30):

| Total PSS worker c10 (without beat) | 5 min | 31 min | 59 min | growth |
|---|---|---|---|---|
| ref Redis | 352 | 366 | 378 | +27 MB |
| rs Redis | 230 | 259 | 286 | +57 MB |
| ref AMQP | 381 | 408 | 422 | +41 MB |
| rs AMQP | 225 | 260 | 284 | +60 MB |

By child rs: +4–8 MB/h, all children (in the ref the growth is concentrated in a few children, others remain stationary). When restarting the broker rs jumps by ~+10 MB, the ref by ~+2 MB. Data: `bench/results/soak-20261027-051343-rs-*.json`, `soak-20261027-010658-ref-*.json`, `09b-soak-report-final.md`, `09b-redis-short-report.md`.

Read `AGENTS.md`, `PLAN.md`, `specs/09b-soak-fuzz.md`. Constraints unchanged (API, traffic, real kombu objects, no rewritten names, tests and `celery/` untouchable).

## Objective

In a soak of 60 minutes: growth of the total rs **≤ that of the reference** on the same broker (better if it tends to a plateau) and jump when the broker is restarted ≤ +3 MB; no increase in initial PSS or degradation of throughput/latency above noise.

## Diagnosis (before correcting)

For each child distinguish, over time:
- **copy-on-write erosion** (pages shared with the zygote becoming private: `Shared_*` → `Private_*`, constant RSS) — caused by refcount/GC touching inherited objects, or modified inherited structures;
- **new allocations** (RSS growing): Python heap (`tracemalloc` in child, triggerable with a diagnosis environment variable, interval snapshot and diff for traceback), Rust heap (feature `memory-probe`), glibc / fragmentation arenas (`malloc_info`), growing caches (e.g. import cache, `linecache`, registers, internal queues, `revoked`, `active_requests`, IPC buffers, logs). For the jump to broker restart: what is loaded or allocated in children and master on reconnect (compare to reference). Use short soaks (`--minutes 15`) and targeted tests; reports a "cause → MB/h" table.

## Corrections

Only with evidence from the numbers and without changing visible behavior: e.g. load into the zygote what the children load next, `gc.freeze()` stable objects after warm-up if proven useful, limit caches and internal structures with limits such as reference, releasing buffers/arenas. Each fix has a test in `python/tests_rs`.

## Check

Short soaks your before/after (15 min, Redis and AMQP). **Do not** execute soak from 60 minutes nor `tools/dev/gates`: the orchestrator does them.

## Work Rules (model quota limited)

Commands in a blocking manner, no waiting messages or progress checks. If a command exceeds the limit of your tool, launch it in the background with output to file and end the shift by indicating the files to read. Short final report: causes table → MB/h, corrections, short soaks before/after, what remains. Commit to the branch (`09c: …`).
