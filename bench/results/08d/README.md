# 08d — worker prefork c10

Measurements on this worktree, Python 3.13.9, 30.000 tasks, 500 latency probes, Redis :6390 and RabbitMQ :5673. Memory in MiB (1024 KiB), PSS summed across the entire tree; `after` also includes the `add.delay().get()` probes. JSONs also contain `throughput_processes`, before probes, when available.

Each ref/rs × Redis/AMQP × bench/pure combination has two executions before and two after. The first Redis/bench pair of the base has significantly more memory than the second: the report preserves both and their interval, without attributing all the before/after difference to the modification. The first Rust builds took place during part of the initial matrix; the final matrix was executed after the end of the compilation, under a single broker lock.

Playback:

```sh
tools/dev/with-brokers .venv-ref/bin/python bench/run_c10.py --phase after
python3 bench/report_c10.py --phase after > bench/results/08d/after.md
```

`before.md` and `after.md` report averages/ranges, savings in MiB and %, PSS and Private_Dirty of master, zygote and each child for each run.

## Changes

- Zygote actually warms up the CFFI parser used by billiard for `prctl`: importing `cffi.cparser` still left the global parser cache empty. Warming occurs before GC freezing and child forking.
- The Rust IPC reader transfers the received buffer to `Bytes`, without copying its payload. The Python child passes the view of the buffer directly to pickle. They do not change frame format, broker traffic, callbacks or pool limits.
- No changes to thresholds or GC enablement. An isolated probe compares these values to the reference, with default and custom settings (GC disabled), on ten children; after 2.000 warm-up jobs measure other 30.000 jobs, with less than 2 MiB of PSS growth allowed per child.
- The benchmark records PID, role, PSS, Private_Dirty, broker and phase; uses unique queues and counters, without FLUSHALL. Runner and report generator included.

## Variants

Eight isolated tests (`variant-*.json`), two per variant/task, Redis c10:

| variant | tasks | idle MiB | peak/after MiB | task/s |
|---|---|---:|---:|---:|
| CFFI hot | bench | 84,0–84,3 | 231,8–231,9 | 2610–2612 |
| CFFI hot | as well | 84,0–84,1 | 102,3–103,1 | 2601–2608 |
| CFFI + redis-py preloaded | bench | 96,4–97,0 | 158,2–160,1 | 1541–2506 |
| CFFI + redis-py preloaded | as well | 96,2–96,7 | 114,4–114,6 | 2368–2592 |

Adopted CFFI heating: approximately 3 MiB idle less than base warm measurements, with no slowdown detected. Rediscarded automatic preloading of redis-py: saves about 73 MiB with the task that imports it, but costs about 12 MiB for the `pure` task, and introduces an import that the application may not use. The first preload test also has much lower throughput than the second; it is not presented as a performance improvement. The benchmark supports `--include redis` to also measure explicit preload via the already existing worker option, if the application uses redis-py.

`pickle-copy.json`: 100 decodes a job with argument from 8 MiB, with tracemalloc. Copy bytes: 16.002 Temporary MiB, 0,185 s; direct view: 8,003 MiB, 0,082 s. Proof isolates avoided copying; it is not a prediction of the broker's throughput. The Rust test adds a payload from 1 MiB followed by a second frame and verifies that the payloads remain valid after subsequent reads.

No new addictions. Mimalloc/arenas not retried: 08b-review2 requires keeping the already measured decision (mimalloc only 34→40 MiB, prefork 71→77 MiB; no gain from glibc arenas).

## Final results

Averages of two runs. PSS in MiB, reference → celery-rs:

| broker/task | idle | peak = after | idle saving | peak/after savings | task/s ref → rs | p50/p99 ms ref → rs |
|---|---:|---:|---:|---:|---:|---|
| Redis bench | 308,7 → 83,9 | 313,2 → 231,4 | 224,8 (72,8%) | 81,9 (26,1%) | 897 → 2674 | 3,11/4,08 → 1,91/4,28 |
| Redis as well | 308,6 → 84,2 | 310,6 → 101,4 | 224,4 (72,7%) | 209,2 (67,3%) | 916 → 2553 | 3,13/4,34 → 1,91/3,97 |
| AMQP bench | 307,0 → 83,9 | 319,6 → 233,1 | 223,2 (72,7%) | 86,5 (27,1%) | 2921 → 3642 | 3,26/3,99 → 2,63/4,35 |
| AMQP as well | 306,7 → 83,5 | 310,3 → 103,1 | 223,2 (72,8%) | 207,2 (66,8%) | 2970 → 3706 | 3,33/4,39 → 2,51/4,34 |

Compared to the hot base (second Redis/bench execution), the peak of the `bench` task remains essentially unchanged (231,7 → 231,4 MiB), while idle passes 87,4 → 83,9 MiB. `pure` Redis goes from average 104,2 MiB to 101,4 MiB after load. On AMQP `pure` the base has throughput 3979–4122 task/s, the final 3662–3750: the measurements show a variability/regression to be reported, without attributing it as fact to the change of the IPC copies; the isolated CFFI comparison on Redis does not highlight this slowdown. The AMQP final p99 latencies remain around 4,3 ms. There is no claim of linear scalability with ten children.

In the final JSON rs the drain after the last publish is 0–5 ms and the client rate is almost the same as end-to-end (Redis about 2.600/s, AMQP about 3.700/s). This load therefore primarily measures the serving limit: growing children beyond four cannot produce linear scaling while the client does not feed a queue any faster. This is a limitation of the benchmark and not a demonstration of saturation of the c10 pool.
`bytes-micro.txt` compares two million small buffers (or 1.000 from 1 MiB) in release: payload copy (`mode=0`), transfer with `slice` (`mode=1`), transfer with `advance` (`mode=2`). On buffers from 32 bytes, 47,7 / 44,0 / 28,2 ns; from 1 MiB, approximately 1163 / 20,4 / 19,9 µs. The implemented path is `slice`, which avoids the second body allocation/copy; this micro test does not demonstrate an end-to-end profit on the broker.
