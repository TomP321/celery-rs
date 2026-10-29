# Capacities and latencies (08c)

Always run under `tools/dev/with-brokers`, which assigns an exclusive lane.

```sh
tools/dev/with-brokers .venv-ref/bin/python bench/run_08c.py --phase before --n 10000 --repeats 1
tools/dev/with-brokers .venv-ref/bin/python bench/probe_08c.py --phase isolated-before
tools/dev/with-brokers .venv-ref/bin/python bench/memory_08c.py --phase isolated-before
# After the change and tools/dev/build-package:

tools/dev/with-brokers .venv-ref/bin/python bench/run_08c.py --phase after --n 10000 --repeats 1
tools/dev/with-brokers .venv-ref/bin/python bench/probe_08c.py --phase isolated-after
tools/dev/with-brokers .venv-ref/bin/python bench/memory_08c.py --phase isolated-after
.venv-ref/bin/python bench/report_08c.py
```

`bench.py --prefill` waits for the worker to be ready and suspends the entire group (master, zygote and children) with `SIGSTOP`. Post exactly N tasks, verify that none have been completed, and resume the group with `SIGCONT`. The clock starts immediately before the restart. This way startup, connections and publishing do not enter the worker's capacity. The counting resolution is 20 ms: use thousands of tasks, not the small N of functional tests. The publisher must terminate within the broker's heartbeat timeout, because worker heartbeats are also suspended during the pause.

Memory is sampled every 50 ms over the entire tree, including zygotes and children; the peak also includes the subsequent probe `delay().get()`. PSS and Private_Dirty of each process are preserved. `pure` counts completions with append to file; `bench` uses INCR Redis and imports redis-py into the child; `bench-toplevel` runs the same INCR, with redis-py imported into the tasks module before the fork. The capacity comparison between `pure` and the other tasks therefore also includes the cost of the counter. No warm-up tasks are added to the throughput count.

`probe_08c.py` crosses ref/rs clients with AMQP ref/rs workers (c10 prefork, or `--pool solo`). The latencies of 1.000 calls are collected without a profiler; a second round of 100 calls produces the cProfile profiles. The `timed` task uses the monotonic clock common to processes of the same machine: it distinguishes going to the entry into the child, execution and return of the response. The first leg aggregates publication, broker, master and IPC; it does not allow the time spent to be attributed separately to each individual. `publish_ms` measures the duration of only the `delay()` call on the client.

The JSONs are in `bench/results/08c/`; logs and binary profiles remain local. The report preserves the real outcomes of the gates and any limitations of the measurement. PSSs must be compared like other active probes: additional Python processes change the distribution of shared pages.
