# Implementation and interpretation

- `--prefill` separates worker capacity from publish: worker ready, group suspended, N publishes, check zero completions, resume and drain. Removed warm-up tasks from the count; 66 trials from 10.000 task verified exactly N completions and zero completions before resuming.
- `bench-toplevel` import redis-py into the tasks module before fork and use the same INCR as the variant with import into the child.
- AMQP reuses the declaration channel only for the pidbox temporary consumer. Ordinary consumers maintain independent channels, QoS and tags. The control reuses the existing pool descriptor, respecting fallback, overriding, and disabling.
- Removed explicit `queue.delete` after AMQP ping: in reference `kombu.transport.base.StdChannel.after_reply_message_received` is empty. The broker performs auto-delete; a durable reply queue must survive. The latter behavior has a differential test with the reference client.
- Waiting for results continues to waste CPU when the result is pending; avoids the `sleep(0)` following the final answer.

## Profiling and variants

Profiling is separate from latencies. In the rs Redis c10 master, on 10.101 tasks, synchronous acks: 2,355 s (233 µs/task); pickle IPC: 0,180 s (18 µs/task); submit to pool: 0,064 s (6 µs/task). The epoll wait includes startup, idle and prefill pause, so it cannot be interpreted as the CPU cost of the task. Textual profiles are stored together with JSON.
Just reusing the channel (`after-latency-*.json`, intermediate variant) was not enough: ping p50 rs 9,68/9,26 ms against 8,79/8,82 of the ref, on worker ref/rs. Adding pool and removing final yield, but still keeping delete (`candidate-latency-*.json`), ping 8,89/9,07 ms; there remained a p99 of 12,48 ms against 11,23 of the reference on the worker rs. These partial solutions were not selected. The final version is documented by files `isolated-*`. TCP_NODELAY was already active in tcp-stream: no Nagle changes to adopt. No prefetches have been changed or acks grouped: the profile identifies the cost, but does not demonstrate a compatible and faster variant to adopt.

## RAM and compromise

Isolated probes show a maximum increase of 0,14 MiB for client/worker rs and no beat growth beyond 0,03 MiB. In the c10 benchmarks with 10.000 task the median peaks grow in some cases beyond 0,5 MiB: up to approximately 1,6 MiB (AMQP bench), however less than the 0,8% of the PSS. Redis bench-toplevel grows approximately 1,1 MiB; pure Redis about 0,5 MiB. They are values explicitly stored in the table, not considered zero. The same version shows a variation between repetitions of 2,4 MiB (AMQP bench) and 0,8 MiB (Redis bench-toplevel).

The final choice conservatively accepts even these small increments for the measured improvement compared to the initial version: RPC p50 on worker ref 3,326 → 2,803 ms (−15,7%); ping on worker rs 9,916 → 8,487 ms (−14,4%). RPC is also 20,4% faster than client ref on worker ref. The isolated comparison does not indicate structural growth above the 0,5 MB threshold; task spikes with import/Redis connections remain sensitive to copy-on-write and GC.

## Limits remaining

- Probes distinguish publish client, go to child, task and response; broker, master receive and IPC are not timed separately.
- `bench-toplevel` saves about 76–80 MiB of peak compared to importing into the child, but does not reach `pure`: Redis clients, connections and COW remain.
- Redis c10 capacity remains limited mainly by synchronous acks; no protocol or broker traffic changes have been introduced to bypass them.
- The publisher prefill must finish within the broker's heartbeat (see README).

No added dependencies; reference sources, upstream tests and unmodified compliance tools.
