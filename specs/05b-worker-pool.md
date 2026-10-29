# 05b — Worker: Rust process pool (zygote + Python children)

Phase 5 of `PLAN.md` (hybrid architecture). Replaces billiard/`AsynPool` (prefork pool) with a Rust supervisor in the master and Python children running `trace_task_ret`/`fast_trace_task` as today. Children are born for `fork()` from a **zygote**: a single-threaded Python process, created before the master's Rust threads start, with apps and tasks already imported and `gc.freeze()`, so the pages remain shared (copy-on-write) among all children. Target: same semantics as `-P prefork`, much lower total worker PSS. In parallel another agent writes the Rust consumer and the main loop (spec 05a): **do not touch** `celery/worker/` nor `crates/celery-worker/src/consumer*`. The shared Waker already exists: `crates/celery-worker/src/wake.rs` and `_celery_rs.Waker` (do not change its interface): the supervisor calls `Waker::notify()` when it has results/events ready.

## References

- `python/celery/concurrency/base.py` (`BasePool`: attributes `signal_safe`, `is_green`, `uses_semaphore`, `task_join_will_block`, `body_can_be_buffer`, `apply_async`, `on_start/stop/
  terminate/close`, `terminate_job`, `restart`, `grow/shrink`, `num_processes`, `info`, `_get_info`, `did_start_ok`, `register_with_event_loop`, `flush`, `maintain_pool`), `concurrency/prefork.py` (`TaskPool`, `process_initializer`, `process_destructor`, signals to reset/ignore, `worker_process_init`/`worker_process_shutdown`), `concurrency/asynpool.py` (handover semantics, `on_job_ready`, `_on_soft_timeout`/`_on_hard_timeout`, flush on shutdown, `WorkerLostError`, children dying during a job, `did_start_ok`).
- billiard (`.venv-ref/lib/python3.13/site-packages/billiard/pool.py`): `Worker` (child loop, `soft_timeout_sighandler` on SIGUSR1, `maxtasksperchild`, `max_memory_per_child` in KB with `resource.getrusage`), `ApplyResult` (`_set`, `_ack`, `_set_terminated`, `_timeout_callback`, `_accept_callback(pid, time_accepted)`, `_callback`, `_error_callback`, `ExceptionInfo`), `Pool._on_hard_timeout`, `terminate_job`.
- `python/celery/worker/request.py::execute_using_pool` (as the pool is called) and `on_accepted`, `on_timeout`, `on_success`, `on_failure`.
- `python/celery/app/trace.py` (`trace_task_ret`, `fast_trace_task`, `setup_worker_optimizations`).

## Project

1. **Rust Supervisor** (`crates/celery-worker/src/pool/`): manages N children via `socketpair` (length binary framing, `Bytes`), limited job queue, assignment to first free child, events to master (accepted, ready with result payload, dead child with job, soft/hard timeout) in one queue + `Waker::notify()`; time limit with timer: soft → SIGUSR1 to child, hard → timeout callback then SIGKILL; child dies/is killed → job in error with `WorkerLostError` (same messages as billiard) and child replaced; `maxtasksperchild` and `max_memory_per_child` (child exits after job, supervisor replaces him); `terminate_job(pid, signal)`; `grow/shrink`; shutdown: orderly shutdown (wait for jobs, `process_destructor`), terminate (SIGTERM/SIGKILL). No unmotivated `unsafe`; `fork()` is done **only in the zygote**, never in the master with active threads.
2. **Zygote** (`python/celery/concurrency/_rs_zygote.py` + Rust part): process forked by the master at pool startup (before the pool's Rust threads; the master may already have the producer runtime: zygote calls the package's `after_fork` and never uses parent sockets), `gc.freeze()`, then waits for "create child" commands on a control socket; for each child it creates the `socketpair`, does `fork()`, passes the master end via `SCM_RIGHTS` and reports the pid. The zygote does not perform tasks. If it dies, the pool recreates it (or fails with `WorkerLostError` on in-flight jobs if it can't: document).
3. **Python child** (`python/celery/concurrency/_rs_child.py`): `process_initializer(app,
   hostname)` of the reference (reset/ignore signals, `worker_process_init`, `set_default_app`, logging, `_set_task_join_will_block(True)`), handler SIGUSR1 → `SoftTimeLimitExceeded` as billiard, loop: reads a job, reports "accepted" (pid, `time.monotonic()` as billiard), executes `target(*args, **kwargs)` (target and arguments serialized with `pickle` as billiard: the target is the function `trace_task_ret`/`fast_trace_task`), sends the result (`pickle`; exceptions as `ExceptionInfo` of billiard) and start again. In the end `process_destructor` + `worker_process_shutdown`.
4. **`TaskPool` Python** (`python/celery/concurrency/rs_prefork.py`): subclass of `BasePool` with the same interface and attributes as `concurrency.prefork.TaskPool` (`apply_async(target, args, kwargs, callback, error_callback, accept_callback,
   timeout_callback, soft_timeout, timeout, correlation_id, ...)` returns an object with the interface used by Celery of `ApplyResult`; `info`/`_get_info` with the same prefork keys — `max-concurrency`, `processes`, `max-tasks-per-child`, `put-guarded-by-semaphore`, `timeouts`, `writes`; `did_start_ok`). Callbacks are called in the master loop thread when the loop calls `pool.process_events()` (non-blocking) after a wake-up of the `Waker` passed to the constructor (`waker=`). For testing this spec use a small driver (`pool.wait(timeout)` = `waker.wait` + `process_events`). Integration with `celery worker -P prefork` and the Rust loop is of the 05c spec: here the pool **doesn't** replace `prefork` in the CLI yet.
5. Signals, fork safety, no threads in the process doing `fork()`; no changes to signatures, names or public defaults.

## Gate (all mandatory, real outcome in final message)

```
cargo build --release && cargo clippy --all-targets -- -D warnings
tools/dev/with-brokers cargo test
tools/dev/build-package && tools/dev/build-native
tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q
tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q
./compat api && ./compat wire && ./compat upstream --venv .venv-rs   # 0 / 0 / 0 regressions
```
Additionally `python/tests_rs/test_worker_pool.py` (`.venv-rs`), Rust pool driven directly with real `Request` of the reference (built from `as_task_v2` messages, as in `t/unit/worker/test_request.py`) and a real Redis backend:
- execution of N concurrent tasks on `-c 4`, results and callbacks identical to billiard (same test also performed with `concurrency.prefork.TaskPool` as reference);
- soft time limit → `SoftTimeLimitExceeded` in the task; hard time limit → `TimeLimitExceeded`, child replaced; child killed (`os.kill` SIGKILL) during a job → `WorkerLostError` with the same message as billiard; `reject_on_worker_lost`;
- `maxtasksperchild=2` and `max_memory_per_child` (task that allocates) → replacement of children;
- `terminate_job` (revoke with terminate=True) with SIGTERM/SIGKILL;
- `grow`/`shrink`; ordered shutdown with job in progress; `worker_process_init`/`shutdown` called in children; no orphaned processes after stopping (check with `/proc`).
- Memory: With an app that imports ~30 MB of modules, total PSS (master+zygote+4 children) of the Rust pool vs `concurrency.prefork.TaskPool` of the reference, idle and after 1000 task. Report the numbers.
- Benchmark: job/s with empty tasks (1000 tasks, `-c 4`) Rust pool vs reference prefork.

## Out of scope

Consumer/loop (05a), CLI integration `celery worker` (05c), autoscale, `-P threads`, eventlet/gevent, Windows.
