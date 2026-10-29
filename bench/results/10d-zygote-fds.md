# 10d — zygote descriptors

The master exposes the registry of descriptors owned by Rust. Waker and IPC sockets use RAII recordings removed upon destruction; entries do not grow beyond living resources. A new register for PID avoids inherited mutexes in the child. For Tokyo, a private probe exactly identifies the epolls that contain it, including copies of the driver: no attribution based only on the moment of opening. Socket broker/backend and eventfd are derived from their `fdinfo`. The Tokyo feature `net` is now explicit; no new addictions.

The photo before the fork and the driver reading in the child cover the changes in connections during the fork. Device/inode identities protect recycled numbers. Zygote closes local copies before imports, without modifying the epoll records shared with the master. Inherited Rust objects remain protected by `ForkSafe` destructors. Python files, sockets, epolls, and logs remain open. No additional scanning in children.

Checks performed:

- `cargo build --release`: passed.
- `cargo clippy --all-targets -- -D warnings`: passed.
- `tools/dev/with-brokers cargo test`: 244 passed, 4 ignored, 0 failed.
- `tools/dev/build-package` and `tools/dev/build-native`: passed.
- `tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs/test_zygote_user_fds.py python/tests_rs/test_zygote_logging_fds.py python/tests_rs/test_ram_budget.py -q`: 11 passed.
- `tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs/test_worker_pool.py -q`: 27 passed; 52 Python warnings about multithreaded fork, even in reference billiard pool.

Differential starts worker ref and rs with `-P prefork -c 2` and `max_tasks_per_child=1`: 12 Implementationally distinct PIDs use the same file and TCP loopback socket open on import, with identical results. In the zygote the set of fds remains identical before and after the replacements. The other tests verify the closure of only Rust fds with Redis and AMQP, subsequent operation of the master, file/queue loggers, and a Python fd that the parent recycles into a socket registered in the shared epoll. The 10c test retains single cleanup verification in the zygote and now requires the Python fd to survive.

RAM, PSS total master + zygote + 10 children (MiB):

| Measurement | Before | After |
|---|---:|---:|
| Rest | 87,41 | 83,23 |
| After load / peak | 108,28 | 102,07 |

No increase observed. Command executed for both measurements:

```
tools/dev/with-brokers .venv-rs/bin/python bench/bench.py --venv .venv-rs --pool prefork --concurrency 10 --task pure --prefill --n 10000 --latency-n 20 --label 10d-before
tools/dev/with-brokers .venv-rs/bin/python bench/bench.py --venv .venv-rs --pool prefork --concurrency 10 --task pure --prefill --n 10000 --latency-n 20 --label 10d-after
```

Aggregate and per-process data in `10d-zygote-fds.json` (full mappings are excluded). Gates complete, soak and `compat integration` not performed, as required by spec; they remain with the orchestrator.

Files modified:

- `crates/celery-py/Cargo.toml`, `src/lib.rs`, `src/transport.rs`.
- `crates/celery-worker/src/fds.rs`, `lib.rs`, `wake.rs`, `pool/spawn.rs`, `pool/supervisor.rs`.
- `python/celery/concurrency/_rs_zygote.py`.
- `python/tests_rs/test_zygote_user_fds.py`, `test_zygote_logging_fds.py`, `test_ram_budget.py`.
- `bench/results/10d-zygote-fds.json` and this report.
