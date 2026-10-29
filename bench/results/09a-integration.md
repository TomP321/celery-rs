# 09a — integration and memory

Core and shell fixes, with no changes to `celery/`, original tests, or dependencies:

- Unanswered revocations accept `timeout=None`: collection timeout is not used without tickets.
- `RedisClient.ttl` and the shim return Redis TTLs, including -1/-2; checked result_failure_expires and result_chord_expires already implemented.
- UNSUBSCRIBE waits for runtime confirmation before close, avoiding residual subscriptions during reentrant deletion. No additional commands.
- AMQP binding ignores unknown properties and None values before conversion, like py-amqp. Republishing from before_task_publish takes the Signal parameter.

Tests added: `python/tests_rs/test_integration_fixes.py` (5 differential cases ref/rs); Rust test for TTL and unsubscribe confirmed.

## Integration

| Brokers | Ref P/F/S/E | rs P/F/S/E to gate | Initial/Gate Regressions |
|---|---|---|---|
| Redis | 356/8/13/1 | 356/8/13/1 | 7/0 |
| AMQP | 355/8/14/1 | 353/10/14/1 | 8/2 |

The initial 15 regressions pass into targeted testing. The full spin reported two additional AMQP regressions (`test_mutable_errback_called_by_chord_from_chain[errback_old_style]` and variant `_tail`). They pass in isolation, but reappear in the complete file: the previous classification as instability and the manual update of the report with isolated repetitions were incorrect. The raw result of the gate is the one shown in the table.

The subsequent diagnosis identified the closure of the logging fds in the zygote: reused by Redis sockets, they received log text and caused protocol errors. The fix preserves handler queue streams and connections. The `python/tests_rs/test_zygote_logging_fds.py` test reproduces the reuse of the fd after fork: 4 failures first, 4 passed after. The final check on the complete canvas file is reported at the bottom.

Common collection error: SQLAlchemy missing. Other missing services and common failures are listed in the integration report, including Django and Azure. No exclusions added.

## Commands and outcomes

- `tools/dev/build-package`: OK before targeted checks.
- `tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs/test_integration_fixes.py -q`: 5 passed. First attempt with AMQP setup of the new test incorrect, corrected before the green result.
- `./compat integration -k 'test_revoked or test_chord_expires or test_failure_expires or test_pubsub_reentrant_cancel_from_finalizer or test_retry_with_unpickleable_exception or test_chord_header_id_duplicated_on_rabbitmq_msg_duplication'`: 0 regressions / 0 execution errors on both brokers.
- `tools/dev/gates`: run once, 3683,1 s, exit 1. Build release, clippy, 235 Rust tests, 81 native tests, 229 tests_rs, API, wire, wire-worker solo/prefork, wire-beat and green CLI. Upstream: only known failure `test_pytest_celery_marker_registration`. Integration: Redis 0 regressions; AMQP the 2 errback regressions described above.
- `./compat integration -k test_mutable_errback_called_by_chord_from_chain`: 4 passed for venv/broker, 0 regressions / 0 execution errors. This isolated repetition did not demonstrate resolution of the full file's regressions.
- `git diff --check`: OK.

Logs: `target/09a-final-gates.log`, `target/gates/20261026-212135-4026502/`, `target/09a-targeted-integration.log`, `target/09a-errback-targeted.log`. Original full report: `target/09a-full-gate-integration/`; isolated repeat: `target/09a-isolated-recheck/`; final report: `tools/compat/out/integration-report.md` and JSON.

## PSS before/after

Sequential comparison under a single lane lock, with completed suites. Baseline: Sources of the `2875522` commit and previous native module, preserved in `target/09a-memory-baseline/`, with the same dependencies as `.venv-rs` and precompiled bytecode. Previous concurrent measures are stored in `target/09a-concurrent-samples/`.

Driver `.venv-rs/bin/python`; `bench/bench.py --pool prefork --concurrency 10 --task pure` worker with 20.000 task and 500 latency measurements; beat `bench/bench_beat.py`, both brokers. `--venv target/09a-memory-baseline --label 09a-control-before` and then `--venv .venv-rs --label 09a-control-after`. All commands under `tools/dev/with-brokers`, exit 0.

| PSS (MiB) | Before | After | Delta |
|---|---:|---:|---:|
| Worker idle c10 | 84.142 | 83.014 | -1.128 |
| Worker peak c10 | 102.035 | 101.480 | -0.555 |
| Beat redis, 3 entries | 28.266 | 28.459 | +0.193 |
| Beat redis, 1000 entries | 32.920 | 33.256 | +0.336 |
| Beat amqp, 3 entries | 28.236 | 28.334 | +0.098 |
| Beat amqp, 1000 entries | 33.264 | 33.168 | -0.096 |

No increment above 0,5 MB. p50 serial latency: 1,915 → 2,083 ms (+0,169 ms), compatible with the new wait for UNSUBSCRIBE confirmation; JSONs also preserve throughput, p99, and memory per process.

The raw gate remains FAIL and has not been relaunched. The subsequent correction of the logging fd is verified only on the complete canvas file; Global gates and full suite are left up to the user, as required. Known pytest failure not investigated. The PSS measurements above precede this further correction.


## Fix AMQP residual state: fd of handlers

Zygote also closed descriptors held by inherited logging handlers. Pytest maintains a FileHandler towards `/dev/null`; its closed fd was reused by Redis. `strace` showed a `ERROR celery.backends.redis...` write to the Redis socket, followed by `ERR Protocol error: unbalanced ...` and closing. Subsequent backend errors stopped propagation to errbacks. The behavior depended on the sequence of tasks and logs, it was not resolved by isolated repetitions.

`_close_inherited_fds` now preserves root and named logger streams, and both ends of handler queues, in both zygote/child cleanup passes. No new persistent structures, dependencies or changes to the original tests.

Checks for this fix:

- `.venv-rs/bin/python -m pytest python/tests_rs/test_zygote_logging_fds.py -q`: 4 failed before; 4 passed after. The subprocess creates a handler, performs forks and cleanup, opens new sockets, and checks that logs are not written to recycled descriptors. Covers file/queue and root/named logger.
- `./compat integration --broker amqp -- t/integration/test_canvas.py`: exit 0; FULL file, ref and rs both **216 passed, 2 common failures, 4 skipped, 0 errors**; **0 regressions and 0 execution errors**. The two reported `errback_old_style` cases already pass the first pass, together with the respective new-style variants. No manual editing or merging of results.
- `git diff --check`: OK.
Log: `target/09a-canvas-repro.log`, `tools/compat/out/integration-amqp-{ref,rs}.log`, report `tools/compat/out/integration-report.{md,json}`. Corruption Test: `target/09a-logging-fds-{before,after}.log`; diagnosis: `target/09a-probe/all-syscalls.log`. Duplicate verification started on separate output was stopped after the green result of the canonical command, and is not counted as verification.

`tools/dev/gates`, the complete suite or new benchmarks for this additional fix were not run at the user's request: those checks remain the responsibility of the user.
