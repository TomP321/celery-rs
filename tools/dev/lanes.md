# Broker lanes and gates

Start `tools/dev/lanes up`, then run `tools/dev/gates`. Gates wait for completion,
print outcomes and durations, and return nonzero on failure. `tools/dev/lanes status`
lists containers; `tools/dev/lanes down` stops and removes them.

`CELERY_RS_LANES` sets the lane count (default 4). Use the same value for startup,
execution and shutdown. Lane N binds Redis `6390+N`, AMQP `5673+N` and management
`15673+N` to loopback. Lane 0 uses Compose project `celery-rs`; others use `celery-rs-lane-N`.

`tools/dev/with-brokers command ...` reserves a free lane with `flock`, waiting if
all are busy. Worktrees share `/tmp/celery-rs-broker-lane-N.lock`. Nested commands
reuse the parent's lock.

| Variable | Value |
|---|---|
| `CELERY_RS_LANE` | Lane index |
| `CELERY_RS_REDIS_URL` | Redis URL without database |
| `CELERY_RS_AMQP_URL` | Full AMQP URL |
| `CELERY_RS_RABBIT_API` | Management URL ending in `/api` |

Existing aliases `CELERY_RS_TEST_REDIS`, `CELERY_RS_TEST_AMQP` and
`CELERY_RS_TEST_RABBIT_API` remain available. New variables take precedence in tests;
the wrapper assigns both sets to the same lane.

The wrapper clears leftover AMQP connections and exclusive queues before and after
each command. A dedicated process group and Linux subreaper collect exited daemons
and terminate all descendants on exit, including SIGINT/SIGTERM and descendants
that created new sessions.

`gates` builds, runs clippy and installs packages sequentially, then distributes
other gates across lanes. `tests_rs` balances files using recorded durations and
runs each file in a separate interpreter. Longer gates start first. This does not
change conformity exclusions or thresholds.

Select subsets with `tools/dev/gates --only tests_rs`, or comma-separated names:
`build,clippy,build-package,build-native,cargo-test,native-tests,api,wire,upstream,wire-worker-solo,wire-worker-prefork,wire-beat,cli`.
Individual `tests_rs-N` groups are also selectable. Subsets do not rebuild automatically.

`target/gates/<name>.log` points to the latest invocation. Permanent logs, XML and
`results.json` live in `target/gates/<timestamp>-<pid>/`. Concurrent invocations use
separate directories. Recorded durations live in `target/gates/durations.json`.

Infrastructure tests: `python3 tools/dev/tests/test_lanes.py -v`. Start at least two
lanes and run outside `with-brokers`.

## Recorded verification: 26 October 2026

The full run after fixes returned 0 in **726.8 s (12m 6.8s)**.
Logs: `target/gates/20261026-163735-2144576/`.

| Gate | Result | Duration | Outcome |
|---|---|---:|---|
| build release | OK | 65.6 s | built |
| clippy | OK | 1.6 s | no warnings |
| build-package | OK | 53.8 s | installed |
| build-native | OK | 1.3 s | installed |
| cargo-test | OK | 14.4 s | 234 passed |
| native-tests | OK | 60.4 s | 81 passed |
| tests_rs-1 | OK | 86.9 s | 12 passed |
| tests_rs-2 | OK | 221.0 s | 89 passed |
| tests_rs-3 | OK | 219.8 s | 52 passed |
| tests_rs-4 | OK | 223.5 s | 51 passed |
| api | OK | 1.9 s | 0 differences, 137 modules |
| wire | OK | 58.9 s | 0 differences, 40 scenarios |
| upstream | OK | 65.5 s | 31,691 tests, 0 regressions |
| wire-worker solo | OK | 224.1 s | 0 differences, 22 scenarios |
| wire-worker prefork | OK | 260.3 s | 0 differences, 22 scenarios |
| wire-beat | OK | 18.8 s | 0 differences, 2 brokers |
| cli | OK | 275.3 s | 0 differences, 190 commands |

The upstream marker-registration failure was reproduced with reference Celery and
recognized through its report, without new exclusions. The known billiard child
segfault appeared in the first `test_worker_pool` logs; that file still passed.

The first run (846.1 s) exposed Docker address, daemon collection and shared Python
state issues, subsequently fixed. Three transient wire differences passed targeted
comparisons with harness `51f5b226223ce03738ad980d5ee17ceb0dfc2687` and the second full run.
Comparisons, normalization and thresholds were unchanged. `cargo fmt --check` found
existing workspace formatting; no general reformat was applied.

An initial Ctrl-C check after 20 s returned nonzero: 42 observed processes,
no remaining PIDs, and zero AMQP connections/exclusive queues on all four lanes.
Artifacts: `target/gates/acceptance/interrupt.log` and `ps-after-interrupt.txt`.
The five infrastructure tests passed in 6.775 s.

Two concurrent subset runs passed (204 tests each, exits 0 / 0) in 377.6 and 370.3 s (377.7 s combined wall time).
Logs: `target/gates/acceptance/parallel-{1,2}.log`; runner directories:
`20261026-165027-2238493` and `20261026-165027-2238494`. Final checks found zero
AMQP connections and exclusive queues on all lanes. A real AMQP connection also
verified management cleanup and its log line under lock.

Changed areas were infrastructure, agent instructions, lane/gate scripts and tests,
and broker addresses in Python/Rust tests, benchmarks and compatibility tools.
No dependencies, core behaviour or upstream tests changed.

RabbitMQ can expose an exclusive queue before its owner connection. DELETE then
returns HTTP 400. Cleanup retries reads and closes for up to 15 s, then reports
remaining resources. This final fix was covered by the real management-owner test,
five infrastructure tests and repeated Ctrl-C verification: exit 1, **43 observed
processes, zero leftovers**, zero AMQP connections and exclusive queues on all four lanes.
