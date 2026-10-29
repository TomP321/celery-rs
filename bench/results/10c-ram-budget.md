# 10c — budget and RAM reduction

**Partial status: Acceptance criteria are NOT all met.** Fleet drops from 78,1/86,0 to 73,3/81,4 MiB for worker idle/load; 8,3/9,4 MiB remains to be recovered to reach 65/72. No changes to reference Celery, tools/compat, test_kombu_identity.py or dependencies.

## Changes adopted

- Zygote already closes all master fds before the first fork. Each child must only close the control, keeping its socket and logs: eliminated the second scan of all loggers, which eroded the CoW.
- Rust consumer returns free glibc pages after `on_ready`, even on reconnect. No GC, threshold, signal or environment changes.
- `bench.py` also saves smaps by mapping and thread count, without adding smaps reads to the frequent peak sampler.
- `ram_budget_probe.py`: explicit diagnostics for master, zygote, children and beat; modules, tracemalloc per file, reachable code object/docstring, mallinfo2, pymalloc statistics and Rust heap of build `memory-probe`.

## Before/after

MiB, PSS Linux. Fleets: budget 2048, c10, 10000 pure tasks completed; `fleet-10c-{before,after}.json`. The 8 GiB are estimates from 8192/PSS average, not a new fleet from 8 GiB.

| measure | before | after | limit |
|---|---:|---:|---:|
| fleet, worker idle | 78,1 | 73,3 | 65: FAILED |
| fleet, worker after load | 86,0 | 81,4 | 72: FAILED |
| actual idle workers within 2 GiB | 26 | 27 | — |
| worker loads estimated within 2 GiB | 23 | 25 | — |
| estimated workers in 8 GiB, idle/load | 104/95 | 111/100 | — |
| single c10 pure, idle/post latency | 85,36/102,60 | 76,65/97,10 | — |
| master pure, idle/post latency | 17,66/16,78 | 14,21/15,30 | 15: idle only PASS |
| zygote pure, idle/post latency | 6,34/7,53 | 5,65/7,06 | — |
| middle child as well, idle/after latency | 6,14/7,83 | 5,68/7,48 | — |
| single c10 bench-toplevel, idle/post latency | 85,69/137,55 | 78,01/137,76 | — |
| middle child bench-toplevel after latency | 11,20 | 11,08 | — |
| beat Redis, 3 entries | 23,36 | 23,71 | 24: PASS |
| beat AMQP, 3 entries | 22,64 | 24,31 | 24: FAILED |
| Redis client after get, PSS / anonymous | 24,15/19,445 | 21,66/19,441 | no increase |
| AMQP client after get, PSS / anonymous | 24,22/19,523 | 21,49/19,523 | no increase |
| pure p50 ms / tasks per second | 2,472/2459 | 2,216/2858 | ref 3,150/864: PASS |
| bench-toplevel p50 ms / tasks per second | 2,257/2828 | 2,124/2682 | ref not remeasured |

The PSS of shared files depends on other processes on the host: the initial baseline also ran during the fleet and measured 78,58/96,70; the repeat with smaps is `10c-before-physical.json`. Don't attribute all of the single PSS decline to the code. Private ones are more stable: idle master 11,98→10,79, idle child 3,50→3,12, child after latency 5,10→4,77. Client: identical 333 modules and anonymous memory unchanged. Beat not changed: The numbers do not demonstrate a change.

## Where every MiB goes

Physical PSS before changes, no probe in worker; middle child c10. `10c-before-physical.json` file, idle_processes/after_processes sections.

| role/status | heap glibc | anonymous, including Python arenas and native stacks | libpython/executable | Rust extension | other libraries | main stack | total | Private_Dirty |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| master idle | 4,42 | 8,30 | 0,46 | 3,07 | 1,28 | 0,04 | 17,66 | 11,98 |
| load master | 4,50 | 9,27 | 0,42 | 1,28 | 1,18 | 0,04 | 16,78 | 12,93 |
| zygote idle | 1,13 | 4,32 | 0,35 | 0,06 | 0,40 | 0,01 | 6,34 | 3,13 |
| zygote load | 1,62 | 5,04 | 0,32 | 0,07 | 0,38 | 0,03 | 7,53 | 4,38 |
| idle son | 1,16 | 4,49 | 0,29 | 0,07 | 0,06 | 0,01 | 6,14 | 3,50 |
| loaded son | 1,72 | 5,37 | 0,31 | 0,26 | 0,08 | 0,03 | 7,83 | 5,10 |

Smaps also keeps Private_Clean and Shared_Clean/Dirty for each mapping. Inherited heaps and anonymous are shared CoW only in the same worker; distinct worker Python objects are not shared. Clean file pages can also be shared between separate workers. New allocations, written CoW pages, TLS and stacks are private. The master has 13 thread; zygote 1; child 1 idle, 2 after the latency probe using the backend. The PSS of pthread stacks is not isolated from other anonymous mappings.

Beat baseline A/B Redis: 24,02 MiB, of which 21,10 anonymous and 21,13 Private_Dirty; full smaps in `10c-beat-ab.json`. The persistent schedule also loads sqlite3/dbm. The distribution by file should not be added to the Python heap: they are different views of the same pages.

Separate diagnostics, **with probe overhead**, in `10c-{heap,physical}-diagnosis.json`. Tracemalloc starts when the interpreter starts; the diagnostic build exposes Rust counters. No acceptance thresholds are evaluated with this build or with tracemalloc active.

| role | Python idle/load plot | code object / docstring reachable | Rust alive idle/load | free glibc idle/load without tracemalloc | arenas pymalloc idle/load | blocks+free pools idle/load |
|---|---:|---:|---:|---:|---:|---:|
| master | 19,04/22,54 | 3,40/0,82 | 0,190/0,275 | 2,04/2,13 | 17/18 | 1,06/0,91 |
| zygote | 27,25/— | 5,02/1,32 | 0,0042/— | 0,70/— | 23/— | 0,26/— |
| son | 30,95/34,41 | 5,02/1,32 | 0,0042/0,0042 | 0,69/0,68 | 25/26 | 0,74/3,15 |
| beat | 17,29/— | 3,03/0,76 | 0,0044/— | 0,61/— | 15/— | 0,62/— |

Tracemalloc lines for files and modules are in JSON; the loader allocates a lot of memory to importlib, so it is **not an exact allocation of the entire heap to each imported module**. The code object/docstrings are an estimate of the objects reachable from the namespaces, deduplicated. Main costs in addition to the loader: typing, enum, collections, defaults, click; in the zygote we add redis/commands, redis/cluster, redis/asyncio and kombu.transport.redis. Charged samples include temporary objects left over from the first diagnosis: do not infer natural growth from their delta.

## Experiments

Child's average Private_Dirty after latency (MiB); same family of c10 tests as well, 10000 task. Data `10c-<variante>.json`.

| variant | private son | decision |
|---|---:|---|
| baseline repetition | 5,17 | comparison |
| collect master before fork | 5,20 | discarded |
| trim master before fork | 5,20 | discarded |
| PYTHONMALLOC=malloc | 5,56 | discarded, it gets worse |
| glibc arena_max=1, trim_threshold=0, mmap_threshold=16384 | 5,17 | discarded, no reliable benefits |
| collect+freeze master | 5,20 | discarded |
| preload children/errors in master | 5,19 | discarded, load prohibited modules into the master |
| no second scan fd | 4,77 | adopted |
| deleted scan + warm build_tracer | 4,82 | further warm discarded |
| scan deleted + collect zygote | 4,99 | collect discarded |
| deleted scan + idempotent environment logging | 4,78 | further modification discarded |
| scan deleted + trim at end of master startup | 4,80 | adopted trim: idle private master 12,11→10,85 |

Trim in beat: alternate test, two samples per broker. Anonymous unchanged 21,10 MiB; Total PSS approximately 24,0→25,7. Extension mapping grows from 608 to 2344 KiB on first native call. **Discarded**: No recovery, new resident code pages. Smaller stacks and docstring removal not adopted: mostly virtual stacks; docstring/API must remain intact. No removal of modules from the task environment.

## Checks performed

- `cargo build --release`: PASS; also diagnostic build with `--features celery-py/memory-probe`, copied out of the normal path.
- `cargo clippy --all-targets -- -D warnings`: PASS.
- `tools/dev/with-brokers cargo test`: 243 PASS, 4 ignored by the suite.
- `tools/dev/build-package`: PASS, before measurements; editable installation.
- `tools/dev/with-brokers .venv-rs/bin/python -m pytest
  python/tests_rs/{test_ram_budget,test_child_memory,test_memory_growth,
  test_worker_c10,test_zygote_logging_fds,test_master_beat_restart,
  test_kombu_identity}.py -q`: 38 PASS, 157,10 s.
- First round targeted ram_budget + zygote_logging_fds: 6 PASS.
- Mandatory benchmarks, under `with-brokers timeout 300`, all exits 0: `bench/fleet.py --venv .venv-rs --budget-mb 2048 --tasks 10000` before/after; `bench/bench.py --venv .venv-rs --pool prefork --concurrency 10
  --task pure|bench-toplevel --prefill --n 10000 --latency-n 30|100` before/after; `bench/bench_beat.py --venv .venv-rs` and `bench/bench_client.py` before/after. Also of the reference remeasured with 100 latency samples.
- `git diff --check`: PASS. No gates, soak or compact integration performed.

Remains open: fleet limits, stable AMQP beat within 24 MiB, master within 15 MiB even after loading; Full mapping of per-module heap and pthread stacks. The commit contains verified savings and diagnostics, it does not certify the completion of the 10c spec.
