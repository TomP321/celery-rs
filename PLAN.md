# celery-rs — Plan

Rewriting Celery (reference: `celery/` 5.7.0rc1, ~47k Python lines) with core in Rust.

## Objectives

1. **Drop-in Python**: `pip install celery-rs` provides the `celery` package. The imports, `Celery(...)`, `@app.task`, `Task`, `.delay/.apply_async`, `AsyncResult`, canvas (`chain/group/chord/signature`), signals, config (`app.conf.*`), CLI `celery worker|beat|inspect|call|purge|...` remain identical.
2. **Protocol interop**: Task Protocol v2 (and read v1) messages, events, and control/broadcast identical byte-for-byte (except id/timestamp). Original Celery client ⇄ Rust worker and vice versa.
3. **Broker/backend v1**: Redis, RabbitMQ (AMQP 0.9.1 + backend `rpc://`), `memory://` / `cache+memory://`.
4. **Minimum memory use and maximum CPU efficiency** (see §Optimizations), always measured against the reference.

Out of scope v1: SQS, DB/SQLAlchemy, Mongo, DynamoDB, GCS, etc. (`ImportError` remain clear); pool `eventlet/gevent` (v2).

## Architecture: "Rust core, thin Python shell"

You cannot execute Python user code without a Python interpreter, and the public API includes classes that user extends (`Task`, `Celery`, bootsteps, signal handler). So:

```text
                      ┌──────────────── User Python process (client) ───────────────────────┐
                      │ celery/ (Python shell: same classes/signatures)                     │
                      │   └─ celery._rs (PyO3) → protocol, serialization, producer,          │
                      │                         result backend, connection pool             │
                      └─────────────────────────────────────────────────────────────────────┘
 broker (Redis/AMQP/memory)
                      ┌──── celery worker (Python master + embedded Rust core) ─────────────┐
                      │ async consumer (tokio), QoS/prefetch, ack, heartbeat, events,        │
                      │ control/broadcast, ETA timer, rate limit, revoke, autoscale         │
                      │   └─ pool: zygote (fork before Rust threads) → N Python children     │
                      │        IPC: pipe/shm, binary frames, execution via celery.app.trace │
                      └─────────────────────────────────────────────────────────────────────┘
```

- **Hybrid worker master** (decision made after 2026-10-24, measures see below): Python process that imports the app and contains the Rust core (tokio threads for I/O broker, consumer, QoS, ETA timer, rate limit, revocation, events, heartbeat, control built-in, pool supervision). Custom signals, bootsteps and control commands remain Python, in the same process where Celery executes them: identical semantics. Kombu, billiard and asynpool are no longer loaded in the worker.
- **Zygote**: forked by master *before* starting Rust threads (safe fork, single thread), with the app already imported and `gc.freeze()`; creates children upon master request via pipe.
- **Children**: CPythons running only `trace_task`, sharing copy-on-write app pages with zygote and master.
Why not a master in pure Rust: empty interpreter 5 MB PSS, `import celery` 6 MB, app + Celery worker machinery 38 MB. Children still need a Python process with the app loaded to be forked from; if that process is the master, its pages are shared with children and the additional PSS is small, while a pure Rust master would require the same Python process as zygote and would have to emulate user signals/bootstep/controls outside their process.
- The **Python shell** maintains Celery modules and public signatures; heavy internal parts delegate to `celery._rs`. `kombu`/`billiard`/`vine` remain importable dependencies only for the public types that the user uses (`kombu.Queue`, `Exchange`), not for transport.

## Repository structure

```
celery-rs/
  Cargo.toml                  workspace, profile release: lto=fat, codegen-units=1, panic=abort
  crates/
    celery-proto/             v1/v2 messages, headers, embed, serializer (json/msgpack/pickle-passthrough), compression
    celery-transport/         trait Transport + redis (fred/redis-rs), amqp (lapin), memory
    celery-backend/           result backend: redis, rpc, memory; chord counter, group results
    celery-worker/            consumer, strategy, request, timer, rate limit, revoke, control, events, pool supervisor
    celery-beat/              scheduler (crontab/solar/interval), persistent store
    celery-py/                PyO3 → top-level `_celery_rs` module (also installable beside
                              original Celery for differential tests; re-exported by the shell)
  python/celery/              Python shell (same structure as celery/celery)
  tools/compat/               conformance tools (§Tests)
  bench/                      memory/throughput benchmarks vs reference
  vendor/celery-ref/ -> ../celery  reference source (read-only)
```

## Optimizations (measurable objectives)

| Area | Technique | Target vs Celery 5.7 |
|---|---|---|
| Master worker | Python + Rust core (no kombu/billiard/asynpool), tokio, mimalloc | Master PSS < 12 MB (vs. 29 MB) |
| Children | fork from zygote + `gc.freeze()` → maximum copy-on-write; `max_memory_per_child` managed by the master | Private/Child RSS −40% |
| Messages | `bytes::Bytes` zero-copy from socket to child pipe; the master does not decode the body | 0 alloc for body in master |
| Header | parse lazy/borrowed (`serde` with `&str`), interned task names (`Arc<str>`), `compact_str` | |
| JSON | `sonic-rs`/`simd-json` for encode/decode in client and backend | encode ≥ 3× faster |
| IPC pool | frame length-prefixed on pipes, buffers reused, no pickle for transport | |
| Structures | `revoked`/`active` with `hashbrown` + Limited LRUs, Heap Timers, `SmallVec` | |
| Events | batch + heartbeat coalescence, `events.state` compacted | |
| Build | LTO fat, `opt-level=3`, `panic=abort`, configurable target-cpu | |

Each optimization only enters if the compliance tool remains green and the bench confirms it.

## Compliance Tool — `tools/compat` (`celery-compat`)

Two virtualenvs: `venv-ref` (Original Celery from `celery/`) and `venv-rs` (our package). Infrastructure via docker on dedicated ports: Redis `:6390`, RabbitMQ `:5673`.

1. **`compat api`** – public surface diff: for each public module enumerates names, types, signatures (`inspect.signature`), MRO, constants (`states`, `defaults` with values), exceptions and hierarchy. JSON output + difference report. Gate: 0 differences on modules in scope.
2. **`compat wire`** – capture of raw messages on the broker (Redis `MONITOR`/list reading, AMQP consumer on capture queue) for a scenario run by ref and rs; normalize id/timestamp/hostname; diff of properties, headers, body. It covers apply_async with all options, canvas, events, control/broadcast, result in the backend.
3. **`compat matrix`** – end-to-end scenarios on {client ref, client rs} × {worker ref, worker rs} × {redis, amqp, memory}: delay/get, retry, exceptions, countdown/eta/expires, revoke/terminate, time limit, rate limit, chain/group/chord/starmap/chunks, inspect/control, beat.
4. **`compat upstream`** – runs `celery/t/unit` and `celery/t/integration` against `venv-rs`; trace pass-rate per file, with explicit list of excluded tests (kombu/billiard internal detail tests) justified one by one.
5. **`compat bench`** – RSS idle/load (master+children, PSS via `/proc/*/smaps_rollup`), throughput task/s, latency p50/p99, same config, ref vs rs.

## Phases

| # | Phase | Contents | Exit gate |
|---|---|---|---|
| 0 ✅ | Setup | workspace, venv ref/rs, docker redis+rabbit, `tools/compat` skeleton, `compat api` working, reference baseline bench | baseline saved |
| 1 ✅ | Protocol | `celery-proto`: CPython compatible JSON, compression; `_celery_rs`: `json_dumps/loads`, `saferepr`, `encode_task_v2` | byte-identical differential tests vs reference (`crates/celery-py/tests`) |
| 2 ✅ | Transport | 02a API+memory, then 02b Redis and 02c AMQP in parallel (separate worktrees) | interop with kombu + `compat wire` on commands |
| 3 ✅ | Python Client | shell `celery`: app, conf/defaults, Task, registry, routing, amqp producer, signature/canvas, AsyncResult/GroupResult | `compat api` green; unit `t/unit/app`, `tasks`, `test_canvas` |
| 4 ✅ | Result backend | redis/rpc/memory, chord join/counter, get/wait/propagate | matrix: client rs + worker ref green |
| 5 ✅ | Workers | Python master + Rust core (consumer, rsloop on epoll, zygote+pool), trace, ack late, retry, time limit, revoke, events, control, autoscale, shutdown/warm/cold, signals | full matrix green |
| 6 ✅ | Beat | scheduler, crontab/solar, persistence | `test_beat` + `t/unit/app/test_schedules` |
| 7 ✅ | CLI | `celery worker/beat/call/inspect/control/purge/result/events/status/multi` | identical output and exit code |
| 8 ✅ | Optimization | **Priority: worker RAM (prefork c10) and beat vs original, then performance.** 8a ✅ pidbox/mingle/gossip on the Rust transport; 8b ✅ memory diet with real kombu objects (client 27 MB vs 35, beat 32 MB vs 43–51, discarded mimalloc); 8d ✅ worker c10 (PSS idle 87 MB vs 309, child 6,3 MB vs 29; cffi preload in the zygote, one less IPC copy); 8e ✅ parallel gate on 4 broker lanes (`tools/dev/gates`, full run 9,5 min instead of ~60); 8f ✅ Beat RAM (compact crontabs in Rust, a single copy of the schedule: 2,7 KB/entry, 10.000 entries 54 MB vs 118 ref, sends/s +50%; open: basic 27 MB vs target 24, `-B`); 8c ✅ latencies without increasing RAM (rpc p50 2,80 vs 3,52 ms ref, ping AMQP 8,49 vs 8,92 ms; c10 capacity with `--prefill`: Redis ×3,2, AMQP ×1,9; realistic app c10 153 MB vs ~313); open: synchronous Redis acks; `get_type_hints(worker_direct)` | table target §Optimizations |
| 9 ✅ | Hardening | 9a ✅ `t/integration` complete identical to the reference (`./compat integration`, Redis and AMQP, 0 regressions; corrected revocations, Redis TTL, pubsub, AMQP properties, logging fd closed by zygote which corrupted Redis sockets); 9b ✅ parser limits (JSON depth, decompression, data after envelope) + fuzz proptest; fork safety of Rust destructors (billiard segfault no longer reproduced); Redis memory in children (preloaded errors in zygote, glibc pages released after payload ≥1 MB, last job freed): worker c10 after 1 h under load 286 MB vs 378 ref (Redis), 284 vs 422 (AMQP); 9c ✅ growth over time equal to the reference: traceback cycles (`ExceptionInfo`) released after READY; soak 60 min Redis c10 190→220 MB (+30, 1110 cycles) vs ref 333→365 (+32, 890 cycles), after reboot rs ~12 MB/h vs ref ~30; open: jump to broker restart +14 MB vs +7 ref | `t/integration` complete |

## Working method (orchestration)

- **Me (Claude)**: plan, module specifications, compliance tools, code reviews, test/bench execution, architectural decisions.
- **DeepSeek v4.1 flash** (`opencode run -m opencode-go/deepseek-v4.1-flash --auto`) implements one task at a time starting from a spec in `specs/NN-nome.md` (exact references to Celery files to replicate, acceptance criteria, test commands).
- Cycle per task: spec → DeepSeek implementation → `cargo test` + `celery-compat` → review → fix (sent back to DeepSeek with the test output) → merge on git with commit per task.
- No task is "done" until its phase gate is green.

## Main risks

- **Upstream tests mocking internals** (kombu/billiard): some are not significant to us; each exclusion must be documented in `tools/compat/excluded.toml`.
- **pickle**: an interpreter is needed; the master treats it as an opaque body, decoding only in the children/clients.
- **User subclasses of internals** (custom Consumer bootsteps, custom pool): supported only where the API is documented; Consumer bootsteps in Python are v2.
- **Size**: ~47k reference lines; the priority is the common path (phases 1–5).

## Reference Baseline (Phase 0, 2026-10-23)

Celery 5.7.0rc1, Python 3.13.9, prefork pool `-c 4`, 20.000 task `noop` (`bench/bench.py`, results in `bench/results/`). Memory = PSS summed over master + children.

| | Redis | RabbitMQ |
|---|---|---|
| PSS worker idle | 146 MB | 145 MB |
| PSS peak under load | 151 MB | 149 MB |
| Client: publish/s | 1.720 | 2.915 |
| End-to-end task/s | 902 | 2.878 |
| Latency `delay().get()` p50 / p99 | 3,05 / 3,96 ms | 2,95 / 3,83 ms |

Reference test suite unit: 31.691 test; 3 failures also known in the reference (`tools/compat/out/upstream-unit-venv-ref.json`), which `compat upstream` does not count as regressions. Surface API in scope: 137 modules, 750 public names, 207 settings, 34 signals, 38 CLI commands.

## Phase 10 — RAM beyond initial plan (owner priority: as little RAM as possible)

- 10a ✅ jump to broker restart (Redis +44 → +2,2 MB, AMQP +3,7 MB, 0 failed tasks: reset closed backend sockets); master c10 22 → 18,3 MB (lazy asynpool/billiard, `TaskPool.Pool/BlockingPool` among accepted differences of `compat api`); build `opt-level=s` dependencies with crontab to 3 (p50 2,01 ms vs 2,48 ref). Open: master ≤ 13 MB, beat ≤ 22 MB (Python heap and core modules).
- 10b ✅ Children's RAM under load: native `prctl(PR_SET_PDEATHSIG)` instead of cffi in each child, targeted release of free pages; child c10 pure 9,97 → 8,09 MB, c10 bench-toplevel 12,42 → 11,38 MB, c50 5,88 / 9,39 MB.
- 10c ✅ RAM balance (`bench/ram_budget_probe.py`): children without fd/logger rescan (CoW preserved), release master pages at startup; worker c10 78,1 → 73,3 MB idle, 86,0 → 81,4 under load; master 17,7 → 14,2. Independent verification `bench/ram_check.py`: PSS = MemAvailable drop (rs 78,2 vs 77,6 MB/worker).
- 10d ✅ zygote only closes Rust runtime fds (registry in `celery-worker/src/fds.rs`): files, sockets and loggers opened by user code remain inherited as in the reference.
- Published on github.com/TomP321/celery-rs; wheel manylinux_2_28 x86_64 in branch `wheels`.
- Open: `test_worker_pool.py::test_hard_time_limit_replaces_child[prefork]` (pool billiard inside pytest with Rust thread) hung once and failed 2 times on ~30 with the machine under gate load; 0/20 without load, on main and 10c.
- 11c ✅ immortal objects (PEP 683, `crates/celery-py/src/immortal.rs`) in the master before the zygote and in the zygote before the freeze, excluding types with finalizer and `_celery_rs` objects; CPython 3.12–3.14 with GIL only. Python 3.13 c10 Redis: rest 66,3 → 49,5 MB, peak under load 73,1 → 53,7 (`bench/memprobe/loadpeak.py`); ref 352/353. `CELERY_RS_IMMORTALIZE=0` to deactivate.
- 11d ✅ RAM before speed (floor = reference throughput, 834 task/s Redis c10): `gc.collect()`+`gc.freeze()` in master before zygote, release memory after each task. Python 3.11 c10: rest 76,5 → 72,5, peak 88,9 → 84,3 MB; 3.13 unchanged (±2 MB). Discarded with measures: pages shared between siblings, adaptive caches turned off, pre-built tracers, filling pymalloc pools, lapin with small buffers (`bench/results/11d-ram-over-speed.md`).
- Wheel 0.1.2 (x86_64, aarch64) and selector 0.1.2 in branch `wheels`.
- 12 ✅ children initializer prepared in the zygote (`prepare_process_initializer`: `log.setup`, `finalize`, shared tracers; signals remain in the child, loader hooks, `worker_process_init`). c10 Redis: 3.11 rest 72,5 → 62,1, peak 84,3 → 78,6 MB; 3.13 51,2 → 46,5 and 56,2 → 52,1. Rejected with measures: `opt-level = "z"` (no gain), superfluous kombu modules (~2 MB, would require altering kombu). Wheel and selector 0.1.3.
- Open: `t/integration/test_backend_leak_in_fixture_teardown.py` fails with rs (`RsConnectionPool` does not expose redis-py's `_available_connections`/`_in_use_connections`); previously masked by the reference timeout to ~120 s. It is not a regression of 11c/11d.
- Reference measurements: `bench/fleet.py` (8 GB: ref 27 worker c10, rs 89), `bench/results/scale-*` (c1–c100).
