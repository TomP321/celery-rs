# 11c — immortal items and prefork load

CPython 3.13.9, Linux x86_64, `-P prefork -c 10`, 30.000 tasks `memapp.add`, Redis and RabbitMQ on the lanes of `tools/dev/with-brokers`. Date: 2026-10-28.

## Implementation

`_celery_rs._immortalize(gc.get_objects())` visits referents via `PyType_GetSlot(Py_tp_traverse)`, including untracked tuples. Does not use a visited set: marks before recursion and skips already immortal refcounts. Stack limited to 128 levels, ten pointers to native types for exclusions, no structure proportional to the number of objects visited. The traced roots are visited individually even if reached by deep paths. A chain of only untracked containers beyond the limit can remain partially normal.

Excludes `tp_finalize`, `tp_del`, and instances of the ten types exported by the core, even when Python exports are overridden. Writes only the low 32 bits of the refcount and keeps the high flags. The values come from the CPython headers: `UINT32_MAX` over [3.12](https://github.com/python/cpython/blob/3.12/Include/object.h) and [3.13](https://github.com/python/cpython/blob/3.13/Include/object.h), `3 << 30` over [3.14](https://github.com/python/cpython/blob/3.14/Include/refcount.h).

The visit occurs in the master before the zygote fork and in the zygote before the final freeze. This is followed by the release of the free pages of the allocator. `CELERY_RS_IMMORTALIZE=0` disables optimization before creating the GC list. Builds without GIL, with `Py_TRACE_REFS`, other interpreters, unsupported architectures, and versions other than 3.12–3.14 are not affected. No added dependencies. Weakref and lifecycle tradeoffs are described in `docs/DIFFERENCES.md`.

## Diagnosis under load

`sd.py` uses the real Rust pool with one child and Celery tracing from `add`. After the task 10 resets `clear_refs` in the child and in the master; after task 2000 it reads `pagemap`, the private memory and the dominant types in the written pages. The classification, which allocates, occurs only after the readings. Does not use tracemalloc. The master of this probe measures IPC and supervision; the complete broker/consumer path is measured separately from `loadpeak.py`.
| Topics | Immortals | Pages written son | Master written pages | Private Child Growth (KiB) | Master Private Growth (KiB) |
|---|---|---:|---:|---:|---:|
| integers | no | 260 | 252 | 16 | 80 |
| integers | yes | 144 | 170 | 44 | 64 |
| 16 KiB | no | 281 | 233 | 44 | 48 |
| 16 KiB | yes | 131 | 159 | 40 | 28 |

The written pages are not all new copies: many were already private to the task
10. All the detected pages belong to regions already present in the task 10. With the immortals, the objects `code` (50/51 pages), then `tuple` (24/17), `str` (21/16) remain dominant; without immortals `function`, `type` and descriptors are much more frequent. Adaptive interpreter counters remain writable even when the refcount is immortal.

The reduction in writes under load is 45% with integers and 53% with 16 KiB. Late private growth is a few tens of KiB. Preemptive compaction or backfilling of pymalloc has not been introduced: these measures do not demonstrate sufficient benefit to justify new retained arenas. The IPC protocol or task buffers have not been changed.

## Playback

Each row of the matrix uses a new process, a unique queue, and an exclusive lane. Final comparisons are performed sequentially, after build and test, to avoid contention with the linker and suite workers. Before = `CELERY_RS_IMMORTALIZE=0`; after = `CELERY_RS_IMMORTALIZE=1`.

```bash
for broker in redis amqp; do
  for bytes in 0 16384; do
    for enabled in 0 1; do
      tools/dev/with-brokers env CELERY_RS_IMMORTALIZE="$enabled" \
        .venv-rs/bin/python bench/memprobe/loadpeak.py \
        "$PWD/.venv-rs" 10 "$PWD/bench/memprobe" 30000 \
        --broker "$broker" --payload-bytes "$bytes"
    done
  done
done
```
`loadpeak.py` checks that the ten children exist before the rest measurement. Sample PSS every 200 ms, finish loading when producer is done and queue is empty, measure again after three seconds. The throughput includes production and consumption, like the original probe; the limit of the measurement is the small window of tasks already running when the empty queue is read. The peak affects the load interval, not the next three seconds. The values are MiB (KiB from `/proc` divided by 1024), although the spec calls them MB. PSS also depends on external processes that share libraries and bytecode.

Soft-dirty probe (repeat with `enabled=0/1` and `bytes=0/16384`):

```bash
tools/dev/with-brokers env CELERY_RS_IMMORTALIZE="$enabled" \
  .venv-rs/bin/python bench/memprobe/sd.py --payload-bytes "$bytes"
```

## Checks performed

| Command | Outcome |
|---|---|
| `tools/dev/build-package` | release installation in venv rs successful |
| `cargo build --release` | OK |
| `cargo clippy --all-targets -- -D warnings` | OK, no warning |
| `tools/dev/with-brokers cargo test` | 245 passed, 4 ignored |
| `tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q` | 274 passed, 57 warning, 928,02 s |
| `tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs/test_immortal.py -q` | 6 passed on the final version |
| `./compat api` | 0 differences on 137 modules + settings/signals/CLI |
| `git diff --check` | OK |

The full suite had already started when the last three no-op cases (unsupported version, free-threaded and trace-refs) were added: the final collection has 277 cases. All six final targeted tests were rerun after the latest changes, also covering these three cases and the new native exclusion assertions. The suite's 57 warnings include deprecations of `fork()` from threaded processes in pool tests.

The same six targeted tests were performed on the final ABI3 module with:

```bash
env PYTHONPATH="$PWD/python" uv run --python 3.12 --no-project --with pytest \
  python -m pytest -o addopts= python/tests_rs/test_immortal.py -q
env PYTHONPATH="$PWD/python" uv run --python 3.14 --no-project --with pytest \
  python -m pytest -o addopts= python/tests_rs/test_immortal.py -q
env PYTHONPATH="$PWD/python" uv run --python 3.11 --no-project --with pytest \
  python -m pytest -o addopts= python/tests_rs/test_immortal.py -q
```
Results: CPython 3.12.3 and 3.14.5, 6 each passed; CPython 3.11.15, 5 passed and 1 skipped (private pages measurement requires PEP 683). On CPython 3.13.9 all six pass. Marker, untracked referents, loops, finalizer and native exclusions (including `CronParseError` and export replacement), normal life of new objects, deactivation flag and page saving after a fork with 40% margin compared to the case without immortals are checked.

No gates, upstream, integration or soak have been launched: the subsequent checks entrusted to the orchestrator by the spec remain. No changes have been made to the baseline, compliance tools, or upstream tests.

## Final results

Sequential array of 30.000 tasks after suite completion; each cell reports **before → after**, in MiB PSS summed over master, zygote and ten children.

| Brokers / Topics | Rest | Peak load | After loading | Task/s |
|---|---:|---:|---:|---:|
| Redis / integers | 66,3 → 49,5 | 73,1 → 53,7 | 73,1 → 53,7 | 2829 → 2822 |
| Redis / 16 KiB | 66,0 → 49,6 | 74,8 → 57,3 | 74,8 → 57,3 | 1828 → 1825 |
| RabbitMQ / integers | 66,3 → 49,8 | 74,7 → 56,5 | 74,7 → 56,5 | 4219 → 3993 |
| RabbitMQ / 16 KiB | 66,5 → 49,8 | 77,8 → 58,1 | 77,8 → 58,2 | 2723 → 2979 |

Spec limits are for Redis with integer arguments: 49,5 ≤ 53 MiB idle, 53,7 ≤ 58 peak, 53,7 ≤ 56 after load. The throughput is 99,75% of the previous case (2822 versus 2829 task/s). Growth from rest to peak drops from 6,8 to 4,2 MiB. At the peak the master drops from 13,5 to 10,9 MiB and the sum of the children from 54,5 to 38,9 MiB.
RabbitMQ with integers showed a reduction of 5,4% in the first short comparison. Two additional pairs of 30.000 tasks produced 4107 → 4096 and 4266 → 3998 task(s); the medians are 4219 → 3998 (−5,24%). To measure the capacitance over a longer range and reduce the quantization of 200 ms, a pair of **300.000 tasks** was executed, again in sequence:

| RabbitMQ / integers, 300.000 tasks | Before | After |
|---|---:|---:|
| Rest (MiB) | 66,0 | 50,1 |
| Peak/after (MiB) | 74,8 / 74,8 | 57,3 / 57,3 |
| Task/s | 4382 | 4292 |

On prolonged load the ratio is **97,95%** (−2,05%), within the limit of 5%. The short comparison retains significant variability near the threshold; was not omitted from the report. It is not a soak: it is a finite measure of capacity on 300.000 tasks (approximately 70 seconds of load per worker), without restarting the broker.

The command is that of the matrix, replacing `30000` with `300000` and using `--broker amqp --payload-bytes 0`. The real logs are:

- [30.000 task matrix](11c-loadpeak.log)
- [Two RabbitMQ repeats](11c-amqp-repeats.log)
- [RabbitMQ 300.000 task](11c-amqp-long.log)
- [Soft-dirty between tasks 10 and 2000](11c-softdirty.log)

## Modified files and limits

- `crates/celery-py/src/immortal.rs`, `lib.rs`: Native visit, ABI guards, exclusions, unit testing and private function recording.
- `python/celery/concurrency/_rs_zygote.py`: calls in master and zygote, trim before fork, deactivate without creating GC list.
- `python/tests_rs/test_immortal.py`: isolated tests and measurement of private pages.
- `bench/memprobe/loadpeak.py`, `sd.py`: Payload from 16 KiB, RabbitMQ and soft-dirty probe on real pool path.
- `docs/DIFFERENCES.md`: object lifetime, weakref, receiver and introspection.
- This report and the four measurement logs.
The documented visit depth limit and variability of the short RabbitMQ benchmark remain. The RAM thresholds of the spec are checked on the required Redis/integer case; with larger payloads and RabbitMQ the real measurements are reported, without implicitly applying the same RAM thresholds to them.
