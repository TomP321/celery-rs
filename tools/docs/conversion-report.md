# README GIF verification

Regenerate with `uvx --with pillow python tools/docs/make_readme_gif.py`.
The generator needs `celery/celery/`, four dependencies from `.venv-ref` and system
DejaVu fonts. It adds no product dependencies.

Output: `docs/assets/celery-rs-conversione.gif`, 960 × 680, 201 frames,
12.5 fps for 16 s followed by a 5 s final pause, looping forever. The original
11b asset was 646,648 bytes, below 4 MB. The generator verifies the size limit,
duration, loop and inventory. It uses one global 128-colour palette and
`optimize=True`; encoded frame extracts go to `target/readme-gif/`.

The 4 × 3 grid has 11 named panels and a compact `porting.log`. Cells are 8 × 8 px
with 10 px pitch. Only `amqp/redis-py` groups up to two files per cell, as labelled.
Mixed files have a blue underline. Each Rust conversion flashes and sends a particle
with an eight-frame trail toward its package bar, avoiding label areas.

Title: “...% of runtime code in Rust”. Subtitle: “reference lines whose role is
handled by Rust”. Historical commit subjects are displayed with English captions.

## Percentages

| Package | Replaced role LOC / total LOC |
|---|---:|
| celery/app (includes other Celery modules, canvas and trace) | 0.34% |
| celery/worker | 5.37% |
| celery/concurrency | 72.31% |
| celery/backends | 3.79% |
| celery/beat+schedules | 6.23% |
| celery/events | 1.27% |
| celery/bin | 0.00% |
| celery/utils | 0.00% |
| kombu | 27.57% |
| billiard | 42.27% |
| amqp/redis-py | 36.67% |

Global: **46.00%**, excluding `original` and `absent` files from the denominator.
Count physical lines, including blanks and comments. The inventory contains 404 files,
including Celery modules outside the named groups; these belong to app so their Python
API cost remains counted. Reject missing files, duplicates, unknown statuses and missing crates.

This is role coverage weighted by reference LOC, rather than CPU time or Rust LOC.
Phases illustrate PLAN.md, not a file-by-file timeline. Bottom captions correspond
to selected subjects from `git log --reverse --format=%s`. Summary measurements come
from the README and the 10b fleet commit (105 idle workers, 96 under load).

## Mixed roles and limits

- Use the same role criterion for Celery and dependencies. Replaced I/O roles can
  retain Python wrappers. Mixed send/API/signal/state symbols count only delegated
  calls; interval unions prevent counting overlaps twice.
- `rust_symbols` lists whole symbols; `rust_calls` uses `symbol|call`. The generator
  recalculates fractions with the reference AST and rejects inconsistent fractions
  or missing symbols/calls. TOML comments record counts, delegation points and Python roles.
- Consumer connections, I/O registration, subscriptions and QoS use
  `RsConnection`/`RsTaskConsumer`. Gossip/pidbox use `_rs_mailbox`; mingle uses
  `_rs_control`. Signals, bootsteps, callbacks, Panel and node state stay Python.
- `worker/strategy.py` counts only task-received publishing via RsProducer.
  `heartbeat.py` counts sends, excluding timers, metrics and heartbeat_sent.
  The heart bootstep still constructs Heart. Rust handles AMQP heartbeats separately;
  loops excludes synloop and fallback.
- Redis counts delegated KV, connection and pubsub consumer I/O via `_rs_redis`;
  callbacks, metadata, chords and API stay Python. RPC counts `_rs_rpc` consumer I/O
  and RsProducer publishing, excluding cache, result conversion and callbacks.
- `app/amqp.py` counts TaskConsumer, producer pool and publish via
  `_rs_consumer`/`_rs_producer`, excluding routing, Task v1/v2 construction and signals.
  `app/control.py` counts broadcast I/O via `_rs_control`; Inspect, other Control
  methods and Panel stay Python. Canvas, trace, loaders and other modules dilute app coverage.
- `events/dispatcher.py` counts `_publish` I/O and RsProducer creation; clock, Event,
  send/publish API and buffer management stay Python.
- Kombu connection/messaging/message/pools/resource and supported transports count
  native I/O roles while public types remain importable. Rust replaces billiard
  supervision/IPC; zygote and trace retain Python. Optional AMQP/Redis cluster,
  sentinel and asyncio paths remain `original`; types/exceptions stay Python.
- `schedules.py` and `kombu/compression.py` retain their original measured fractions:
  native crontab/codecs, with Python timezones, registries and custom codecs.

### Reviewed Celery file shares

| Reference file | Rust role share |
|---|---:|
| `celery/app/amqp.py` | 3.89% |
| `celery/app/control.py` | 4.56% |
| `celery/backends/redis.py` | 18.69% |
| `celery/backends/rpc.py` | 20.49% |
| `celery/events/dispatcher.py` | 9.54% |
| `celery/worker/consumer/connection.py` | 24.07% |
| `celery/worker/consumer/consumer.py` | 10.82% |
| `celery/worker/consumer/events.py` | 5.33% |
| `celery/worker/consumer/gossip.py` | 5.34% |
| `celery/worker/consumer/heart.py` | 5.56% |
| `celery/worker/consumer/mingle.py` | 12.50% |
| `celery/worker/consumer/tasks.py` | 9.68% |
| `celery/worker/heartbeat.py` | 9.84% |
| `celery/worker/loops.py` | 50.22% |
| `celery/worker/pidbox.py` | 7.94% |
| `celery/worker/strategy.py` | 4.04% |

## Recorded 11b checks

Regeneration, py_compile and git diff --check passed. Encoded start, 25%, 50%, 75%
and final frames were inspected: labels fit, cells/flashes/particles were visible
and long log lines were ellipsized. Binary GIF parsing found one global palette
and no local palettes.

The original memory:// worker probe (memory backend, solo pool, concurrency 1)
started and stopped under tools/dev/with-brokers with exit 0. It used `.venv-rs`
and the worktree shell through PYTHONPATH. `target/readme-gif/imports.json` included
`_rs_consumer`, `_rs_mailbox`, `_rs_producer` and public Kombu/billiard/AMQP modules.
Imports do not imply Python I/O execution. This solo probe does not verify prefork;
the map uses source delegation points for that pool.

That revision changed no Rust, Python shell, reference Celery or compatibility tools.
Cargo was not run for 11b, as its spec required. No push was made.

## 13c English regeneration

Translated tool/benchmark comments, docstrings, help, diagnostics and reports;
updated output consumers together. Compatibility checks, verdicts and stable
`target/gates/<name>.log` names are preserved. Conversion map counts and fractions
match the original after translating the `original`/`absent` status names.
No product dependency was added. Reference Celery and historical benchmark reports
were left unchanged.

Both prescribed `uvx --with pillow python tools/docs/<script>.py` commands passed.
Four encoded frames per GIF (start, one-third, two-thirds, final) were inspected;
no clipped or overlapping text was found. Contact sheets are in
`target/13c-gif-review/`. Asset names and dimensions are unchanged.

| Asset | Dimensions | Bytes |
|---|---|---:|
| `celery-rs-conversione.gif` | 960 × 680 | 635,377 |
| `memoria-1-copy-on-write.gif` | 1920 × 1080 | 1,940,966 |
| `memoria-2-zygote.gif` | 1920 × 1080 | 1,948,423 |
| `memoria-3-inizializzazione.gif` | 1920 × 1080 | 985,692 |
| `memoria-4-bilancio.gif` | 1920 × 1080 | 585,513 |

The Italian-text scan found only preserved GIF filenames (`memoria-*.gif` and
`celery-rs-conversione.gif`) and the `é☃𝄞` protocol fixture. Benchmark Markdown reports belong to 13d and were excluded.

| Verification command | Result |
|---|---|
| `cargo build --release` | Passed |
| `cargo clippy --all-targets -- -D warnings` | Passed |
| `tools/dev/with-brokers cargo test` | 245 passed, 4 ignored, 0 failed |
| `tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q` | 283 passed, 57 fork deprecation warnings, 945.64 s |
| `tools/dev/build-package` | Installed the editable package in `.venv-rs` |
| `./compat api` | 0 differences in 137 modules plus settings/signals/CLI |
| `./compat cli` | 0 differences in 190 commands; English summary verified |
| `tools/dev/lanes status` | Four healthy Redis/RabbitMQ lanes |
| `python3 tools/dev/tests/test_lanes.py -v` | 5 passed |
| `.venv-rs/bin/python -m pytest bench/test_soak.py -q` | 3 passed |
| `tools/dev/with-brokers .venv-rs/bin/python bench/memprobe/loadpeak.py $PWD/.venv-rs 10 $PWD/bench/memprobe 3000` | Passed; peak 48.8 MiB, 2667 tasks/s |
| Python AST parsing, shell `bash -n`, `git diff --check` | Passed |

The first Python collection attempt found the worktree venv without the installed
package. `tools/dev/build-package` corrected the environment before rerunning.
Logs are retained in `target/13c-verification/`.

All required 13c checks passed. No acceptance item remains open.
