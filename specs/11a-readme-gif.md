# 11a — GIF for the README: What's in Rust and what's not

Goal: An animated GIF for the GitHub README that shows, at a glance, which Celery components have been ported to Rust, which ones remain in Python by choice (public API), and what is not covered. Read `AGENTS.md`, `README.md`, `docs/DIFFERENCES.md`. **Do not modify celery-rs code** (`python/celery/`, `crates/`): Documentation and GIF script only.

## Content (classification already verified by the orchestrator; check it on the code e

report any discrepancies in the report, without inventing)

**In Rust** (orange):
- Message protocol: task v1/v2, header, JSON, compression — `celery-proto`
- Redis transport, RabbitMQ transport (AMQP 0-9-1), `memory://` transport — `celery-transport`
- Producer: `delay()`/`apply_async()` (serialization and sending)
- Redis and RPC Results Backend — `celery-backend`
- Worker consumer: QoS/prefetch, ack, events, heartbeat, ETA timer, rate limit, revocations —
  `celery-worker`
- Remote control: pidbox, mingle, gossip (transport in Rust; commands remain Python)
- Prefork pool: supervisor, zygote, IPC with children
- Beat: calculation of deadlines (crontab) and sending — `celery-beat`
- CLI Control Client (`inspect`, `control`)

**In Python by choice** (blue: public API identical to Celery, user extensible):
- App, configuration, registry and class `Task`
- Canvas (chain, group, chord), `AsyncResult`
- Signals, bootsteps, control commands (`Panel`)
- Execution of tasks in children (trace) and user code
- CLI (click), loader, Django fixup
- Persistence of the beat schedule (shelve)

**Original, unconverted Celery path** (gray):
- Other brokers (SQS, …) via kombu
- Other results backends (database/SQLAlchemy, cache, S3, MongoDB, DynamoDB, …)

**Not supported / not tested** (red):
- Pools `eventlet` and `gevent`
- macOS and Windows (Linux only)
**Numbers to show at the end** (summary): 23.300 Rust lines in 6 crate, Python shell 52.600 lines; worker `-c 10`: 300 → 73 MB (−76%); worker `-c 10` to 8 GB: 27 → 96–105; throughput `-c 10` Redis ×3; tested with the public API (137 modules), broker messages, 190 CLI commands, 31.691 unit tests, and Celery's `t/integration` suite.

## Shape

- Reproducible script `tools/docs/make_readme_gif.py` (Pillow, executable with `uvx --with pillow python tools/docs/make_readme_gif.py`), output `docs/assets/celery-rs-conversione.gif`.
- 960 px width, readable text on GitHub (DejaVu Sans font, already on the system), light background, accessible colors and a legend. Animation: components appear/color by groups (Rust, then Python, then unported, then unsupported), then the summary; long pause on last frame; infinite loop. File size ≤ 1,5 MB, duration 10–20 s.
- Labels in Italian, like the rest of the README.
- Insert the GIF in `README.md` immediately after the introduction, with alternative text.
- Look at the result (extract 3–4 frames in PNG and read them) before declaring finished: no cut or overlapping text.

## Rules

Blocking commands, no waiting messages. **Don't push.** Commit to the branch (`11a: …`) and close with a short report (files created, GIF size and duration, any discrepancies in classification).
