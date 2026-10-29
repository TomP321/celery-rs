# 11b — Dynamic workflow style README GIF (replaces 11a)

The GIF of 11a (static rectangles with lists) is not good. The user wants an animation like the one on Bun's blog "Rewriting Bun in Rust" (https://bun.com/blog/bun-in-rust, section "Compiler errors as a work queue"): a dark box that animates, with each macro package's files converting to Rust one after the other, and a percentage bar per package filling. Read `AGENTS.md`. **Do not modify celery-rs code** (`python/celery/`, `crates/`).

## How is Bun's reference (to reproduce in style, do not copy trademarks)

Almost black background (`#0b0c10`), dark gray rounded edge, all in monospace.
- At the top left an orange pill badge (`#e8946f`, semi-transparent `#d97757` border) with the text "✻ CLAUDE CODE · DYNAMIC WORKFLOW".
- A large, bold white counter that changes ("≈13,376 errors left"), with a gray label, and an advancing date/time next to it.
- On the left a terminal style column `errors.txt` with `error:` rows in red flowing.
- In the center "DIVVIED UP · 64 CLAUDES": 4 `worktree 1…4` boxes, each with rows of small squares (fix → review → apply phases, in yellow, purple and green) and moving light particles.
- On the right "→ COMMITS LAND PER CRATE": for each crate name, colored bar that fills and number.
- At the bottom a scrolling commit log (`· phase-d(bundler): …`), the last line clearer.

## Our version

Badge: "✻ CLAUDE CODE · DYNAMIC WORKFLOW" (celery-rs was written with Claude Code and codex: that's correct). Small title: "CELERY → RUST". Texts in Italian.

1. **Large counter**: percentage of the execution path in Rust that goes up from 0 to the final one (e.g. "62% of the runtime in Rust"); next to the current phase (`fase 1 · protocollo`, …, `fase 10 · RAM`), from the phases of `PLAN.md`.
2. **Left column `porting.log`**: rows like `port  kombu/transport/redis.py → celery-transport/src/redis/…` in orange, `keep  celery/canvas.py (API Python)` in blue. They must be real mappings (see below).
3. **Center: a grid of files per major package** (this is the heart): each small square is a file `.py` of the reference. The files that celery-rs replaces with Rust light up from gray to orange one after another (with a `.py → .rs` label or a glowing particle going towards the package bar); those that remain Python turn blue; those not used by celery-rs (other brokers/backends, eventlet/gevent) remain dark grey. Macro packages: `celery/app`, `celery/worker`, `celery/concurrency`, `celery/backends`, `celery/beat+schedules`, `celery/events`, `celery/bin`, `celery/utils`, `kombu` (transports, messages, serialization), `billiard` (pool), `amqp`/`redis-py` (broker clients). If you need too many squares, group them together, but it remains readable.
4. **Right column "→ IN RUST PER PACKAGE"**: package name, filling bar, final percentage (package code lines replaced by Rust / total lines).
5. **Log at the bottom**: **true** commit messages from the repository (`git log --format=%s`, choose the significant ones, shortened), scrolling; the last line lighter.
6. **Final frame** (pause ~5 s): Full bars and a summary: 23.300 lines of Rust in 6 crate; worker `-c 10` 300 → 73 MB (−76%); in 8 GB 27 → 96–105 worker; Redis task/s ×3.

## Data: no made-up numbers

Create `tools/docs/conversion_map.toml` which gives **each** file `.py` of the packages above (from `celery/celery/` and from `.venv-ref/lib/python3.13/site-packages/{kombu,billiard,amqp,redis}`) a state: `rust` (in celery-rs its role at runtime is played by a crate: indicate which one), `python` (remains Python for public API, also executed in celery-rs), `originale` (untranslated path: other transports/backends, used only if user configures them), `assente` (not supported: eventlet/gevent). Rule to decide: look in `python/celery/` what has been replaced or overridden by modules `_rs_*` and `celery._rs`/`_celery_rs`, and what is imported from a real celery-rs worker/beat (you can check this with `python -X importtime` or `sys.modules` in a worker `memory://` started and stopped immediately, under `tools/dev/with-brokers`). A partially replaced file may have a fraction (`rust = 0.6`), justified in a comment. The script calculates percentages from row counts and the map; the global counter = rows `rust` / rows of `rust`+`python` (exclude `originale` and `assente`). Report the exit percentages in the report.

## Shape

- Script `tools/docs/make_readme_gif.py` (replace that of 11a), Pillow, executable with `uvx --with pillow python tools/docs/make_readme_gif.py`; DejaVu Sans Mono / Sans font (`/usr/share/fonts/truetype/dejavu/`).
- Output `docs/assets/celery-rs-conversione.gif` (overwrite), width 960 px, height ~540–600 px, 12–15 fps, 15–25 s, infinite loop, ≤ 4 MB (optimized palette, one global palette, `optimize=True`; you need to reduce fps, not readability). Smooth animation: bars and counter grow gradually, not in phase bursts.
- Update the alt text in `README.md` (the line is already there after the introduction).
- Mandatory visual check: extract at least 5 frames (start, 25%, 50%, 75%, end) in PNG and look at them: no cut or overlaid text, readable contrast.

## Rules

Blocking commands, no waiting messages. No cargo build/test: you don't touch Rust (just `git diff --stat` proves it). **Do not push.** Commit to branch (`11b: …`), short report: file, GIF size/duration/fps, percentages per package and global, questionable files in map.
