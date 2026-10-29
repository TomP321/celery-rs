# 08b — revision 2 (session 3, codex)

Read `AGENTS.md`, `specs/08b-memory.md` and `specs/08b-review.md` (binding decision: real kombu/billiard objects, no rewritten `__module__`/`__qualname__`/`__name__`, no replacement classes; `python/tests_rs/test_kombu_identity.py` does not change).

## Status (commits `61699ed`, sessions 1–2 of another agent)

- Green gates at last run: `compat api` 0, `wire` 0/40, `wire-worker` only 0/22, `wire-beat` 0/2, `cli` 0/190, `upstream` 0 regressions, `test_kombu_identity.py` 3/3, `crates/celery-py/tests` 81. `tests_rs` complete: last round had 2 failures (`test_master_does_not_import_broker_modules`), then corrected but not everything re-executed. `wire-worker --pool prefork` not re-executed.
- tried and discarded mimalloc (solo 34→40 MB, prefork 71→77 MB); glibc arenas: no gains. Report these numbers in the final message, do not repeat the test.

Current measurements (`bench/results/rs-08b-final-*.json`, `client.json`, `beat-rs-beat-*.json`):

| what | ref | main (before 08b) | now | objective |
|---|---|---|---|---|
| PSS client after `get()` | 34,6 MB | 30,6 | 27,2 | ≤ 26 |
| master prefork c4 idle | 29 | 20,9 | 19,4 | ≤ 16 |
| `-P solo` Redis idle | 49,7 | 37,0 | 35,4 | ≤ 30 |
| prefork c4 `--task pure` peak | — | — | 73 | as low as possible |
| beat 3 entries | 51 / 43 | 45,7 | 32 | ≤ 38 ✅ |
| beat send/s Redis | 406 | 497 | 430–456 | ≥ main |

## To do

1. **Client ≤ 26 MB** honestly. Modules still loaded by the client in addition to those imposed by real kombu objects (`kombu.utils` → `amqp`/`billiard`/`importlib.metadata`, `kombu.entity`/`kombu.serialization` → `yaml`/`msgpack`): check with `bench/bench_client.py` and `python -X importtime` who carries `click` (e.g. `CeleryCommandException` in `celery.exceptions`), `greenlet`, `dateutil`, `tzlocal`, `kombu.transport.*`, `redis`, and charge them only where you need them. Allowed: `__getattr__` of module (PEP 562) that creates/imports the object on first access, as long as the resulting object actually lives to the declared name (verified by identity test and `./compat api`). Update `python/tests_rs/test_import_diet.py` accordingly.
2. **Beat**: Send/s on Redis dropped ~10% compared to `main` (497). Find the cause with a profile (`bench/bench_beat.py`, `cProfile` of the send loop) and fix it, or prove with 3 alternating main/branch measurements that it is noise.
3. **Worker master**: Breakdown of master PSS (prefork and solo) for Python modules (`tracemalloc`/`sys.modules`), Rust heap, mapped libraries (`/proc/<pid>/smaps`); reduce what you can honestly. If ≤ 16 / ≤ 30 MB is not reachable with real objects, write in the final message what it weighs and why.
4. **Full gates** of `specs/08b-memory.md` plus `test_kombu_identity.py`, each with the actual outcome; final bench table ref / main / branch (client, solo and prefork Redis+AMQP with `--task pure` and normal, beat, CLI).
5. Commit to the branch (`08b: …`) and close with the report: what you did, files touched, commands and outcomes, what remains open.

Rules: Tests using Redis/RabbitMQ always under `tools/dev/with-brokers`; do not modify `celery/` (reference), `tools/compat/`, `celery/t/`; after each change to imports `pyflakes` on the touched files (`uvx pyflakes …`).
