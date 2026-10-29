# 08b — revision of session 1 (to be applied before declaring finished)

The work of the session 1 is in commit `f4f90b9` (wip): diet of client, worker and beat imports, `upstream` to 0 regressions, `compat wire/wire-worker/wire-beat/cli` to 0. The problems below remain. Read also `specs/08b-memory.md`.

## Project owner's decision: real kombu items

Where the reference uses or exposes a kombu/billiard object (classes, functions, instances), celery-rs must use **that real object or a subclass of it with its own honest name**, even if it costs memory (measured: approximately +5 MB in the client for `kombu.utils`, which carries `amqp`, `billiard`, `importlib.metadata`; 1,5 MB for `kombu.entity`/`kombu.serialization`). So:

1. **Do not rewrite `__module__`, `__qualname__`, `__name__`, `__class__`** to make an item sound like kombu/billiard or another name for celery. Remove all the occurrences introduced (in `_compat.py`, `utils/imports.py`, `app/trace.py`, `utils/functional.py`, the placeholders of `_lazy.py`…) and the one already present in `main` in `app/_fast_mailbox.py` (`FastMailbox` → must be a subclass of `kombu.pidbox.Mailbox` with its own name).
2. **No replacement classes**: `celery/app/_fast_entities.py` (fake Queue/Exchange) should be removed; `app.amqp.queues`, `default_exchange`, router, `Queues.new_missing` return true `kombu.Queue`/`kombu.Exchange`. `producer.connection` must be instance of `kombu.connection.PooledConnection` as in the reference (before 08b `FastConnection` was derived from `kombu.Connection`: back to that model, with pool as the reference). Placeholders that "pretend" to be kombu/billiard classes (`lazy_class_attr` and similar in `_lazy.py`) are not allowed: either the real object, or a module `__getattr__` (PEP 562) that returns the real object on first access.
3. Lazy imports **honest** remain welcome: a module that uses kombu only in a rare branch imports it into that branch; a re-exported name resolves to form `__getattr__`; an expensive item is created on first access. But everything a class uses in its definition (bases, decorators like `cached_property`) really needs to be imported.
4. **Bug found**: With `app.conf.task_queues = [kombu.Queue('extra', kombu.Exchange('extra',
   type='direct'), routing_key='extra')]`, `app.amqp.queues['extra']` raises `TypeError`.

## Identity test (written by orchestrator: DO NOT modify)

`python/tests_rs/test_kombu_identity.py`:
- `test_no_masqueraded_names`: each class/function reachable from the `celery.*` modules must be the object that is actually located at the declared `__module__` + `__qualname__`;
- `test_runtime_objects_are_kombu_objects[redis|amqp]`: for queues, exchange, connections, producer, mailbox, dispatcher... the kombu classes of the reference MRO must appear (`isinstance` on the real class) also in celery-rs. Today it fails with 19 disguised names and ~20 objects at runtime: everything must pass.

## Test to reset

- `test_import_diet.py`: Put back real identity checks (`issubclass(mlazy,
  kombu.utils.functional.lazy)`, `is`), not name checks. Update the list of banned modules to what remains honestly avoidable (for example `yaml`/`msgpack`/`kombu.transport.*`/`redis`/ `click`/`greenlet` if they are really not needed), justifying each remaining module in the final message.
- `test_producer.py::test_producer_exposes_kombu_connection_without_opening_it`: return as in `main` (kombu connection, not open).

## Still open targets of the 08b spec

- **Client**: new PSS target after `get()` ≤ 26 MB (ref 34,6) with real objects.
- **Prefork children**: with `--task pure` the children remain at ~11 MB until they save a result; after `add` tasks (result in backend) they go up to ~24 MB each. Find what imports the result path into the child and load it into the zygote before `gc.freeze()` or avoid it. Target `--task pure` ≤ 60 MB peak and after load.
- **Master**: prefork 19,4 MB (target ≤ 16), `-P solo` Redis 35,4 MB (target ≤ 30): with the above rule some MBs will return; measures and reports the decomposition required by point 3.
- **mimalloc / arenas glibc** (point 4): Not tried yet. Try it and report the numbers even if you discard it.
- **Beat**: sends/s dropped from 497→464 (Redis) and 553→510 (AMQP) versus `main`: repeat measure 3 times; if it is real, find the cause.

## End

All gates of `specs/08b-memory.md` + `python/tests_rs/test_kombu_identity.py`, with the real outcome. Commit to the branch when finished (message `08b: …`).
