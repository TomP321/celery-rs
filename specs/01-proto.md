# 01 — Protocol: Python-compatible JSON, saferepr, compression, task message v2

Phase 1 of `PLAN.md`. Target: The path to creating a client-side task message (`as_task_v2` + serialization + compression) in Rust, **byte-identical** to the reference.

## References (read only)

- `celery/celery/app/amqp.py` — `AMQP.as_task_v2` (lines ~325–430)
- `celery/celery/utils/saferepr.py` — `saferepr` (whole file) and `celery/celery/utils/text.py::truncate`
- `.venv-ref/lib/python3.13/site-packages/kombu/utils/json.py` — `JSONEncoder`, `dumps`, `loads`, `object_hook`
- `.venv-ref/lib/python3.13/site-packages/kombu/serialization.py` — `register_json`
- `.venv-ref/lib/python3.13/site-packages/kombu/compression.py` — content type and alias
- CPython `json` (C encoder): `ensure_ascii` rules, float repr, dict keys, loop detection

## Native module name

The `crates/celery-py` crate produces the top-level module **`_celery_rs`** (not `celery._rs`): so it can also be installed in `.venv-ref` next to the original Celery for differential testing. The Python shell (stage 3) will re-export it. Update `[lib] name` and `#[pymodule]` accordingly. Build/Install: `maturin` (add to `.venv-ref` with `uv pip install maturin`), then `VIRTUAL_ENV=.venv-ref .venv-ref/bin/maturin develop --release -m crates/celery-py/Cargo.toml`.

## A. `crates/celery-proto` (Pure Rust, no dependency on PyO3)

1. `json::write` — write primitives to `Vec<u8>` identical to CPython `json.dumps` with kombu defaults (`ensure_ascii=True`, separators `', '` and `': '`, `allow_nan=True`):
   - strings: escape `\"` `\\` `\n` `\r` `\t` `\b` `\f`, others `< 0x20` like `\u00XX`, everything that is not ASCII like `\uXXXX` lowercase, code point > U+FFFF as surrogate pair. The input is a sequence of code points (it must also handle isolated surrogates, which Python `str` can contain).
   - float: identical to `float.__repr__` (shortest round-trip; exponential notation when the decimal exponent is < -4 or ≥ 16, format `1e+16`, `1.5e-05`; `.0` for integer values; `-0.0`), with `Infinity`, `-Infinity`, `NaN`.
   - integers: native i64/u64; arbitrary integers already come as a decimal string.
2. `json::read` — event parser (pull/visitor, no intermediate tree) with CPython's `json.loads` semantics: accepts `NaN`, `Infinity`, `-Infinity`; integers of any size (returned as text slices when they do not fit in i64); float; escape `\uXXXX` including isolated surrogates; duplicate keys (last one wins, builder does); JSON whitespace; error with position (offset in characters like CPython) on invalid input or with trailing data.
3. `compression` — `compress(name_or_alias, &[u8]) -> (Vec<u8>, content_type)` and
   `decompress(content_type, &[u8])`. Same table as kombu:
   `application/x-gzip` (aliases `gzip`, `zlib`; zlib format, not gzip), `application/x-bz2`
   (`bzip2`, `bzip`), `application/x-lzma` (`lzma`, `xz`; xz format as in `lzma.compress`),
   `application/x-brotli` (`brotli`), `application/zstd` (`zstd`, `zstandard`).
   Requirement: interoperability in both directions with Python libraries
   (`zlib`, `bz2`, `lzma`, `brotli` if installed, `backports.zstd`); compressed bytes need NOT
   be identical. Unknown name: kombu raises `KeyError` from `get_encoder`; the Python module
   must raise the same `KeyError`.
4. Rust unit tests: table of floats (at least 200 values, including bounds and subnormals) with the expected output generated once with Python and saved as a fixture; string escape; parser on valid and invalid inputs.

## B. `crates/celery-py` → module `_celery_rs`

All functions construct/read Python objects **directly** (no intermediate structures like `serde_json::Value`) and release the GIL only where they do not touch Python objects (e.g. compression).

1. `json_dumps(obj, default) -> bytes` — identical to `kombu.utils.json.dumps(obj).encode('utf-8')`. `default` is the fallback callable (the shell will pass `kombu.utils.json.JSONEncoder().default`).
   - Native fast path for: `str`, `int`, `float`, `bool`, `None`, `list`, `tuple`, `dict` and **their subclasses** with the same rules of CPython's C encoder (e.g. `Signature` is a subclass of `dict` → JSON object; `IntEnum` → number via `int.__repr__`; subclasses of `float` via `float.__repr__`; `namedtuple` → arrays).
   - Dict keys: `str` as is; `int`/`float`/`bool`/`None` converted as CPython (`true`/`false`/`null`, number repr); other types → `TypeError` with the same message as CPython (`keys must be str, int, float, bool or None, not tuple`).
   - Any other objects (datetime, Decimal, UUID, bytes, objects with `__json__`, user-registered types with `register_type`, …): call `default(o)` and encode the result. If `default` raises, the exception propagates unchanged.
   - Circular references → `ValueError('Circular reference detected')`.
2. `json_loads(data, object_hook) -> object` — identical to `kombu.utils.json.loads(data)`: accepts `bytes`, `bytearray`, `memoryview`, `str`; constructs objects during parsing; for each dict call `object_hook(d)` (the shell will pass `kombu.utils.json.object_hook`; for performance: call the hook only for dicts with exactly the keys `__type__` and `__value__`, and document that it is equivalent because the kombu hook returns the other dicts unchanged). Errors → `json.JSONDecodeError(msg, doc, pos)` (same type and same location as CPython; the message must be the same at least for the common cases: `Expecting value`, `Extra data`, `Unterminated string starting at`, `Expecting ',' delimiter`, `Expecting property name enclosed in double quotes`).
3. `saferepr(obj, maxlen=None, maxlevels=3) -> str` — identical to `celery.utils.saferepr.saferepr`, including truncation (`truncate`), levels, `', ...'`, hanging brackets, empty sets/frozensets and subclasses, one-element tuples, non-UTF-8 bytes (hexadecimal), recursion protection (`<Recursion on list with id=...>` like `pprint._recursion`). For objects other than those handled natively in `reprstream` (non-builtin Number, Decimal, range, arbitrary objects) call Python's `repr()`, with the same `<Unrepresentable ...>` fallback as the reference.
4. `compress(body: bytes, name: str) -> tuple[bytes, str]` and `decompress(body: bytes, content_type: str) -> bytes`.
5. `encode_task_v2(...)` — equivalent to `AMQP.as_task_v2` followed by JSON serialization and optional compression. Signature (keyword parameters, same names as `as_task_v2`): `task_id, name, args, kwargs, eta, expires, group_id, group_index, retries, chord, callbacks,
   errbacks, reply_to, time_limit, soft_time_limit, create_sent_event, root_id, parent_id, shadow,
   chain, origin, ignore_result, argsrepr, kwargsrepr, stamped_headers, stamps,
   replaced_task_nesting, argsrepr_maxsize, kwargsrepr_maxsize, repr_maxlevels, compression, default`.
   - `eta`/`expires` already arrive as an ISO string or `None` (the conversion from countdown/datetime remains in the Python shell, because it depends on `app.now()` and the app's timezone).
   - `origin` arrives already calculated (the shell passes `origin or anon_nodename()`).
   - Same validation: `args` not list/tuple → `TypeError('task args must be a list or tuple')`; `kwargs` not `Mapping` → `TypeError('task keyword arguments must be a mapping')`.
   - Returns `(headers: dict, properties: dict, body: bytes, content_type: str,
     content_encoding: str, sent_event: dict | None)` where `headers`, `properties` and `sent_event` are **equal** (==, same key order) to those of `as_task_v2`, and `body` is identical to `json.dumps((args, kwargs, embed))` of kombu (then compressed if required; in that case `headers['compression']` is set as `Producer._prepare` does).
   - Serializers other than JSON are not in this spec: the shell will use kombu for pickle/yaml/msgpack.

## C. Differential Tests — `crates/celery-py/tests/test_differential.py`

Done with `.venv-ref/bin/python -m pytest crates/celery-py/tests -q` (original Celery and `_celery_rs` in the same process). Use random generator with fixed seed (stdlib `random`), no new dependencies. Each test compares the reference with `_celery_rs`:

1. `json_dumps`: ≥ 50.000 random nested values (depth ≤ 6) including: str with ASCII, BMP, astrals, control characters, isolated surrogates; int small, negative, > 2**64; limit float (`0.1`, `1e16`, `1e-5`, `-0.0`, `5e-324`, `1.7976931348623157e308`, `nan`, `inf`); bool; None; nested list/tuple/dict; dict with keys int/float/bool/None; `Signature` (`celery.signature('x', args=(1,))`); namedtuple; IntEnum; datetime aware/naive, date, time, Decimal, UUID, bytes UTF-8 and not; objects with `__json__`. Output **byte-identical**. Error cases (tuple key, non-serializable object, loop): same exception type and same message.
2. `json_loads`: round-trip of all values above (result `==` and same types) + a corpus of ≥ 60 handwritten, valid and invalid JSON strings (same result or same error type and same `pos`).
3. `saferepr`: ≥ 20.000 values × `maxlen ∈ {None, 0, 1, 5, 64, 1024}` × `maxlevels ∈ {0, 1, 3, 10}`, including recursive structures, set/frozenset (use elements with deterministic iteration order), subclasses of list/dict/tuple, bytes. Identical output.
4. `encode_task_v2`: ≥ 500 option combinations (all options of `as_task_v2`, random args/kwargs, with and without zlib compression). Comparison: `headers`, `properties`, `sent_event` equal to the result of `app.amqp.as_task_v2(...)` with the same `eta`/`expires`/`origin`; `body` identical to `kombu.serialization.dumps(body, 'json')[2]` (uncompressed, if compressed).
5. Compression: For each available algorithm, Rust→Python and Python→Rust on random payloads from 0 B to 1 MB.
6. Informative micro-benchmark (not a gate), printed with `-s`: `json_dumps` and `encode_task_v2` vs reference on a typical payload. Report the numbers in the final message.

## Acceptance criteria (execute them all and report the real outcome)

```
cargo build --release
cargo clippy --all-targets -- -D warnings
cargo test
VIRTUAL_ENV=.venv-ref .venv-ref/bin/maturin develop --release -m crates/celery-py/Cargo.toml
.venv-ref/bin/python -m pytest crates/celery-py/tests -q
```
All green, no tests skipped or marked xfail to pass the suite.

## Out of scope

Transports, result backend, `celery/` Python shell, v1 protocol, events. Do not touch `celery/`, `tools/compat/`, `bench/`.
