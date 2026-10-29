# 13b — Python shell, native package and tests in English

Read `specs/13-english-style.md` (mandatory rules) and `AGENTS.md`.

Scope: `python/celery/` (only the parts that differ from `celery/celery/`: about 50 files,
including every `_rs_*` module and `concurrency/rs_prefork.py`), `python/_celery_rs/`, and
`python/tests_rs/` (comments, docstrings, test names, helper names, messages).

Find what differs with: `for f in $(cd python && find celery -name '*.py'); do cmp -s python/$f celery/$f || echo $f; done`.
Lines identical to the reference must stay identical. Our code must read like Celery's own code.
Messages from Rust (`_celery_rs`) are being translated in parallel by 13a: if a test asserts an
Italian Rust message, make the assertion tolerant only if needed and list it in the report.
