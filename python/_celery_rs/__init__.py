"""Core Rust di celery-rs. Il modulo nativo è `_celery_rs._celery_rs`; qui lo si ri-esporta
così `import _celery_rs` funziona sia installato col pacchetto `celery` sia da solo
(`maturin develop -m crates/celery-py/Cargo.toml`, usato nei test differenziali)."""
from ._celery_rs import *  # noqa: F401,F403
from ._celery_rs import __version__, _release_task_memory  # noqa: F401
