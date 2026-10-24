# celery-rs

Celery con core in Rust e API Python identica. Vedi `PLAN.md`.

- `python/celery/` — pacchetto `celery` (guscio Python, partito dal sorgente di Celery 5.7.0rc1)
- `crates/` — core Rust; `_celery_rs` è il modulo nativo
- `./compat api|wire|upstream` — conformità con Celery di riferimento
