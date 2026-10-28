# celery-rs — wheel precompilate

Wheel di celery-rs pronte all'installazione (nessuna toolchain Rust necessaria).

| file | piattaforma | Python |
|---|---|---|
| `celery-5.7.0rc1+rs.0.1.1-cp310-abi3-manylinux_2_28_x86_64.whl` (consigliata) | Linux x86_64, glibc ≥ 2.28 (Debian bookworm, Ubuntu ≥ 20.04, …) | ≥ 3.10 |
| `celery-5.7.0rc1+rs.0.1.0-cp310-abi3-manylinux_2_28_x86_64.whl` | come sopra; l'extra `[redis]` blocca `packaging==26.3` | ≥ 3.10 |

Nel `pyproject.toml`, al posto di `celery==…`:

```toml
"celery[redis] @ https://github.com/TomP321/celery-rs/raw/wheels/celery-5.7.0rc1%2Brs.0.1.1-cp310-abi3-manylinux_2_28_x86_64.whl",
```
