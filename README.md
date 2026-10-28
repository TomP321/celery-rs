# celery-rs — wheel precompilate

Wheel di celery-rs pronte all'installazione (nessuna toolchain Rust necessaria). Linux, glibc ≥ 2.28
(Debian ≥ 10, Ubuntu ≥ 20.04, immagini `python:*-slim`/`-bookworm`), Python ≥ 3.10.

| file | architettura |
|---|---|
| `celery-5.7.0rc1+rs.0.1.1-cp310-abi3-manylinux_2_28_x86_64.whl` | x86_64 (Intel/AMD) |
| `celery-5.7.0rc1+rs.0.1.1-cp310-abi3-manylinux_2_28_aarch64.whl` | aarch64 (ARM: Mac Apple Silicon in Docker, Graviton, Ampere) |
| `celery-5.7.0rc1+rs.0.1.0-…-x86_64.whl` | versione precedente: l'extra `[redis]` blocca `packaging==26.3` |

Nel `pyproject.toml`, al posto di `celery==…` (il marcatore sceglie la wheel giusta):

```toml
"celery[redis] @ https://github.com/TomP321/celery-rs/raw/wheels/celery-5.7.0rc1%2Brs.0.1.1-cp310-abi3-manylinux_2_28_x86_64.whl ; platform_machine == 'x86_64'",
"celery[redis] @ https://github.com/TomP321/celery-rs/raw/wheels/celery-5.7.0rc1%2Brs.0.1.1-cp310-abi3-manylinux_2_28_aarch64.whl ; platform_machine == 'aarch64'",
```
