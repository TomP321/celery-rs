# celery-rs — prebuilt wheels

Ready-to-install celery-rs wheels (no Rust toolchain needed). Linux, glibc ≥ 2.28
(Debian ≥ 10, Ubuntu ≥ 20.04, `python:*-slim`/`-bookworm` images), Python ≥ 3.10.

| File | Architecture |
|---|---|
| `celery-5.7.0rc1+rs.0.1.3-cp310-abi3-manylinux_2_28_x86_64.whl` | x86_64 (Intel/AMD) |
| `celery-5.7.0rc1+rs.0.1.3-cp310-abi3-manylinux_2_28_aarch64.whl` | aarch64 (ARM: Apple Silicon in Docker, Graviton, Ampere) |
| `celery-5.7.0rc1+rs.0.1.0-…-x86_64.whl` | older release: its `[redis]` extra pins `packaging==26.3` |

**One line, architecture picked automatically** (pip and uv): the source package
`celery-5.7.0rc1+rs.0.1.3.tar.gz` only contains a selector that downloads the right wheel for the
machine and verifies its SHA-256.

```toml
"celery[redis] @ https://github.com/TomP321/celery-rs/raw/wheels/celery-5.7.0rc1%2Brs.0.1.3.tar.gz",
```

Alternatively, the wheels directly, with a platform marker:

```toml
"celery[redis] @ https://github.com/TomP321/celery-rs/raw/wheels/celery-5.7.0rc1%2Brs.0.1.3-cp310-abi3-manylinux_2_28_x86_64.whl ; platform_machine == 'x86_64'",
"celery[redis] @ https://github.com/TomP321/celery-rs/raw/wheels/celery-5.7.0rc1%2Brs.0.1.3-cp310-abi3-manylinux_2_28_aarch64.whl ; platform_machine == 'aarch64'",
```
