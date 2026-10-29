# Wheel selector

`celery_rs_selector.py` is the PEP 517 backend for the sdist on the `wheels` branch
(`celery-<version>.tar.gz`). When pip/uv installs it, the backend selects the prebuilt
x86_64/aarch64 wheel, downloads it from `wheels`, verifies SHA-256 and returns it as the build result.

For a release, update `VERSION` and `WHEELS` (names and SHA-256 hashes). Create the sdist with
`pyproject.toml` (`build-backend = "celery_rs_selector"`, `backend-path = ["."]`), the backend,
`METADATA`/`PKG-INFO` from a wheel, and `LICENSE`.
