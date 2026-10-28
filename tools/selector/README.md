# Selettore delle wheel

`celery_rs_selector.py` è il backend PEP 517 del pacchetto sorgente pubblicato nel branch `wheels`
(`celery-<versione>.tar.gz`): quando pip/uv lo installano sceglie la wheel precompilata per
l'architettura (x86_64/aarch64), la scarica dal branch `wheels`, ne verifica lo SHA-256 e la
consegna come wheel costruita. Per una nuova versione: aggiornare `VERSION` e `WHEELS` (nomi e
SHA-256 delle wheel), poi creare `celery-<versione>.tar.gz` con `pyproject.toml` (backend
`celery_rs_selector`, `backend-path = ["."]`), questo file, `METADATA`/`PKG-INFO` presi da una
delle wheel e `LICENSE`.
