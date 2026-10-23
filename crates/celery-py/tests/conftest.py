"""Importa il Celery di riferimento (sorgente in ``celery/``) e ``_celery_rs``."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
REFERENCE = str(ROOT / "celery")
if REFERENCE not in sys.path:
    # Evita che la directory ``celery/`` del repository (namespace package)
    # oscuri il pacchetto installato/editable.
    sys.path.insert(0, REFERENCE)
