"""Import reference Celery and the native extension side by side."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
REFERENCE = str(ROOT / "celery")
if REFERENCE not in sys.path:
    # Prevent the repository's namespace package from shadowing installed Celery.

    sys.path.insert(0, REFERENCE)
