"""Rust shell tests using the installed venv package instead of the reference source."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REFERENCE = (ROOT / 'celery').resolve()

sys.path[:] = [entry for entry in sys.path if Path(entry or '.').resolve() != REFERENCE]

# A plugin importing the reference first would invalidate these tests.
for _name in [name for name in sys.modules if name == 'celery' or name.startswith('celery.')]:
    del sys.modules[_name]
