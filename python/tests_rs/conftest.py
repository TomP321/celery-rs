"""Test del producer Rust (``celery.app._rs_producer``), eseguiti con ``.venv-rs``.

Il ``pytest.ini`` del repository mette il *sorgente di riferimento* in
``pythonpath`` per i test differenziali di ``crates/celery-py``: qui invece
serve il pacchetto installato nel venv, quindi la directory ``celery/`` del
repository viene tolta da ``sys.path`` prima che i test importino ``celery``.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REFERENCE = (ROOT / 'celery').resolve()

sys.path[:] = [entry for entry in sys.path if Path(entry or '.').resolve() != REFERENCE]

# Se un plugin avesse già importato il riferimento, il test non sarebbe valido.
for _name in [name for name in sys.modules if name == 'celery' or name.startswith('celery.')]:
    del sys.modules[_name]
