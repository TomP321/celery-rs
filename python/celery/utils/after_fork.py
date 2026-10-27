"""``register_after_fork`` senza importare ``kombu.utils.compat``.

`kombu.utils.compat` importa ``kombu.exceptions`` (e quindi py-amqp); il
worker master usa solo la registrazione dei callback di fork.
"""
from __future__ import annotations

import os
import weakref
from itertools import count

__all__ = ('register_after_fork',)

# Come multiprocessing: il registro non deve tenere vive app e connessioni.
_after_forkers = weakref.WeakValueDictionary()
_counter = count()
_registered = False


def _run_after_forkers():
    for (_, _, func), obj in list(_after_forkers.items()):
        func(obj)


def register_after_fork(obj, func):
    """Registra ``func(obj)`` nel figlio di ``fork`` (come kombu)."""
    global _registered
    _after_forkers[(next(_counter), id(obj), func)] = obj
    if not _registered and hasattr(os, 'register_at_fork'):
        os.register_at_fork(after_in_child=_run_after_forkers)
        _registered = True
