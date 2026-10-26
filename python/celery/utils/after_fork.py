"""``register_after_fork`` senza importare ``kombu.utils.compat``.

`kombu.utils.compat` importa ``kombu.exceptions`` (e quindi py-amqp); il
worker master usa solo la registrazione dei callback di fork.
"""
from __future__ import annotations

import os

__all__ = ('register_after_fork',)

_after_forkers = []
_registered = False


def _run_after_forkers():
    for obj, func in list(_after_forkers):
        func(obj)


def register_after_fork(obj, func):
    """Registra ``func(obj)`` nel figlio di ``fork`` (come kombu)."""
    global _registered
    _after_forkers.append((obj, func))
    if not _registered and hasattr(os, 'register_at_fork'):
        os.register_at_fork(after_in_child=_run_after_forkers)
        _registered = True
