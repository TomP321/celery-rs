"""Fork callbacks without importing kombu and py-amqp."""
from __future__ import annotations

import os
import weakref
from itertools import count

__all__ = ('register_after_fork',)

# Like multiprocessing, the registry must not keep apps and connections alive.
_after_forkers = weakref.WeakValueDictionary()
_counter = count()
_registered = False


def _run_after_forkers():
    for (_, _, func), obj in list(_after_forkers.items()):
        func(obj)


def register_after_fork(obj, func):
    """Register ``func(obj)`` to run after fork in the child."""
    global _registered
    _after_forkers[(next(_counter), id(obj), func)] = obj
    if not _registered and hasattr(os, 'register_at_fork'):
        os.register_at_fork(after_in_child=_run_after_forkers)
        _registered = True
