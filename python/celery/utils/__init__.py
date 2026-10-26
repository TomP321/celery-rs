"""Utility functions.

Don't import from here directly anymore, as these are only
here for backwards compatibility.
"""
from typing import TYPE_CHECKING

from .functional import chunks, noop
from .imports import gen_task_name, import_from_cwd, instantiate
from .imports import qualname as get_full_cls_name
# ------------------------------------------------------------------------ #
# > XXX Compat
from .nodenames import nodename, nodesplit, worker_direct

__all__ = (
    'LOG_LEVELS',
    'cached_property',
    'chunks',
    'gen_task_name',
    'gen_unique_id',
    'get_cls_by_name',
    'get_full_cls_name',
    'import_from_cwd',
    'instantiate',
    'memoize',
    'nodename',
    'nodesplit',
    'noop',
    'uuid',
    'worker_direct'
)

#: Nomi che il riferimento riesporta da kombu: restano gli stessi oggetti,
#: risolti al primo accesso (importare `kombu.utils` carica py-amqp).
_LAZY_KOMBU = {
    'LOG_LEVELS': ('kombu.log', 'LOG_LEVELS'),
    'cached_property': ('kombu.utils.objects', 'cached_property'),
    'get_cls_by_name': ('kombu.utils.imports', 'symbol_by_name'),
    'gen_unique_id': ('kombu.utils.uuid', 'uuid'),
    'memoize': ('kombu.utils.functional', 'memoize'),
    'uuid': ('kombu.utils.uuid', 'uuid'),
}


if TYPE_CHECKING:  # nomi risolti da __getattr__, per pyflakes
    from kombu.log import LOG_LEVELS  # noqa: F401
    from kombu.utils.functional import memoize  # noqa: F401
    from kombu.utils.imports import symbol_by_name as get_cls_by_name  # noqa: F401
    from kombu.utils.objects import cached_property  # noqa: F401
    from kombu.utils.uuid import uuid  # noqa: F401
    gen_unique_id = uuid


def __getattr__(name):
    try:
        module, attr = _LAZY_KOMBU[name]
    except KeyError:
        raise AttributeError(
            f'module {__name__!r} has no attribute {name!r}') from None
    from importlib import import_module
    value = getattr(import_module(module), attr)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(_LAZY_KOMBU))
