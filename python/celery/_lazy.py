"""Nomi di modulo risolti al primo accesso (PEP 562).

Serve alla dieta degli import (spec 08b): i nomi che il riferimento riesporta
da kombu/click/billiard restano gli **stessi oggetti** del riferimento, ma la
libreria non viene caricata finché qualcuno non li usa davvero.
"""
from __future__ import annotations

import importlib

__all__ = ('install', 'lazy_class_attr', 'lazy_module')


def install(namespace, exports):
    """Installa ``__getattr__``/``__dir__`` nel namespace di un modulo.

    ``exports`` mappa nome -> ``(modulo, attributo)`` per i re-export, oppure
    nome -> callable senza argomenti che costruisce il valore (per classi che
    esistono solo se richieste). I valori risolti restano nel namespace, così
    l'accesso successivo non ripete l'import.
    """
    module_name = namespace.get('__name__', '?')

    def __getattr__(name):
        try:
            spec = exports[name]
        except KeyError:
            raise AttributeError(
                f'module {module_name!r} has no attribute {name!r}') from None
        if isinstance(spec, tuple):
            value = getattr(importlib.import_module(spec[0]), spec[1])
        else:
            value = spec()
        namespace[name] = value
        return value

    def __dir__():
        return sorted(set(namespace) | set(exports))

    namespace['__getattr__'] = __getattr__
    namespace['__dir__'] = __dir__


def lazy_class_attr(module, name):
    """Classe segnaposto risolta al primo accesso, usabile come attributo di classe.

    Serve per attributi come ``Task.OperationalError``: il dump di ``compat api``
    vede una classe con l'origin giusto (``inspect.getattr_static``), mentre
    l'accesso vero restituisce la classe di kombu senza importarla al caricamento
    del modulo. Un ``except Task.OperationalError`` funziona: ``__get__``
    restituisce la classe reale.
    """
    def _resolve():
        return getattr(importlib.import_module(module), name)

    class _LazyClassAttrMeta(type):
        def __get__(cls, instance, owner=None):
            return _resolve()

        def __getattr__(cls, item):
            return getattr(_resolve(), item)

        def __call__(cls, *args, **kwargs):
            return _resolve()(*args, **kwargs)

        def __instancecheck__(cls, instance):
            return isinstance(instance, _resolve())

        def __subclasscheck__(cls, subclass):
            return issubclass(subclass, _resolve())

    return _LazyClassAttrMeta(name, (), {
        '__module__': module,
        '__qualname__': name,
        '_resolve': staticmethod(_resolve),
    })


def lazy_module(name):
    """Un vero ``module`` che importa *name* al primo attributo usato.

    ``type(module).__name__`` resta ``'module'`` (il dump di ``compat api``
    confronta il tipo degli attributi di classe, es. ``Persistent.storage``),
    ma il modulo reale (es. ``shelve``) non viene caricato finché serve.
    """
    import sys
    import types
    module = types.ModuleType(name, f'Modulo {name!r} risolto al primo uso.')

    def __getattr__(attr):
        return getattr(importlib.import_module(name), attr)

    module.__dict__['__getattr__'] = __getattr__
    sys.modules.setdefault(name, module) if False else None
    return module
