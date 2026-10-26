"""Proxy di classi kombu che non importa la libreria al caricamento.

Il worker master deve poter usare ``app.amqp.Consumer``/``Producer``/
``Connection`` senza pagare ``kombu.connection`` (e con esso py-amqp) quando
tutto l'I/O passa dal core Rust. Il proxy è una vera classe (per
``inspect``/``isinstance``) e risolve la classe reale solo quando viene
chiamata o usata come base.
"""
from __future__ import annotations

import importlib

__all__ = ('lazy_class',)


class _LazyMeta(type):
    def __call__(cls, *args, **kwargs):
        return cls._resolve()(*args, **kwargs)

    def __getattr__(cls, name):
        return getattr(cls._resolve(), name)

    def __instancecheck__(cls, instance):
        return isinstance(instance, cls._resolve())

    def __subclasscheck__(cls, subclass):
        return issubclass(subclass, cls._resolve())


class _LazyKombuClass(metaclass=_LazyMeta):
    @classmethod
    def _resolve(cls):
        raise NotImplementedError


def _resolver(module, name):
    def resolve(cls):
        return getattr(importlib.import_module(module), name)
    return classmethod(resolve)


def lazy_class(module, name):
    """Una classe proxy con l'origin (modulo/nome) della classe reale."""
    return _LazyMeta(name, (_LazyKombuClass,), {
        '__module__': module,
        '__qualname__': name,
        '_resolve': _resolver(module, name),
    })
