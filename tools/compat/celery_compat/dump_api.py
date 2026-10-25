"""Dump della superficie pubblica di `celery` nel venv in cui viene eseguito.

Uso: <venv>/bin/python -I dump_api.py --modules modules.json --out api.json

Gira dentro il venv da analizzare (riferimento o celery-rs), quindi usa solo la stdlib.
L'output è deterministico: stesso pacchetto → stesso JSON, così il diff è significativo.
"""
import argparse
import enum
import importlib
import inspect
import json
import re
import sys
import types

ADDR_RE = re.compile(r' at 0x[0-9a-fA-F]+')
SIMPLE = (str, bytes, int, float, bool, type(None))
MAX_REPR = 400


def norm(text):
    text = ADDR_RE.sub('', text)
    return text if len(text) <= MAX_REPR else text[:MAX_REPR] + '…'


def value_repr(value, depth=0):
    """repr stabile di valori semplici; None se il valore non è confrontabile."""
    if isinstance(value, enum.Enum):
        return f'{type(value).__qualname__}.{value.name}'
    if isinstance(value, SIMPLE):
        return norm(repr(value))
    if depth < 3 and isinstance(value, (tuple, list, frozenset, set)):
        items = [value_repr(v, depth + 1) for v in value]
        if None in items:
            return None
        if isinstance(value, (set, frozenset)):
            items.sort()
        return f'{type(value).__name__}[{", ".join(items)}]'
    if depth < 3 and isinstance(value, dict) and len(value) <= 64:
        items = []
        for k, v in value.items():
            kr, vr = value_repr(k, depth + 1), value_repr(v, depth + 1)
            if kr is None or vr is None:
                return None
            items.append(f'{kr}: {vr}')
        return '{' + ', '.join(sorted(items)) + '}'
    return None


def public_type(cls):
    """Primo tipo pubblico nella MRO: una sottoclasse privata (`_Nome`) del tipo del riferimento
    è un dettaglio di implementazione, non superficie pubblica."""
    for klass in getattr(cls, '__mro__', (cls,)):
        if not getattr(klass, '__qualname__', '_').split('.')[-1].startswith('_'):
            return klass
    return cls


def origin(obj):
    mod = getattr(obj, '__module__', None)
    qual = getattr(obj, '__qualname__', None) or getattr(obj, '__name__', None)
    return f'{mod}.{qual}' if mod and qual else None


def signature(obj):
    try:
        return norm(str(inspect.signature(obj)))
    except (TypeError, ValueError):
        return None


def describe_callable(obj):
    d = {'kind': 'function', 'sig': signature(obj)}
    if inspect.iscoroutinefunction(obj):
        d['async'] = True
    return d


def describe_class(cls):
    d = {
        'kind': 'class',
        'origin': origin(cls),
        'mro': [origin(c) for c in cls.__mro__[1:] if c is not object],
        'sig': signature(cls),
        'members': {},
    }
    for name in sorted(dir(cls)):
        if name.startswith('_') and name not in ('__call__', '__init__', '__enter__', '__exit__',
                                                  '__iter__', '__len__', '__getitem__', '__eq__',
                                                  '__hash__', '__reduce__', '__or__', '__and__'):
            continue
        try:
            raw = inspect.getattr_static(cls, name)
        except AttributeError:
            continue
        d['members'][name] = describe_member(cls, name, raw)
    return d


def describe_member(cls, name, raw):
    if isinstance(raw, staticmethod):
        return {'kind': 'staticmethod', 'sig': signature(raw.__func__)}
    if isinstance(raw, classmethod):
        return {'kind': 'classmethod', 'sig': signature(raw.__func__)}
    if isinstance(raw, property):
        return {'kind': 'property', 'settable': raw.fset is not None}
    if inspect.isfunction(raw) or inspect.ismethoddescriptor(raw) or inspect.isbuiltin(raw):
        return {'kind': 'method', 'sig': signature(raw)}
    if inspect.isclass(raw):
        return {'kind': 'nested-class', 'origin': origin(raw)}
    if hasattr(raw, '__get__') and not isinstance(raw, SIMPLE):
        # descrittori custom (cached_property, Proxy, ...)
        return {'kind': 'descriptor', 'type': origin(type(raw))}
    return {'kind': 'attr', 'type': type(raw).__name__, 'value': value_repr(raw)}


def describe(obj):
    if isinstance(obj, types.ModuleType):
        return {'kind': 'module', 'name': obj.__name__}
    if inspect.isclass(obj):
        return describe_class(obj)
    if inspect.isfunction(obj) or inspect.isbuiltin(obj):
        d = describe_callable(obj)
        d['origin'] = origin(obj)
        return d
    if callable(obj) and not isinstance(obj, SIMPLE):
        return {'kind': 'callable-instance', 'type': origin(public_type(type(obj))),
                'sig': signature(obj)}
    return {'kind': 'value', 'type': type(obj).__name__, 'value': value_repr(obj)}


def public_names(mod):
    names = getattr(mod, '__all__', None)
    if names is None:
        names = [n for n in vars(mod) if not n.startswith('_')]
        # nomi risolti al primo accesso (PEP 562: `__getattr__` + `__dir__` di modulo)
        if '__getattr__' in vars(mod) and '__dir__' in vars(mod):
            names += [n for n in mod.__dir__() if not n.startswith('_')]
    return sorted(set(names))


def dump_module(modname):
    try:
        mod = importlib.import_module(modname)
    except Exception as exc:  # noqa: BLE001 — ogni errore di import è un dato del report
        return {'import_error': f'{type(exc).__name__}: {exc}'}
    out = {'all': getattr(mod, '__all__', None) is not None, 'names': {}}
    for name in public_names(mod):
        if name.startswith('__'):
            continue  # __file__, __path__, ...: dipendono da dove è installato
        try:
            obj = getattr(mod, name)
        except Exception as exc:  # noqa: BLE001
            out['names'][name] = {'kind': 'getattr-error', 'error': f'{type(exc).__name__}: {exc}'}
            continue
        # nomi importati da librerie esterne (os, kombu, ...) non fanno parte della nostra API
        # se il modulo non li dichiara in __all__
        org = getattr(obj, '__module__', None)
        if not (inspect.isclass(obj) or inspect.isroutine(obj) or isinstance(obj, types.ModuleType)):
            org = getattr(public_type(type(obj)), '__module__', org)
        if not out['all'] and isinstance(org, str) and not org.startswith('celery') \
                and not isinstance(obj, types.ModuleType):
            continue
        if not out['all'] and isinstance(obj, types.ModuleType):
            continue
        out['names'][name] = describe(obj)
    return out


def type_name(t):
    return t.__name__ if isinstance(t, type) else t


def dump_settings():
    from celery.app.defaults import NAMESPACES, flatten
    out = {}
    for key, opt in flatten(NAMESPACES):
        out[key] = {
            'type': type_name(getattr(opt, 'type', None)),
            'default': value_repr(opt.default) if opt.default is not None else 'None',
            'old': sorted(getattr(opt, 'old', ()) or ()),
        }
    return out


def dump_signals():
    import celery.signals as s
    from celery.utils.dispatch import Signal
    out = {}
    for name in dir(s):
        obj = getattr(s, name)
        if isinstance(obj, Signal):
            out[name] = {'name': obj.name, 'providing_args': sorted(getattr(obj, 'providing_args', ()) or ())}
    return out


def dump_cli():
    import click
    from celery.bin.celery import celery as root

    def walk(cmd, path):
        res = {path: {
            'params': [{
                'name': p.name, 'opts': sorted(p.opts + p.secondary_opts),
                'type': getattr(p.type, 'name', type(p.type).__name__),
                'default': value_repr(p.default) if not callable(p.default) else '<callable>',
                'multiple': getattr(p, 'multiple', False),
                'is_flag': getattr(p, 'is_flag', False),
                'required': p.required,
            } for p in cmd.params],
        }}
        if isinstance(cmd, click.Group):
            for name, sub in sorted(cmd.commands.items()):
                res.update(walk(sub, f'{path} {name}'))
        return res
    return walk(root, 'celery')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--modules', required=True, help='JSON con la lista dei moduli da analizzare')
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    with open(args.modules) as fh:
        modules = json.load(fh)

    try:
        import celery
        meta = {'celery_file': celery.__file__, 'version': getattr(celery, '__version__', None)}
    except ImportError as exc:
        meta = {'import_error': str(exc)}
    result = {
        'meta': {**meta, 'python': sys.version.split()[0]},
        'modules': {m: dump_module(m) for m in modules},
    }
    for section, fn in (('settings', dump_settings), ('signals', dump_signals), ('cli', dump_cli)):
        try:
            result[section] = fn()
        except Exception as exc:  # noqa: BLE001
            result[section] = {'__error__': f'{type(exc).__name__}: {exc}'}
    with open(args.out, 'w') as fh:
        json.dump(result, fh, indent=1, sort_keys=True)


if __name__ == '__main__':
    main()
