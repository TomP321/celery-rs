"""Copie locali delle utilità di kombu usate internamente.

La dieta degli import (spec 08b) non può caricare ``kombu.utils`` nel percorso
client: il pacchetto ``kombu.utils`` importa ``kombu.exceptions`` (e quindi
py-amqp), ``billiard`` (e quindi cffi/multiprocessing) e ``importlib.metadata``.
Qui ci sono copie fedeli delle poche utilità che servono davvero, con la stessa
semantica di kombu 5.7; il codice interno le usa al posto delle originali.

I nomi che il riferimento riesporta come API pubblica (``celery.utils.memoize``,
``celery.utils.functional.lazy``, ...) restano invece risolti pigramente verso
kombu (stessi oggetti: ``celery.utils.X is kombu.utils.X``); per le classi qui
sotto che compaiono come attributi di classi pubbliche (``cached_property``)
``__module__`` è quello di kombu, così la superficie descritta da
``compat api`` resta identica.
"""
from __future__ import annotations

import threading
import traceback
from collections import OrderedDict, UserDict
from collections.abc import Iterable, Mapping
from functools import wraps
from itertools import count
from time import sleep, time

__all__ = (
    'LRUCache', 'cached_property', 'bytes_to_str', 'dictfilter', 'fxrange',
    'fxrangemax', 'is_list', 'lazy', 'maybe_evaluate', 'maybe_fileno',
    'maybe_list', 'memoize', 'reprcall', 'reprkwargs', 'retry_over_time',
    'safe_repr', 'safe_str', 'str_to_bytes',
)

KEYWORD_MARK = object()


class cached_property:
    """``cached_property`` di kombu (stesso setter/deleter opzionali)."""

    def __init__(self, fget=None, fset=None, fdel=None):
        self.func = fget
        self.attrname = getattr(fget, '__name__', None)
        self.__set = fset
        self.__del = fdel
        self.lock = threading.RLock()

    def __set_name__(self, owner, name):
        if self.attrname is None:
            self.attrname = name
        elif name != self.attrname:
            raise TypeError(
                'Cannot assign the same cached_property to two different names '
                f'({self.attrname!r} and {name!r}).'
            )

    def __get__(self, instance, owner=None):
        if instance is None:
            return self
        with self.lock:
            try:
                return instance.__dict__[self.attrname]
            except KeyError:
                pass
        value = self.func(instance)
        instance.__dict__[self.attrname] = value
        return value

    def __set__(self, instance, value):
        if instance is None:
            return self
        with self.lock:
            if self.__set is not None:
                value = self.__set(instance, value)
            instance.__dict__[self.attrname] = value

    def __delete__(self, instance):
        if instance is None:
            return self
        with self.lock:
            value = instance.__dict__.pop(self.attrname, None)
            if self.__del and value is not None:
                self.__del(instance, value)

    def setter(self, fset):
        return self.__class__(self.func, fset, self.__del)

    def deleter(self, fdel):
        return self.__class__(self.func, self.__set, fdel)


#: L'API pubblica è la classe di kombu: stesso origin nel dump di ``compat api``.
cached_property.__module__ = 'kombu.utils.objects'
cached_property.__qualname__ = 'cached_property'


class LRUCache(UserDict):
    """LRU cache limitata, come ``kombu.utils.functional.LRUCache``."""

    def __init__(self, limit=None):
        self.limit = limit
        self.mutex = threading.RLock()
        self.data = OrderedDict()

    def __getitem__(self, key):
        with self.mutex:
            value = self[key] = self.data.pop(key)
            return value

    def update(self, *args, **kwargs):
        with self.mutex:
            data, limit = self.data, self.limit
            data.update(*args, **kwargs)
            if limit and len(data) > limit:
                for _ in range(len(data) - limit):
                    data.popitem(last=False)

    def popitem(self, last=True):
        with self.mutex:
            return self.data.popitem(last)

    def __setitem__(self, key, value):
        with self.mutex:
            if (key not in self.data and
                    self.limit and len(self.data) >= self.limit):
                self.data.pop(next(iter(self.data)))
            self.data[key] = value

    def __iter__(self):
        return iter(self.data)

    def _iterate_items(self):
        with self.mutex:
            for k in self:
                try:
                    yield (k, self.data[k])
                except KeyError:  # pragma: no cover
                    pass
    iteritems = _iterate_items

    def _iterate_values(self):
        with self.mutex:
            for k in self:
                try:
                    yield self.data[k]
                except KeyError:  # pragma: no cover
                    pass
    itervalues = _iterate_values

    def _iterate_keys(self):
        with self.mutex:
            return self.data.keys()
    iterkeys = _iterate_keys

    def incr(self, key, delta=1):
        with self.mutex:
            newval = int(self.data.pop(key)) + delta
            self[key] = str(newval)
            return newval

    def __getstate__(self):
        d = dict(vars(self))
        d.pop('mutex')
        return d

    def __setstate__(self, state):
        self.__dict__ = state
        self.mutex = threading.RLock()

    keys = _iterate_keys
    values = _iterate_values
    items = _iterate_items


def memoize(maxsize=None, keyfun=None, Cache=LRUCache):
    """Decorator to cache function return value."""
    def _memoize(fun):
        mutex = threading.Lock()
        cache = Cache(limit=maxsize)

        @wraps(fun)
        def _M(*args, **kwargs):
            if keyfun:
                key = keyfun(args, kwargs)
            else:
                key = args + (KEYWORD_MARK,) + tuple(sorted(kwargs.items()))
            try:
                with mutex:
                    value = cache[key]
            except KeyError:
                value = fun(*args, **kwargs)
                _M.misses += 1
                with mutex:
                    cache[key] = value
            else:
                _M.hits += 1
            return value

        def clear():
            cache.clear()
            _M.hits = _M.misses = 0

        _M.hits = _M.misses = 0
        _M.clear = clear
        _M.original_func = fun
        return _M

    return _memoize


class lazy:
    """Holds lazy evaluation (come ``kombu.utils.functional.lazy``)."""

    def __init__(self, fun, *args, **kwargs):
        self._fun = fun
        self._args = args
        self._kwargs = kwargs

    def __call__(self):
        return self.evaluate()

    def evaluate(self):
        return self._fun(*self._args, **self._kwargs)

    def __str__(self):
        return str(self())

    def __repr__(self):
        return repr(self())

    def __eq__(self, rhs):
        return self() == rhs

    def __ne__(self, rhs):
        return self() != rhs

    def __deepcopy__(self, memo):
        memo[id(self)] = self
        return self

    def __reduce__(self):
        return (self.__class__, (self._fun,), {'_args': self._args,
                                               '_kwargs': self._kwargs})


#: ``celery.utils.functional.mlazy`` estende la classe di kombu: stesso origin.
lazy.__module__ = 'kombu.utils.functional'
lazy.__qualname__ = 'lazy'


def maybe_evaluate(value):
    """Evaluate value only if value is a lazy instance (locale o di kombu)."""
    if isinstance(value, lazy):
        return value.evaluate()
    import sys
    module = sys.modules.get('kombu.utils.functional')
    if module is not None and isinstance(value, module.lazy):
        return value.evaluate()
    return value


def is_list(obj, scalars=(Mapping, str), iters=(Iterable,)):
    """Return true if the object is iterable, but not a mapping/string."""
    return isinstance(obj, iters) and not isinstance(obj, scalars or ())


def maybe_list(obj, scalars=(Mapping, str)):
    """Return list of one element if ``l`` is a scalar."""
    return obj if obj is None or is_list(obj, scalars) else [obj]


def dictfilter(d=None, **kw):
    """Remove all keys from dict ``d`` whose value is :const:`None`."""
    d = kw if d is None else (dict(d, **kw) if kw else d)
    return {k: v for k, v in d.items() if v is not None}


def fxrange(start=1.0, stop=None, step=1.0, repeatlast=False):
    cur = start * 1.0
    while 1:
        if not stop or cur <= stop:
            yield cur
            cur += step
        else:
            if not repeatlast:
                break
            yield cur - step


def fxrangemax(start=1.0, stop=None, step=1.0, max=100.0):
    sum_, cur = 0, start * 1.0
    while 1:
        if sum_ >= max:
            break
        yield cur
        if stop:
            cur = min(cur + step, stop)
        else:
            cur += step
        sum_ += cur


class LazyErrors:
    """Tupla di eccezioni risolta solo quando un errore viene davvero alzato."""

    __slots__ = ('_factory',)

    def __init__(self, factory):
        self._factory = factory

    def resolve(self):
        return self._factory()

    def __eq__(self, other):
        try:
            return self.resolve() == other
        except Exception:  # pragma: no cover - risoluzione impossibile
            return NotImplemented

    def __ne__(self, other):
        result = self.__eq__(other)
        return result if result is NotImplemented else not result


def _caught(exc, catch):
    if isinstance(catch, LazyErrors):
        catch = catch.resolve()
    return isinstance(exc, catch)


def retry_over_time(fun, catch, args=None, kwargs=None, errback=None,
                    max_retries=None, interval_start=2, interval_step=2,
                    interval_max=30, callback=None, timeout=None):
    """Retry the function over and over until max retries is exceeded."""
    kwargs = {} if not kwargs else kwargs
    args = [] if not args else args
    interval_range = (
        min(value, float(interval_max))
        for value in fxrange(
            interval_start,
            interval_max + interval_start,
            interval_step,
            repeatlast=True,
        )
    )
    end = time() + timeout if timeout else None
    for retries in count():
        try:
            return fun(*args, **kwargs)
        except BaseException as exc:
            if not _caught(exc, catch):
                raise
            if max_retries is not None and retries >= max_retries:
                raise
            if end and time() > end:
                raise
            if callback:
                callback()
            tts = float(errback(exc, interval_range, retries) if errback
                        else next(interval_range))
            tts = min(tts, float(interval_max))
            if tts:
                for _ in range(int(tts)):
                    if callback:
                        callback()
                    sleep(1.0)
                sleep(abs(int(tts) - tts))


def str_to_bytes(s):
    """Convert str to bytes."""
    if isinstance(s, str):
        return s.encode()
    return s


def bytes_to_str(s):
    """Convert bytes to str."""
    if isinstance(s, bytes):
        return s.decode(errors='replace')
    return s


def safe_str(s, errors='replace'):
    """Safe form of str(), void of unicode errors."""
    s = bytes_to_str(s)
    if not isinstance(s, (str, bytes)):
        return safe_repr(s, errors)
    return _safe_str(s, errors)


def _safe_str(s, errors='replace', file=None):
    if isinstance(s, str):
        return s
    try:
        return str(s)
    except Exception as exc:
        return '<Unrepresentable {!r}: {!r} {!r}>'.format(
            type(s), exc, '\n'.join(traceback.format_stack()))


def safe_repr(o, errors='replace'):
    """Safe form of repr, void of Unicode errors."""
    try:
        return repr(o)
    except Exception:
        return _safe_str(o, errors)


def reprkwargs(kwargs, sep=', ', fmt='{0}={1}'):
    return sep.join(fmt.format(k, safe_repr(v)) for k, v in kwargs.items())


def reprcall(name, args=(), kwargs=None, sep=', '):
    kwargs = {} if not kwargs else kwargs
    return '{}({}{}{})'.format(
        name, sep.join(map(safe_repr, args or ())),
        (args and kwargs) and sep or '',
        reprkwargs(kwargs, sep),
    )


def maybe_fileno(f):
    """Get object fileno, or :const:`None` if not defined."""
    import numbers
    from io import UnsupportedOperation
    try:
        if isinstance(f, numbers.Integral):
            return f
        if f is None:
            raise ValueError('file descriptor is None')
        return f.fileno()
    except (AttributeError, ValueError, UnsupportedOperation):
        return None


class urlparts(tuple):
    """Named tuple dei componenti di un URL (come ``kombu.utils.url.urlparts``)."""

    __slots__ = ()

    def __new__(cls, scheme, hostname, port, username, password, path, query):
        return tuple.__new__(
            cls, (scheme, hostname, port, username, password, path, query))

    scheme = property(lambda self: self[0])
    hostname = property(lambda self: self[1])
    port = property(lambda self: self[2])
    username = property(lambda self: self[3])
    password = property(lambda self: self[4])
    path = property(lambda self: self[5])
    query = property(lambda self: self[6])


def url_to_parts(url):
    """Parse URL into :class:`urlparts` (copie locale di kombu)."""
    from urllib.parse import parse_qsl, unquote, urlparse
    scheme = urlparse(url).scheme
    schemeless = url[len(scheme) + 3:]
    parts = urlparse('http://' + schemeless)
    path = parts.path or ''
    path = path[1:] if path and path[0] == '/' else path
    return urlparts(
        scheme,
        unquote(parts.hostname or '') or None,
        parts.port,
        unquote(parts.username or '') or None,
        unquote(parts.password or '') or None,
        unquote(path or '') or None,
        dict(parse_qsl(parts.query)),
    )


_parse_url = url_to_parts


def maybe_sanitize_url(url, mask='**'):
    """Sanitize url, or do nothing if url undefined (copie locale).

    Come ``kombu.utils.url.as_url``, il path (anche vuoto) è sempre preceduto
    da ``/``.
    """
    if not (isinstance(url, str) and '://' in url):
        return url
    scheme, _sep, rest = url.partition('://')
    creds, at, hostpath = rest.rpartition('@')
    if not at:
        return url
    user, colon, _password = creds.partition(':')
    creds = f'{user}:{mask}' if colon else user
    hostport, _slash, path = hostpath.partition('/')
    return f'{scheme}://{creds}@{hostport}/{path}'


_environment = None


def detect_environment():
    """Detect the current environment (copie di ``kombu.utils.compat``)."""
    global _environment
    if _environment is None:
        _environment = _detect_environment()
    return _environment


def _detect_environment():
    import sys
    if 'eventlet' in sys.modules:
        try:
            import socket
            from eventlet.patcher import is_monkey_patched as is_eventlet
            if is_eventlet(socket):
                return 'eventlet'
        except ImportError:
            pass
    if 'gevent' in sys.modules:
        try:
            import socket
            from gevent import socket as _gsocket
            if socket.socket is _gsocket.socket:
                return 'gevent'
        except ImportError:
            pass
    return 'default'


class LamportClock:
    """Copie di ``kombu.clocks.LamportClock`` (nessun import di kombu)."""

    value = 0

    def __init__(self, initial_value=0, Lock=None):
        from threading import Lock as _Lock
        self.value = initial_value
        self.mutex = (Lock or _Lock)()

    def adjust(self, other):
        with self.mutex:
            value = self.value = max(self.value, other) + 1
            return value

    def forward(self):
        with self.mutex:
            self.value += 1
            return self.value

    def sort_heap(self, h):
        from itertools import islice
        if h[0][0] == h[1][0]:
            same = []
            for PN in zip(h, islice(h, 1, None)):
                if PN[0][0] != PN[1][0]:
                    break
                same.append(PN[0])
            return sorted(same, key=lambda event: event[1])[0]
        return h[0]

    def __repr__(self):
        return f'<LamportClock: {self.value}>'


def operational_error():
    """``kombu.exceptions.OperationalError`` risolta solo quando serve."""
    from kombu.exceptions import OperationalError
    return OperationalError


_ACCEPT_TYPES = {'json': 'application/json'}


def prepare_accept_content(content_types):
    """Copie di ``kombu.serialization.prepare_accept_content``.

    Il caso ``json`` (quello del core Rust) non importa il registro di kombu;
    per gli altri serializer decide la funzione originale.
    """
    if content_types is None:
        return None
    try:
        return {n if '/' in n else _ACCEPT_TYPES[n] for n in content_types}
    except KeyError:
        from kombu.serialization import prepare_accept_content as _prepare
        return _prepare(content_types)


def human_status(status):
    """Copie di ``billiard.common.human_status``."""
    if (status or 0) < 0:
        import signal
        signum = -status
        try:
            return 'signal {0} ({1})'.format(signum, signal.Signals(signum).name)
        except (ValueError, AttributeError):
            return f'signal {signum}'
    return f'exitcode {status}'


class RestartFreqExceeded(Exception):
    """Restarts too fast (billiard)."""


def restart_freq_exceeded():
    """La classe di billiard se è già importata, altrimenti la copia locale."""
    import sys
    module = sys.modules.get('billiard.exceptions')
    return module.RestartFreqExceeded if module is not None else RestartFreqExceeded


class restart_state:
    """Copie di ``billiard.common.restart_state``."""

    RestartFreqExceeded = RestartFreqExceeded

    def __init__(self, maxR, maxT):
        self.maxR, self.maxT = maxR, maxT
        self.R, self.T = 0, None

    def step(self, now=None):
        from time import monotonic
        now = monotonic() if now is None else now
        R = self.R
        if self.T and now - self.T >= self.maxT:
            self.T, self.R = now, 0
        elif self.maxR and self.R >= self.maxR:
            if self.R:
                self.R = 0
                raise self.RestartFreqExceeded(f'{R!r} in {self.maxT!r}s')
        if self.T is None:
            self.T = now
        self.R += 1


def to_timestamp(d, default_timezone=None, time=None):
    """Copie di ``kombu.asynchronous.timer.to_timestamp``.

    ``diff`` è lo scarto fra orologio di parete e monotonic: senza si
    otterrebbero timestamp spostati di ~1.7e9 (gli ETA non scadrebbero mai).
    """
    from datetime import datetime, timezone
    from time import monotonic, time as wall_time
    if isinstance(d, datetime):
        if default_timezone is None:
            default_timezone = timezone.utc
        if d.tzinfo is None:
            d = d.replace(tzinfo=default_timezone)
        diff = wall_time() - (time or monotonic)()
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        return max((d - epoch).total_seconds() - diff, 0)
    return d


def json_dumps(obj, **kwargs):
    """``kombu.utils.json.dumps`` risolto al primo uso."""
    from kombu.utils.json import dumps
    return dumps(obj, **kwargs)
