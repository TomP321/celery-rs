"""Redis-py interface backed by Rust for supported backend connection options."""
from __future__ import annotations

import os
import threading
import weakref

try:  # The full package installs the extension; the reference may not.
    import _celery_rs
except ImportError:  # pragma: no cover - native extension unavailable
    _celery_rs = None

__all__ = (
    'RsRedis', 'RsPipeline', 'RsPubSub', 'RsConnectionPool',
    'RsPubSubConnectionPool', 'RsConnection', 'use_rs_backend', 'stats',
)

# Environment switch to restore redis-py.
ENV_DISABLE = 'CELERY_RS_BACKEND'


def _reset_on_broker_restart(app, broker):
    from urllib.parse import urlsplit

    backend = app._backend
    client = backend.__dict__.get('client') if backend is not None else None
    if type(client) is not RsRedis:
        return False
    url = urlsplit(broker)
    params = client._params
    if (url.scheme not in ('redis', 'rediss') or client._unix
            or (url.scheme == 'rediss') != client._ssl
            or url.hostname != (params.get('host') or 'localhost')
            or (url.port or 6379) != int(params.get('port') or 6379)):
        return False
    # Discard stale sockets before a command, as redis-py does, to avoid failing the first GET after restart.
    client.reset()
    return True


# Counters let tests verify that the backend uses Rust.
_STATS = {
    'clients': 0,
    'pipelines': 0,
    'pubsubs': 0,
    'commands': 0,
    'forks': 0,
    'instances': 0,
}


def stats():
    """Copy Rust client counters."""
    return dict(_STATS)


# Keep inherited instances alive in children so Drop cannot access the parent's absent tokio runtime.
_LIVE = weakref.WeakSet()
_ORPHANED = []


def _enabled():
    value = os.environ.get(ENV_DISABLE)
    if value is None:
        return True
    return value.strip().lower() not in ('0', 'false', 'no', 'off')


def _data_error(message):
    """Import ``redis.exceptions.DataError`` on demand."""
    try:
        from redis.exceptions import DataError
    except ImportError:  # pragma: no cover - redis-py backend dependency
        return ValueError(message)
    return DataError(message)


def _is_connection_error(exc):
    """Return whether an exception is a redis-py connection error or builtin equivalent."""
    try:
        from redis.exceptions import ConnectionError as RedisConnectionError
        from redis.exceptions import TimeoutError as RedisTimeoutError
    except ImportError:  # pragma: no cover
        return isinstance(exc, (ConnectionError, TimeoutError, OSError))
    return isinstance(
        exc, (RedisConnectionError, RedisTimeoutError, ConnectionError,
              TimeoutError, OSError))


def _encode(value):
    """Encode arguments as ``redis.Encoder`` does."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, bool):
        raise _data_error(
            "Invalid input of type: 'bool'. Convert to a "
            "bytes, string, int or float first."
        )
    if isinstance(value, (int, float)):
        return repr(value).encode()
    if isinstance(value, str):
        return value.encode('utf-8')
    raise _data_error(
        f"Invalid input of type: '{type(value).__name__}'. "
        "Convert to a bytes, string, int or float first."
    )


def _flatten(args):
    """Accept a sequence or separate arguments as redis-py does."""
    if len(args) == 1 and isinstance(args[0], (list, tuple, set, frozenset)):
        return list(args[0])
    return list(args)


# Unsupported parameters retain redis-py to preserve behavior.
_ALLOWED_PARAMS = frozenset({
    'host', 'port', 'db', 'password', 'username', 'max_connections',
    'socket_timeout', 'socket_connect_timeout', 'client_name',
    'health_check_interval', 'socket_keepalive', 'path', 'connection_class',
    'ssl_cert_reqs', 'driver_info', 'lib_name', 'lib_version',
    'decode_responses', 'retry_on_timeout',
})

_REQUIRED_CERT_REQS = frozenset({
    'CERT_REQUIRED', 'required', '2', 2,
})


def _is_ssl_connection(connection_class):
    # Check the real or lazy module by name without importing redis-py.
    return (getattr(connection_class, '__name__', '') == 'SSLConnection' and
            str(getattr(connection_class, '__module__', '')).startswith('redis'))


def _is_unix_connection(connection_class):
    return (getattr(connection_class, '__name__', '') ==
            'UnixDomainSocketConnection' and
            str(getattr(connection_class, '__module__', '')).startswith('redis'))


class RsRedis:
    """Provide the ``redis.Redis`` interface used by the result backend."""

    def __init__(self, **params):
        self._params = dict(params)
        self.decode_responses = bool(params.get('decode_responses', False))
        connection_class = params.get('connection_class')
        self._unix = _is_unix_connection(connection_class) or 'path' in params
        self._ssl = _is_ssl_connection(connection_class)
        native = _celery_rs.RedisClient(
            host=params.get('host') or 'localhost',
            port=int(params.get('port') or 6379),
            db=int(params.get('db') or 0),
            username=params.get('username'),
            password=params.get('password'),
            socket_timeout=params.get('socket_timeout'),
            socket_connect_timeout=params.get('socket_connect_timeout'),
            client_name=params.get('client_name'),
            max_connections=params.get('max_connections'),
            path=params.get('path') if self._unix else None,
            ssl=self._ssl,
            ssl_cert_reqs=(
                str(params.get('ssl_cert_reqs'))
                if params.get('ssl_cert_reqs') is not None else None
            ),
            health_check_interval=params.get('health_check_interval'),
        )
        self._rs = native
        self.connection_pool = RsConnectionPool(self)
        _LIVE.add(self)
        _STATS['clients'] += 1
        _STATS['instances'] += 1

    @classmethod
    def supports(cls, params):
        """Return whether Rust supports all connection parameters."""
        if _celery_rs is None:
            return False
        unknown = set(params) - _ALLOWED_PARAMS
        if unknown:
            return False
        if params.get('decode_responses'):
            return False
        if params.get('retry_on_timeout'):
            return False
        if params.get('credential_provider'):
            return False
        for key in ('ssl_ca_certs', 'ssl_certfile', 'ssl_keyfile'):
            if params.get(key):
                return False
        for key in ('password', 'username'):
            if callable(params.get(key)):
                return False
        connection_class = params.get('connection_class')
        if connection_class is not None and not (
            _is_ssl_connection(connection_class)
            or _is_unix_connection(connection_class)
        ):
            return False
        if _is_ssl_connection(connection_class):
            if params.get('ssl_cert_reqs') not in _REQUIRED_CERT_REQS:
                return False
        # redis-rs manages multiplexed socket reconnection; keepalive and health checks do not alter wire commands.
        if 'path' in params and not params.get('path'):
            return False
        for key in ('host', 'path', 'client_name'):
            if params.get(key) is not None and not isinstance(params[key], str):
                return False
        for key in ('port', 'db', 'max_connections'):
            value = params.get(key)
            if value is not None and not isinstance(value, int):
                return False
        for key in ('socket_timeout', 'socket_connect_timeout',
                    'health_check_interval'):
            value = params.get(key)
            if value is not None and not isinstance(value, (int, float)):
                return False
        return True


    def get(self, name):
        _STATS['commands'] += 1
        return self._rs.get(_encode(name))

    def mget(self, keys, *args):
        keys = list(keys)
        if args:
            keys.extend(args)
        _STATS['commands'] += 1
        return self._rs.mget([_encode(key) for key in keys])

    def set(self, name, value, ex=None, px=None, nx=False, xx=False,
            keepttl=False, get=False, exat=None, pxat=None):
        if px is not None or nx or xx or keepttl or get or exat is not None \
                or pxat is not None:
            raise NotImplementedError(
                'SET option unsupported by Rust Redis client')
        _STATS['commands'] += 1
        return self._rs.set(_encode(name), _encode(value),
                            ex=None if ex is None else int(ex))

    def setex(self, name, time, value):
        _STATS['commands'] += 1
        return self._rs.setex(_encode(name), int(time), _encode(value))

    def delete(self, *names):
        _STATS['commands'] += 1
        return self._rs.delete([_encode(name) for name in _flatten(names)])

    def incr(self, name, amount=1):
        _STATS['commands'] += 1
        return self._rs.incr(_encode(name), None if amount == 1 else int(amount))

    def expire(self, name, time):
        _STATS['commands'] += 1
        return self._rs.expire(_encode(name), int(time))

    def ttl(self, name):
        _STATS['commands'] += 1
        return self._rs.ttl(_encode(name))

    def publish(self, channel, message):
        _STATS['commands'] += 1
        return self._rs.publish(_encode(channel), _encode(message))

    def zadd(self, name, mapping, nx=False, xx=False, ch=False, incr=False,
             gt=False, lt=False):
        if nx or xx or ch or incr or gt or lt:
            raise NotImplementedError(
                'ZADD options unsupported by Rust Redis client')
        pairs = [(_encode(score), _encode(member))
                 for member, score in mapping.items()]
        _STATS['commands'] += 1
        return self._rs.zadd(_encode(name), pairs)

    def zcount(self, name, min, max):
        _STATS['commands'] += 1
        return self._rs.zcount(_encode(name), _encode(min), _encode(max))

    def zrange(self, name, start, end, desc=False, withscores=False,
               score_cast_func=float, byscore=False, bylex=False,
               offset=None, num=None):
        if desc or withscores or byscore or bylex or offset is not None \
                or num is not None:
            raise NotImplementedError(
                'ZRANGE options unsupported by Rust Redis client')
        _STATS['commands'] += 1
        return self._rs.zrange(_encode(name), int(start), int(end))

    def rpush(self, name, *values):
        _STATS['commands'] += 1
        return self._rs.rpush(_encode(name), [_encode(v) for v in _flatten(values)])

    def llen(self, name):
        _STATS['commands'] += 1
        return self._rs.llen(_encode(name))

    def lrange(self, name, start, end):
        _STATS['commands'] += 1
        return self._rs.lrange(_encode(name), int(start), int(end))


    def pipeline(self, transaction=True, shard_hint=None):
        _STATS['pipelines'] += 1
        return RsPipeline(self, transaction)

    def pubsub(self, ignore_subscribe_messages=False, **kwargs):
        _STATS['pubsubs'] += 1
        return RsPubSub(self._rs.pubsub(), ignore_subscribe_messages)

    def reset(self):
        self._rs.reset()

    def close(self):
        self.reset()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def __repr__(self):
        return f'<RsRedis {self._params.get("host")}:{self._params.get("port")}>'


class RsPipeline:
    """Redis-py pipeline with chainable commands and MULTI/EXEC by default."""

    def __init__(self, client, transaction=True):
        self._client = client
        self._native = client._rs.pipeline(bool(transaction))
        self.transaction = bool(transaction)
        _LIVE.add(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def reset(self):
        self._native.reset()

    def _push(self, method, *args):
        getattr(self._native, method)(*args)
        return self

    def get(self, name):
        return self._push('get', _encode(name))

    def mget(self, keys, *args):
        keys = list(keys)
        if args:
            keys.extend(args)
        return self._push('mget', [_encode(key) for key in keys])

    def set(self, name, value, ex=None, px=None, nx=False, xx=False,
            keepttl=False, get=False, exat=None, pxat=None):
        if px is not None or nx or xx or keepttl or get or exat is not None \
                or pxat is not None:
            raise NotImplementedError(
                'SET option unsupported by Rust Redis client')
        return self._push('set', _encode(name), _encode(value),
                          None if ex is None else int(ex))

    def setex(self, name, time, value):
        return self._push('setex', _encode(name), int(time), _encode(value))

    def delete(self, *names):
        return self._push('delete', [_encode(name) for name in _flatten(names)])

    def incr(self, name, amount=1):
        return self._push('incr', _encode(name),
                          None if amount == 1 else int(amount))

    def expire(self, name, time):
        return self._push('expire', _encode(name), int(time))

    def publish(self, channel, message):
        return self._push('publish', _encode(channel), _encode(message))

    def zadd(self, name, mapping, nx=False, xx=False, ch=False, incr=False,
             gt=False, lt=False):
        if nx or xx or ch or incr or gt or lt:
            raise NotImplementedError(
                'ZADD options unsupported by Rust Redis client')
        pairs = [(_encode(score), _encode(member))
                 for member, score in mapping.items()]
        return self._push('zadd', _encode(name), pairs)

    def zcount(self, name, min, max):
        return self._push('zcount', _encode(name), _encode(min), _encode(max))

    def zrange(self, name, start, end, desc=False, withscores=False,
               score_cast_func=float, byscore=False, bylex=False,
               offset=None, num=None):
        if desc or withscores or byscore or bylex or offset is not None \
                or num is not None:
            raise NotImplementedError(
                'ZRANGE options unsupported by Rust Redis client')
        return self._push('zrange', _encode(name), int(start), int(end))

    def rpush(self, name, *values):
        return self._push('rpush', _encode(name),
                          [_encode(v) for v in _flatten(values)])

    def llen(self, name):
        return self._push('llen', _encode(name))

    def lrange(self, name, start, end):
        return self._push('lrange', _encode(name), int(start), int(end))

    def execute(self, raise_on_error=True):
        _STATS['commands'] += 1
        return self._native.execute()


class RsConnection:
    """Provide the ``redis.Connection`` interface used by ``ResultConsumer``."""

    _sock = None

    def __init__(self, pubsub):
        self._pubsub = pubsub
        self._callbacks = []

    def can_read(self, timeout=0):
        return self._pubsub.can_read_now(timeout)

    def register_connect_callback(self, callback):
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    def deregister_connect_callback(self, callback):
        try:
            self._callbacks.remove(callback)
        except ValueError:
            pass

    def disconnect(self):
        pass

    def connect(self):
        pass

    def is_connected(self):
        return not self._pubsub.closed

    def __repr__(self):
        return '<RsConnection>'


class RsConnectionPool:
    """Client connection pool supporting ``reset`` and ``get_engine``."""

    def __init__(self, client):
        self._client = client

    def reset(self):
        self._client.reset()

    def disconnect(self):
        self._client.reset()

    def get_connection(self, *args, **kwargs):
        # Only pubsub has a usable connection pool; fail explicitly for other callers.
        raise NotImplementedError(
            'RsConnectionPool.get_connection is not used by the result backend')

    def release(self, connection):
        pass

    def get_encoder(self):
        return None

    @property
    def max_connections(self):
        return self._client._params.get('max_connections')


class RsPubSubConnectionPool:
    """Expose a pubsub connection through ``get_connection``."""

    def __init__(self, pubsub):
        self._pubsub = pubsub

    def get_connection(self, *args, **kwargs):
        return self._pubsub.connection

    def release(self, connection):
        pass

    def reset(self):
        self._pubsub._client.reset()


class RsPubSub:
    """Provide ``redis.client.PubSub`` for the backend result consumer."""

    # Marker for ResultConsumer's shim-specific path.
    _celery_rs_shim = True

    def __init__(self, native, ignore_subscribe_messages=False, client=None):
        self._native = native
        self._client = client
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self.channels = {}
        self.subscribed_event = threading.Event()
        self.connection = RsConnection(self)
        self.connection_pool = RsPubSubConnectionPool(self)
        self.closed = False
        _LIVE.add(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def subscribed(self):
        return self.subscribed_event.is_set()

    def subscribe(self, *args, **kwargs):
        channels = _flatten(args)
        if kwargs:
            channels.extend(kwargs)
        if not channels:
            raise _data_error('No channels to subscribe to')
        keys = [_channel_key(channel) for channel in channels]
        self._native.subscribe(keys)
        for channel, key in zip(channels, keys):
            self.channels[channel] = None
        if not self.subscribed_event.is_set():
            self.subscribed_event.set()

    def unsubscribe(self, *args):
        if args:
            channels = _flatten(args)
        else:
            channels = list(self.channels)
        if not channels:
            return None
        keys = [_channel_key(channel) for channel in channels]
        self._native.unsubscribe(keys)
        for channel in channels:
            self.channels.pop(channel, None)
        if not self.channels:
            self.subscribed_event.clear()

    def get_message(self, ignore_subscribe_messages=False, timeout=0.0):
        message = self._native.get_message(timeout)
        return message

    def can_read_now(self, timeout=0):
        return self._native.can_read(timeout)

    def wait_readable(self, timeout):
        try:
            return self._native.can_read(timeout)
        except Exception as exc:
            if not _is_connection_error(exc):
                raise
            # Report replaced sockets as readable so reconnect_on_error handles the next can_read failure.
            return True

    def on_connect(self, connection):
        if self.channels:
            self.subscribe(list(self.channels))

    def close(self):
        if not self.closed:
            self.closed = True
            self._native.close()
            self.channels.clear()
            self.subscribed_event.clear()

    reset = close

    def __repr__(self):
        return f'<RsPubSub channels={len(self.channels)}>'


def _channel_key(channel):
    """Convert a channel to a string for the Rust binding."""
    if isinstance(channel, bytes):
        return channel.decode('utf-8')
    return str(channel)


def use_rs_backend(backend):
    """Return whether the backend can use :class:`RsRedis`."""
    if _celery_rs is None or not _enabled():
        return False
    # Keep redis-py for replaced Backend.redis modules; the lazy module retains its real name and type.
    module = getattr(backend, 'redis', None)
    if type(module).__name__ != 'module' or getattr(module, '__name__', None) != 'redis':
        return False
    # Custom _get_client implementations, including Sentinel, must retain redis-py.
    getter = getattr(type(backend), '_get_client', None)
    if (getattr(getter, '__module__', None) != 'celery.backends.redis' or
            getattr(getter, '__name__', None) != '_get_client'):
        return False
    if getattr(backend, '_ConnectionPool', None) is not None:
        return False
    return RsRedis.supports(getattr(backend, 'connparams', None) or {})


_pid = os.getpid()


def after_fork():
    """Discard inherited Rust connections while keeping counters in the child."""
    global _pid
    pid = os.getpid()
    if pid == _pid:
        return
    _pid = pid
    _STATS['forks'] += 1
    # Retain inherited clients to prevent Drop from waking the parent's absent tokio driver.
    if _LIVE:
        _ORPHANED.extend(_LIVE)
        _LIVE.clear()
    if _celery_rs is not None:
        # Reset the runtime here too: the backend may be used without importing _rs_producer.
        _celery_rs.after_fork()


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=after_fork)
