"""Shim redis-py sopra il client Redis del core Rust.

Il backend ``celery.backends.redis`` continua a usare la stessa logica del
riferimento; cambia solo l'oggetto restituito da ``_create_client``. Questo
modulo riproduce le firme e i tipi di ritorno di redis-py per il sottoinsieme
usato dal backend (comandi singoli, pipeline, pub/sub, pool) e lascia passare
le eccezioni di redis-py, così ``connection_errors``, ``ensure`` e
``_reconnect_pubsub`` restano invariati.

Il percorso Rust è attivo solo quando tutti i parametri di connessione sono
mappati (vedi :meth:`RsRedis.supports`); ``CELERY_RS_BACKEND=0`` lo disattiva.
"""
from __future__ import annotations

import os
import threading
import weakref

try:  # il pacchetto completo lo installa sempre; il riferimento no.
    import _celery_rs
except ImportError:  # pragma: no cover - solo senza estensione nativa
    _celery_rs = None

__all__ = (
    'RsRedis', 'RsPipeline', 'RsPubSub', 'RsConnectionPool',
    'RsPubSubConnectionPool', 'RsConnection', 'use_rs_backend', 'stats',
)

#: Variabile d'ambiente per tornare al client redis-py.
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
    # I socket inattivi sono del server riavviato; redis-py li scarta
    # prima del comando, senza trasformare il primo GET in un errore del task.
    client.reset()
    return True


#: Contatori interni (i test verificano che il backend usi davvero Rust).
_STATS = {
    'clients': 0,
    'pipelines': 0,
    'pubsubs': 0,
    'commands': 0,
    'forks': 0,
    'instances': 0,
}


def stats():
    """Copia dei contatori del client Rust."""
    return dict(_STATS)


#: Istanze vive: dopo `fork()` si tengono referenziate per sempre nel figlio
#: (vedi `after_fork`), così i loro `Drop` Rust non toccano il runtime tokio
#: del padre, che nel figlio non esiste più.
_LIVE = weakref.WeakSet()
_ORPHANED = []


def _enabled():
    value = os.environ.get(ENV_DISABLE)
    if value is None:
        return True
    return value.strip().lower() not in ('0', 'false', 'no', 'off')


def _data_error(message):
    """``redis.exceptions.DataError`` importato solo quando serve.

    A import di questo modulo non si carica redis-py: il client Rust non ne
    ha bisogno e il costo in RSS si vedrebbe su ogni comando che usa il
    backend (``celery call``/``result``).
    """
    try:
        from redis.exceptions import DataError
    except ImportError:  # pragma: no cover - redis-py è una dipendenza del backend
        return ValueError(message)
    return DataError(message)


def _is_connection_error(exc):
    """True per gli errori di connessione di redis-py (o builtin equivalenti)."""
    try:
        from redis.exceptions import ConnectionError as RedisConnectionError
        from redis.exceptions import TimeoutError as RedisTimeoutError
    except ImportError:  # pragma: no cover
        return isinstance(exc, (ConnectionError, TimeoutError, OSError))
    return isinstance(
        exc, (RedisConnectionError, RedisTimeoutError, ConnectionError,
              TimeoutError, OSError))


def _encode(value):
    """Stessa codifica degli argomenti di ``redis.Encoder``."""
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
    """``list_or_args`` di redis-py: una sequenza singola o argomenti vari."""
    if len(args) == 1 and isinstance(args[0], (list, tuple, set, frozenset)):
        return list(args[0])
    return list(args)


#: Parametri che il client Rust sa applicare. Qualunque altro lascia il
#: backend su redis-py: meglio lenti che diversi.
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
    # Confronto per nome: la classe vera di redis-py (o il segnaposto lazy del
    # backend) senza importare il pacchetto.
    return (getattr(connection_class, '__name__', '') == 'SSLConnection' and
            str(getattr(connection_class, '__module__', '')).startswith('redis'))


def _is_unix_connection(connection_class):
    return (getattr(connection_class, '__name__', '') ==
            'UnixDomainSocketConnection' and
            str(getattr(connection_class, '__module__', '')).startswith('redis'))


class RsRedis:
    """Stessa superficie di ``redis.Redis`` usata dal result backend."""

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
        """True se tutti i parametri di connessione sono mappati dal core."""
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
        # `socket_keepalive` e `health_check_interval` sono accettati: redis-rs
        # gestisce socket e riconnessione della connessione multiplexata e
        # questi valori non cambiano i comandi inviati al server.
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

    # ------------------------------------------------------------ comandi

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
                'opzione SET non supportata dal client Redis Rust')
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
                'opzioni ZADD non supportate dal client Redis Rust')
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
                'opzioni ZRANGE non supportate dal client Redis Rust')
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

    # ------------------------------------------------------- pipeline/pubsub

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
    """Pipeline redis-py: comandi concatenabili e MULTI/EXEC di default."""

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
                'opzione SET non supportata dal client Redis Rust')
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
                'opzioni ZADD non supportate dal client Redis Rust')
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
                'opzioni ZRANGE non supportate dal client Redis Rust')
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
    """``redis.Connection`` minimo visto da ``ResultConsumer``.

    ``can_read`` è l'unico metodo che il drainer usa sul socket; la parte
    privata ``_sock`` non serve perché ``ResultConsumer`` riconosce lo shim.
    """

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
    """``ConnectionPool`` del client (solo ``reset`` e ``get_engine``)."""

    def __init__(self, client):
        self._client = client

    def reset(self):
        self._client.reset()

    def disconnect(self):
        self._client.reset()

    def get_connection(self, *args, **kwargs):
        # Il backend non preleva connessioni dal pool del client: il pub/sub
        # ha il proprio pool (``RsPubSubConnectionPool``). Meglio un errore
        # esplicito che una connessione inutilizzabile.
        raise NotImplementedError(
            'RsConnectionPool.get_connection non è usato dal result backend')

    def release(self, connection):
        pass

    def get_encoder(self):
        return None

    @property
    def max_connections(self):
        return self._client._params.get('max_connections')


class RsPubSubConnectionPool:
    """Vista del pool legata a un pubsub (``get_connection`` restituisce quella)."""

    def __init__(self, pubsub):
        self._pubsub = pubsub

    def get_connection(self, *args, **kwargs):
        return self._pubsub.connection

    def release(self, connection):
        pass

    def reset(self):
        self._pubsub._client.reset()


class RsPubSub:
    """``redis.client.PubSub`` per il ``ResultConsumer`` del backend."""

    #: Marker usato da ``ResultConsumer`` per il ramo dedicato (solo shim).
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
            # Stesso contratto di ``_socket_readable``: un socket chiuso o
            # sostituito è "leggibile". L'errore vero viene sollevato dal
            # successivo ``can_read`` dentro ``reconnect_on_error``, così il
            # backend riconnette invece di far fallire ``get()``.
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
    """Canale come stringa per il binding Rust."""
    if isinstance(channel, bytes):
        return channel.decode('utf-8')
    return str(channel)


def use_rs_backend(backend):
    """True se *backend* può usare :class:`RsRedis` al posto di redis-py."""
    if _celery_rs is None or not _enabled():
        return False
    # Sostituzioni di `Backend.redis` nei test upstream (finto modulo): lì
    # deve restare redis-py. Il modulo segnaposto del backend si chiama
    # ``redis`` ed è un ``module``, senza importare redis-py.
    module = getattr(backend, 'redis', None)
    if type(module).__name__ != 'module' or getattr(module, '__name__', None) != 'redis':
        return False
    # `_get_client` sovrascritto (Sentinel o test): resta redis-py.
    getter = getattr(type(backend), '_get_client', None)
    if (getattr(getter, '__module__', None) != 'celery.backends.redis' or
            getattr(getter, '__name__', None) != '_get_client'):
        return False
    if getattr(backend, '_ConnectionPool', None) is not None:
        return False
    return RsRedis.supports(getattr(backend, 'connparams', None) or {})


_pid = os.getpid()


def after_fork():
    """Nel figlio i contatori restano, le connessioni Rust si scartano."""
    global _pid
    pid = os.getpid()
    if pid == _pid:
        return
    _pid = pid
    _STATS['forks'] += 1
    # Nel figlio i client/pubsub ereditati non sono utilizzabili; tenerli vivi
    # evita che il GC ne esegua il Drop (che risveglierebbe il driver tokio
    # del padre, ormai assente).
    if _LIVE:
        _ORPHANED.extend(_LIVE)
        _LIVE.clear()
    if _celery_rs is not None:
        # Il runtime tokio del padre non è utilizzabile nel figlio: il modulo
        # nativo lo ricrea al primo uso. Lo fa già `_rs_producer`, ma il
        # backend deve essere fork-safe anche se il producer non è importato.
        _celery_rs.after_fork()


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=after_fork)
