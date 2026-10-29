"""Kombu-compatible producer with Rust serialization, compression and transport."""
from __future__ import annotations

import os
import time
from contextlib import contextmanager
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from kombu import Connection as KombuConnection
from kombu import Producer as KombuProducer
from kombu import exceptions as kombu_exceptions
from kombu.connection import ConnectionPool, PooledConnection
from kombu.connection import is_connection
from kombu.entity import Exchange, Queue, binding as binding_t
from kombu.entity import maybe_delivery_mode
from kombu.exceptions import OperationalError
from kombu.pools import ProducerPool
from kombu.serialization import dumps
from kombu.pools import set_limit
from kombu.utils.functional import dictfilter
from kombu.utils.json import JSONEncoder
from kombu.utils.url import as_url, parse_url

from celery.utils.queue_arguments import to_rabbitmq_queue_arguments

try:  # The full package installs the extension; the reference may not.
    import _celery_rs
except ImportError:  # pragma: no cover - native extension unavailable
    _celery_rs = None

__all__ = ('RsProducer', 'FastProducerPool', 'FastConnection',
           'use_fast_producer', 'fast_connection', 'after_fork', 'stats')

# Environment switch to restore the kombu producer.
ENV_DISABLE = 'CELERY_RS_PRODUCER'

# URL schemes and driver types supported by Rust.
_SCHEMES = {
    'redis': 'redis',
    'rediss': 'redis',
    'amqp': 'amqp',
    'amqps': 'amqp',
    'pyamqp': 'amqp',
}

# Custom transport aliases invalidate the Rust path; compare string aliases without importing transports.
_DEFAULT_ALIASES = {
    'redis': 'kombu.transport.redis:Transport',
    'rediss': 'kombu.transport.redis:Transport',
    'amqp': 'kombu.transport.pyamqp:Transport',
    'amqps': 'kombu.transport.pyamqp:SSLTransport',
    'pyamqp': 'kombu.transport.pyamqp:Transport',
}

_SECURE_SCHEMES = frozenset({'rediss', 'amqps'})

# Unsupported transport options retain kombu to preserve behavior.
_SUPPORTED_OPTIONS = {
    'redis': frozenset({
        'visibility_timeout', 'sep', 'priority_steps', 'queue_order_strategy',
        'fanout_prefix', 'fanout_patterns', 'global_keyprefix', 'unacked_key',
        'unacked_index_key', 'unacked_mutex_key', 'unacked_mutex_expire',
        'health_check_interval', 'socket_timeout', 'polling_interval',
        'body_encoding', 'deadletter_queue',
    }),
    'amqp': frozenset({'connection_name', 'confirm_publish', 'qos_global'}),
}

# Retry connection errors only; channel errors such as PRECONDITION_FAILED are permanent, as in py-amqp.
_RETRY_ERRORS = (ConnectionError, OSError)

# Kombu's JSON default reads its encoder registry on every call.
_JSON_DEFAULT = JSONEncoder().default

# Counters let tests verify that publishing uses Rust.
_STATS = {
    'connections': 0,
    'publishes': 0,
    'declarations': 0,
    'retries': 0,
    'forks': 0,
}


def stats():
    """Copy internal Rust producer counters."""
    return dict(_STATS)


def _enabled():
    value = os.environ.get(ENV_DISABLE)
    if value is None:
        return True
    return value.strip().lower() not in ('0', 'false', 'no', 'off')


def _scheme_is_fast(scheme):
    """Check that the scheme still maps to kombu's default transport."""
    if not isinstance(scheme, str):
        return False
    scheme = scheme.lower()
    if scheme not in _DEFAULT_ALIASES:
        return False
    # Custom aliases require kombu.transport to be loaded; otherwise the default string alias suffices.
    import sys
    transport = sys.modules.get('kombu.transport')
    if transport is None:
        return True
    return transport.TRANSPORT_ALIASES.get(scheme) == _DEFAULT_ALIASES[scheme]


def driver_type_of(connection):
    """Read ``transport.driver_type`` without creating the transport."""
    return _SCHEMES[(connection.transport_cls or '').lower()]


def connection_driver_type(connection):
    """Read ``transport.driver_type`` without importing the transport for FastConnection."""
    if isinstance(connection, FastConnection):
        return connection.driver_type
    return connection.transport.driver_type


def _options_are_fast(driver, options):
    return not (set(options or ()) - _SUPPORTED_OPTIONS[driver])


def _ssl_is_fast(scheme, ssl):
    if not ssl:
        return True
    # TLS requires a secure scheme with no custom options.
    if scheme not in _SECURE_SCHEMES:
        return False
    return not (isinstance(ssl, dict) and ssl)


def use_fast_producer(connection):
    """Return whether the connection supports the Rust producer."""
    if not _enabled() or _celery_rs is None or connection is None:
        return False
    scheme = getattr(connection, 'transport_cls', None)
    if not _scheme_is_fast(scheme):
        return False
    # Rust supports one URL, not multi-URL failover.
    if getattr(connection, 'alt', None):
        return False
    if not _ssl_is_fast(scheme.lower(), getattr(connection, 'ssl', None)):
        return False
    if getattr(connection, 'login_method', None):
        # Rust transport supports only PLAIN authentication.
        return False
    driver = _SCHEMES[scheme.lower()]
    options = getattr(connection, 'transport_options', None) or {}
    return _options_are_fast(driver, options)


class FastConnection(KombuConnection):
    """Build a kombu connection from parsed components without importing its transport."""

    @property
    def driver_type(self):
        """Read ``transport.driver_type`` without importing the transport."""
        return _SCHEMES[(self.transport_cls or '').lower()]

    @property
    def connection_errors(self):
        """Return Rust connection errors without loading a kombu transport."""
        return (ConnectionError, OSError, kombu_exceptions.OperationalError)

    @property
    def channel_errors(self):
        return (kombu_exceptions.OperationalError,)

    def as_uri(self, include_password=False, mask='**', getfields=None):
        """Format the connection URI without accessing ``self.transport``."""
        return as_url(
            self.transport_cls or '', self.hostname or 'localhost', self.port,
            self.userid, self.password, self.virtual_host,
            sanitize=not include_password, mask=mask,
        )

    def release(self):
        """Close lazy references without creating a kombu transport just to close it."""
        if (self._transport is None and self._connection is None
                and self._default_channel is None):
            self.declared_entities.clear()
            self._closed = True
            return
        super().release()

    close = release

    @contextmanager
    def _reraise_as_library_errors(
        self,
        ConnectionError=kombu_exceptions.OperationalError,
        ChannelError=kombu_exceptions.OperationalError,
    ):
        try:
            yield
        except (ConnectionError, ChannelError):
            raise
        except OSError as exc:
            raise ConnectionError(str(exc)) from exc


def _connection_params(app, url):
    """Build the arguments that ``Celery._connection`` passes to kombu."""
    conf = app.conf
    return {
        'hostname': url,
        'userid': conf.broker_user,
        'password': conf.broker_password,
        'virtual_host': conf.broker_vhost,
        'port': conf.broker_port,
        'transport': conf.broker_transport,
        'ssl': app.either('broker_use_ssl', None),
        'heartbeat': None,
        'login_method': conf.broker_login_method,
        'failover_strategy': conf.broker_failover_strategy,
        'transport_options': dict(conf.broker_transport_options or {}),
        'connect_timeout': app.either('broker_connection_timeout', None),
    }


def fast_connection(app, url=None):
    """Build a write connection without importing its transport; return ``None`` if unsupported."""
    if not _enabled() or _celery_rs is None:
        return None
    if not _connection_not_overridden(app):
        # Respect custom or mocked app.connection_for_write implementations.
        return None
    url = url or app.conf.broker_write_url
    if not isinstance(url, str) or not url or ';' in url:
        return None
    try:
        scheme = app.conf.broker_transport or urlsplit(url).scheme or ''
    except ValueError:
        return None
    if not _scheme_is_fast(scheme):
        return None
    try:
        # Merge URL and configuration fields without making kombu import the transport for can_parse_url.
        params = dict(_connection_params(app, url))
        parsed = parse_url(url)
        params.update(dictfilter(parsed), hostname=parsed['hostname'])
        connection = FastConnection(**params)
    except Exception:
        # Delegate unrecognized URLs or configuration to kombu.
        return None
    if not use_fast_producer(connection):
        return None
    return connection


def is_fast_connection(connection):
    """Return whether the connection is a :class:`FastConnection`."""
    return isinstance(connection, FastConnection)


def _connection_not_overridden(app):
    """Return whether the app retains its original ``connection_for_write``."""
    if 'connection_for_write' in getattr(app, '__dict__', {}):
        return False
    from celery.app.base import Celery
    return type(app).connection_for_write is Celery.connection_for_write


def ensure_connection(connection, errback=None, max_retries=None,
                      interval_start=2, interval_step=2, interval_max=30):
    """Ensure a Rust connection with kombu retry intervals, limits and errbacks."""
    retries = 0
    while True:
        try:
            _native_entry(connection)
            return connection
        except _RETRY_ERRORS as exc:
            if max_retries is not None and retries >= max_retries:
                raise OperationalError(str(exc)) from exc
            interval = min(interval_start + interval_step * retries, interval_max)
            if errback is not None:
                errback(exc, interval)
            if interval:
                # Match retry_over_time's one-second sleep steps.
                for _ in range(int(interval)):
                    time.sleep(1.0)
                time.sleep(abs(int(interval) - interval))
            retries += 1
            _STATS['retries'] += 1


def release_connection(connection):
    """Close the Rust connection associated with *connection*, ignoring errors."""
    entry = _entries.get(_native_key(connection))
    if entry is not None:
        _discard_entry(connection, entry)


def pool_limit(app):
    """Read the effective pool limit without creating ``app.pool``."""
    return set_limit(app.conf.broker_pool_limit)


def _merge_query(url, extra):
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update({key: str(value) for key, value in extra.items()})
    return urlunsplit((parts.scheme, parts.netloc, parts.path,
                       urlencode(query), parts.fragment))


def _broker_url(connection):
    """Rebuild a connection URL without accessing ``connection.transport``."""
    scheme = (connection.transport_cls or '').lower()
    host = connection.hostname or 'localhost'
    userid = connection.userid
    password = connection.password
    if callable(password):
        password = password()
    parts = [f'{scheme}://']
    if userid or password:
        if userid:
            parts.append(quote(str(userid), safe=''))
        if password:
            parts.append(':' + quote(str(password), safe=''))
        parts.append('@')
    host_text = str(host or '')
    if ':' in host_text and not host_text.startswith('['):
        # IPv6 hostnames lose brackets in Connection but require them in URLs.
        parts.append(f'[{host_text}]')
    else:
        parts.append(quote(host_text, safe=''))
    if connection.port:
        parts.append(f':{connection.port}')
    # Match as_url's leading slash and skipped empty parts: vhost '/' becomes '//'.
    parts.extend(['/', str(connection.virtual_host or '')])
    url = ''.join(part for part in parts if part)

    if scheme in ('amqp', 'amqps', 'pyamqp'):
        extra = {
            # Match py-amqp's default heartbeat of zero; lapin's default would disconnect idle producers.
            'heartbeat': int(getattr(connection, 'heartbeat', None) or 0),
        }
        timeout = getattr(connection, 'connect_timeout', None)
        if timeout is not None:
            seconds = int(timeout)
            if seconds > 0:
                extra['connection_timeout'] = seconds
        url = _merge_query(url, extra)
    return url


def _native_key(connection):
    url = _broker_url(connection)
    options = dict(getattr(connection, 'transport_options', None) or {})
    return (url, repr(sorted(options.items())))


class _NativeEntry:
    """Rust connection and declaration cache shared per process and URL."""

    __slots__ = ('connection', 'declared')

    def __init__(self, connection):
        self.connection = connection
        self.declared = set()


# Cache per URL and options; clear after fork because inherited sockets are unusable.
_entries = {}

# Keep inherited connections alive so destructors cannot access the parent's absent tokio runtime.
_orphaned = []


def _native_entry(connection, key=None):
    """Create a connection per URL and options, using atomic ``dict.setdefault`` under the GIL."""
    if key is None:
        key = _native_key(connection)
    entry = _entries.get(key)
    if entry is not None:
        return entry
    options = dict(getattr(connection, 'transport_options', None) or {})
    # Retry is managed here as in kombu.Connection.ensure; disable native retries.
    fresh = _NativeEntry(_celery_rs.Connection(
        key[0], options, retry_policy={'enabled': False}))
    entry = _entries.setdefault(key, fresh)
    if entry is not fresh:
        try:
            fresh.connection.close()
        except Exception:  # Close the losing connection if another thread populated the cache first.
            pass
        return entry
    _STATS['connections'] += 1
    return entry


def _discard_entry(connection, entry):
    """Remove *entry* from the cache and close its connection, ignoring errors."""
    key = _native_key(connection)
    if _entries.get(key) is entry:
        _entries.pop(key, None)
    try:
        entry.connection.close()
    except Exception:  # The connection is already lost; ignore close errors.
        pass


_fork_pid = os.getpid()


def after_fork():
    """Discard inherited Rust connections and runtime in the child."""
    global _fork_pid
    pid = os.getpid()
    if _fork_pid == pid:
        return
    _fork_pid = pid
    # Only one thread survives fork; avoid locks possibly held by vanished threads.
    if _entries:
        _orphaned.extend(_entries.values())
        _entries.clear()
    _STATS['forks'] += 1
    if _celery_rs is not None:
        # Reset the inherited runtime; the native module recreates it on first use.
        _celery_rs.after_fork()


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=after_fork)


def _prepare_body(body, serializer, content_type, content_encoding):
    """Prepare messages as kombu does, with Rust JSON encoding and compression."""
    if not content_type:
        if serializer is None:
            if isinstance(body, bytes):
                return body, 'application/data', 'binary'
            if isinstance(body, str):
                return body.encode('utf-8'), 'text/plain', 'utf-8'
        if serializer in (None, 'json'):
            return _celery_rs.json_dumps(body, _JSON_DEFAULT), 'application/json', 'utf-8'
        content_type, content_encoding, body = dumps(body, serializer=serializer)
        if isinstance(body, str):
            body = body.encode(content_encoding)
        return body, content_type, content_encoding

    if isinstance(body, str):
        content_encoding = content_encoding or 'utf-8'
        body = body.encode(content_encoding)
    elif not content_encoding:
        content_encoding = 'binary'
    return body, content_type, content_encoding


def _compress_body(body, compression, headers):
    if not compression:
        return body
    try:
        body, headers['compression'] = _celery_rs.compress(body, compression)
    except KeyError:
        # Custom kombu compression methods are not part of Rust.
        from kombu.compression import compress as kombu_compress
        body, headers['compression'] = kombu_compress(body, compression)
    return body


class RsProducer(KombuProducer):
    """Provide the kombu producer interface with Rust publishing and declarations."""

    def __init__(self, channel=None, exchange=None, routing_key=None,
                 serializer=None, auto_declare=None, compression=None,
                 on_return=None):
        # Skip kombu.Producer.__init__, which would bind a ChannelPromise and open kombu on first exchange use.
        self._channel = channel
        self.__connection__ = channel if is_connection(channel) else None
        self.exchange = exchange if exchange is not None else Exchange('')
        self.routing_key = routing_key or ''
        self.serializer = serializer or None
        self.compression = compression or None
        self.on_return = on_return or None
        # Cache URL, options and native entry per connection; revive invalidates them.
        self._rs_key = None
        self._rs_entry = None
        if auto_declare is not None:
            self.auto_declare = auto_declare

    @property
    def connection(self):
        """Return the producer's lazy kombu connection."""
        if self.__connection__ is not None:
            return self.__connection__
        channel = self._channel
        if channel is None:
            return None
        return getattr(getattr(channel, 'connection', None), 'client', None)

    @property
    def channel(self):
        """Create a kombu channel on demand."""
        channel = self._channel
        if channel is None:
            return None
        if is_connection(channel):
            return channel.default_channel
        return channel

    @channel.setter
    def channel(self, value):
        self._channel = value

    def revive(self, channel=None):
        """Update the channel after reconnection."""
        self._rs_key = None
        self._rs_entry = None
        if channel is None:
            return
        self._channel = channel
        if is_connection(channel):
            self.__connection__ = channel
        else:
            self.__connection__ = getattr(
                getattr(channel, 'connection', None), 'client', None)

    def _key(self):
        if self._rs_key is None:
            self._rs_key = _native_key(self.connection)
        return self._rs_key

    def _entry(self):
        key = self._key()
        entry = self._rs_entry
        if entry is not None and _entries.get(key) is entry:
            return entry
        self._rs_entry = None
        entry = _entries.get(key)
        if entry is None:
            entry = _native_entry(self.connection, key)
        self._rs_entry = entry
        return entry

    def _native(self):
        connection = self.connection
        if connection is None:
            # Permanent errors must not be retried.
            raise OperationalError('producer has no connection')
        return connection, self._entry()

    def _delivery_details(self, exchange, delivery_mode=None):
        if isinstance(exchange, Exchange):
            return exchange.name, maybe_delivery_mode(
                delivery_mode or exchange.delivery_mode,
            )
        return exchange, maybe_delivery_mode(
            delivery_mode or self.exchange.delivery_mode,
        )


    def _declare_exchange(self, entry, exchange):
        name = exchange.name
        if exchange.no_declare or not name or name.startswith('amq.'):
            return
        entry.connection.declare_exchange(
            name, exchange.type or 'direct',
            durable=exchange.durable,
            auto_delete=exchange.auto_delete,
            passive=exchange.passive,
            arguments=exchange.arguments,
        )
        _STATS['declarations'] += 1

    def _declare_queue(self, entry, queue):
        if queue.no_declare:
            return
        if queue.exchange:
            self._declare_exchange(entry, queue.exchange)
        arguments = to_rabbitmq_queue_arguments(
            queue.queue_arguments or {},
            expires=queue.expires,
            message_ttl=queue.message_ttl,
            max_length=queue.max_length,
            max_length_bytes=queue.max_length_bytes,
            max_priority=queue.max_priority,
        )
        name = entry.connection.declare_queue(
            queue.name or None,
            durable=queue.durable,
            auto_delete=queue.auto_delete,
            exclusive=queue.exclusive,
            arguments=arguments or None,
        )
        if not queue.name:
            queue.name = name
        if queue.exchange and queue.exchange.name:
            entry.connection.bind(
                queue.name, queue.exchange.name, queue.routing_key or '',
                arguments=queue.binding_arguments,
            )
        for extra in queue.bindings:
            self._declare_binding(entry, extra, queue)
        _STATS['declarations'] += 1

    def _declare_binding(self, entry, extra, queue):
        """Declare an additional queue-to-exchange binding from ``Queue.bindings``."""
        exchange = extra.exchange
        if exchange is None:
            return
        self._declare_exchange(entry, exchange)
        if exchange.name and queue.name:
            entry.connection.bind(
                queue.name, exchange.name, extra.routing_key or '',
                arguments=extra.arguments,
            )

    def _declare_entity(self, entry, entity):
        if isinstance(entity, Queue):
            self._declare_queue(entry, entity)
        elif isinstance(entity, Exchange):
            self._declare_exchange(entry, entity)
        elif isinstance(entity, binding_t):
            # Rust does not support exchange-to-exchange bindings.
            raise NotImplementedError(
                'exchange-to-exchange binding unsupported by Rust producer')
        else:
            # Fail explicitly rather than silently omitting a declaration.
            raise TypeError(
                f'entity cannot be declared by Rust producer: {type(entity)!r}')

    def _maybe_declare(self, entry, entity):
        ident = None
        if getattr(entity, 'can_cache_declaration', False):
            ident = hash(entity)
            if ident in entry.declared:
                return False
        self._declare_entity(entry, entity)
        if ident is not None:
            entry.declared.add(ident)
        return True

    def declare(self):
        """Declare the default exchange if named."""
        if self.exchange and self.exchange.name:
            _, entry = self._native()
            self._declare_exchange(entry, self.exchange)

    def maybe_declare(self, entity, retry=False, **retry_policy):
        """Declare *entity* using the connection cache, as ``kombu.common.maybe_declare`` does."""
        if not entity:
            return None
        if retry:
            return self._ensure(
                lambda: self._maybe_declare(self._native()[1], entity),
                retry_policy,
            )
        return self._call(lambda: self._maybe_declare(self._native()[1], entity))


    def _call(self, fun):
        """Run *fun*, discarding a broken Rust connection even when retries are disabled."""
        try:
            return fun()
        except _RETRY_ERRORS:
            self._drop_native()
            raise

    def _ensure(self, fun, retry_policy):
        """Retry with ``kombu.Connection.ensure`` semantics."""
        policy = dict(retry_policy or {})
        errback = policy.pop('errback', None)
        max_retries = policy.pop('max_retries', None)
        interval_start = policy.pop('interval_start', 1)
        interval_step = policy.pop('interval_step', 1)
        interval_max = policy.pop('interval_max', 1)
        retries = 0
        while True:
            # Match ensure: call errback only for errors after a successful connection.
            had_connection = self._has_native_connection()
            try:
                return self._call(fun)
            except _RETRY_ERRORS as exc:
                if max_retries is not None and retries >= max_retries:
                    raise OperationalError(str(exc)) from exc
                if had_connection and errback is not None:
                    errback(exc, 0)
                interval = min(interval_start + interval_step * retries,
                               interval_max)
                if interval > 0:
                    time.sleep(interval)
                retries += 1
                _STATS['retries'] += 1

    def _has_native_connection(self):
        connection = self.connection
        if connection is None:
            return False
        try:
            key = self._key()
        except Exception:
            return False
        return key in _entries

    def _drop_native(self):
        """Discard the current Rust connection so the next attempt reconnects."""
        connection = self.connection
        if connection is None:
            return
        self._rs_entry = None
        try:
            key = self._key()
        except Exception:
            return
        entry = _entries.get(key)
        if entry is not None:
            _discard_entry(connection, entry)

    def _publish_native(self, body, priority, content_type, content_encoding,
                        headers, properties, routing_key, exchange_name,
                        declare, retry, retry_policy):
        _, entry = self._native()
        for entity in declare:
            if retry:
                self._ensure(
                    lambda entity=entity: self._maybe_declare(entry, entity),
                    retry_policy,
                )
            else:
                self._maybe_declare(entry, entity)
        entry.connection.publish(
            body,
            exchange=exchange_name,
            routing_key=routing_key,
            headers=headers,
            properties=properties,
            content_type=content_type,
            content_encoding=content_encoding,
            priority=priority,
            delivery_mode=properties.get('delivery_mode'),
            omit_priority=priority is None,
        )
        _STATS['publishes'] += 1

    def _publish_kombu(self, *args, **kwargs):
        """Fall back to kombu for unsupported Rust transport options."""
        properties = dict(kwargs.pop('properties'))
        # delivery_mode is explicit; expiration is already a millisecond string in properties.
        properties.pop('delivery_mode', None)
        kwargs.pop('expiration', None)
        producer = KombuProducer(
            self.channel,
            exchange=self.exchange,
            routing_key=self.routing_key,
            serializer=self.serializer,
            auto_declare=self.auto_declare,
            compression=self.compression,
            on_return=self.on_return,
        )
        return producer.publish(*args, **kwargs, **properties)

    def publish(self, body, routing_key=None, delivery_mode=None,
                mandatory=False, immediate=False, priority=0,
                content_type=None, content_encoding=None, serializer=None,
                headers=None, compression=None, exchange=None, retry=False,
                retry_policy=None, declare=None, expiration=None, timeout=None,
                confirm_timeout=None, **properties):
        """Publish with Rust serialization and I/O using kombu semantics."""
        declare = [] if declare is None else declare
        headers = {} if headers is None else headers
        retry_policy = {} if retry_policy is None else retry_policy
        routing_key = self.routing_key if routing_key is None else routing_key
        compression = self.compression if compression is None else compression

        exchange_name, properties['delivery_mode'] = self._delivery_details(
            exchange or self.exchange, delivery_mode,
        )
        if expiration is not None:
            properties['expiration'] = str(int(expiration * 1000))

        body, content_type, content_encoding = _prepare_body(
            body, serializer or self.serializer, content_type, content_encoding)
        body = _compress_body(body, compression, headers)

        if self.auto_declare and self.exchange.name:
            if self.exchange not in declare:
                declare.append(self.exchange)

        reply_to = properties.get('reply_to')
        if isinstance(reply_to, Queue):
            properties['reply_to'] = reply_to.name

        if self.on_return or mandatory or immediate:
            return self._publish_kombu(
                body, routing_key=routing_key,
                delivery_mode=properties['delivery_mode'],
                mandatory=mandatory, immediate=immediate, priority=priority,
                content_type=content_type, content_encoding=content_encoding,
                serializer=None, headers=headers, compression=None,
                exchange=exchange_name, retry=retry,
                retry_policy=retry_policy, declare=declare,
                timeout=timeout, confirm_timeout=confirm_timeout,
                properties=properties,
            )

        args = (
            body, priority, content_type, content_encoding, headers,
            properties, routing_key, exchange_name, declare, retry,
            retry_policy,
        )
        if retry:
            return self._ensure(lambda: self._publish_native(*args), retry_policy)
        return self._call(lambda: self._publish_native(*args))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def release(self):
        """Keep the connection shared by the process; no resources need releasing here."""

    close = release


class FastConnectionPool(ConnectionPool):
    """Create pooled connections from FastConnection parameters without loading the transport."""

    def new(self):
        conn = self.connection
        return PooledConnection(self, **_pooled_params(conn))


def _pooled_params(conn):
    return {
        'hostname': conn.hostname,
        'userid': conn.userid,
        'password': conn.password,
        'virtual_host': conn.virtual_host,
        'port': conn.port,
        'ssl': conn.ssl,
        'transport': conn.transport_cls,
        'connect_timeout': conn.connect_timeout,
        'transport_options': dict(conn.transport_options or {}),
        'login_method': conn.login_method,
        'heartbeat': conn.heartbeat,
        'failover_strategy': conn._failover_strategy,
        'alternates': conn.alt,
        'credential_provider': getattr(conn, 'credential_provider', None),
    }


class FastProducerPool(ProducerPool):
    """Create ``RsProducer`` instances without kombu sockets."""

    Producer = RsProducer

    def __init__(self, connection, limit=None, driver_type=None):
        # Preserve kombu PooledConnection identity without importing its transport.
        super().__init__(
            FastConnectionPool(connection, limit=limit), limit=limit)
        # Avoid loading broker clients through transport.driver_type when choosing ETA behavior.
        self.driver_type = driver_type or driver_type_of(connection)

    def create_producer(self):
        # The acquired connection is a descriptor; I/O uses the shared Rust connection.
        connection = self._acquire_connection()
        try:
            return self.Producer(connection)
        except BaseException:
            connection.release()
            raise
