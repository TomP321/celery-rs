"""Producer kombu-compatibile che pubblica con il core Rust.

Sostituisce il percorso caldo ``apply_async`` → ``send_task`` → ``publish``:
la serializzazione (``_celery_rs.json_dumps``), la compressione
(``_celery_rs.compress``) e la pubblicazione (``_celery_rs.Connection``)
avvengono in Rust, con lo stesso messaggio prodotto da kombu.

Il producer espone la stessa interfaccia di :class:`kombu.Producer` usata da
Celery: ``connection`` resta un normale :class:`kombu.Connection` (nessun
socket e nessun import del trasporto finché non serve) e ``channel`` crea un
canale kombu solo se qualcuno lo richiede (per esempio il backend ``rpc://``).

Il percorso veloce è attivo per Redis/AMQP senza opzioni non supportate (vedi
:func:`use_fast_producer`); ``CELERY_RS_PRODUCER=0`` lo disattiva.
"""
from __future__ import annotations

import os
import time
from contextlib import contextmanager
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from kombu import Connection as KombuConnection
from kombu import Producer as KombuProducer
from kombu import exceptions as kombu_exceptions
from kombu.connection import is_connection
from kombu.entity import Exchange, Queue, binding as binding_t
from kombu.entity import maybe_delivery_mode
from kombu.exceptions import OperationalError
from kombu.pools import ProducerPool, connections as connection_pools
from kombu.pools import set_limit
from kombu.resource import Resource
from kombu.serialization import dumps
from kombu.transport import TRANSPORT_ALIASES
from kombu.transport.base import to_rabbitmq_queue_arguments
from kombu.utils.functional import dictfilter
from kombu.utils.json import JSONEncoder
from kombu.utils.url import parse_url

try:  # il pacchetto completo lo installa sempre; il riferimento no.
    import _celery_rs
except ImportError:  # pragma: no cover - solo senza estensione nativa
    _celery_rs = None

__all__ = ('RsProducer', 'FastProducerPool', 'FastConnection',
           'use_fast_producer', 'fast_connection', 'after_fork', 'stats')

#: Variabile d'ambiente per tornare al producer di kombu.
ENV_DISABLE = 'CELERY_RS_PRODUCER'

#: Trasporti (schema URL → ``driver_type``) coperti dal core Rust.
_SCHEMES = {
    'redis': 'redis',
    'rediss': 'redis',
    'amqp': 'amqp',
    'amqps': 'amqp',
    'pyamqp': 'amqp',
}

#: Valori di default di ``kombu.transport.TRANSPORT_ALIASES``: se l'utente ha
#: registrato un trasporto custom per lo stesso schema, il percorso Rust non
#: è equivalente. Sono stringhe: confrontarle non importa nulla.
_DEFAULT_ALIASES = {
    'redis': 'kombu.transport.redis:Transport',
    'rediss': 'kombu.transport.redis:Transport',
    'amqp': 'kombu.transport.pyamqp:Transport',
    'amqps': 'kombu.transport.pyamqp:SSLTransport',
    'pyamqp': 'kombu.transport.pyamqp:Transport',
}

_SECURE_SCHEMES = frozenset({'rediss', 'amqps'})

#: Opzioni di trasporto che il binding Rust sa applicare. Qualunque altra
#: opzione lascia il producer su kombu: meglio lenti che diversi.
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

#: Errori del binding Rust che valgono un nuovo tentativo: solo quelli di
#: connessione. Gli errori di canale (es. PRECONDITION_FAILED) sono definitivi,
#: come in kombu con py-amqp.
_RETRY_ERRORS = (ConnectionError, OSError)

#: default del JSON di kombu: legge ``kombu.utils.json._encoders`` a ogni uso.
_JSON_DEFAULT = JSONEncoder().default

#: Contatori interni (verificati dai test: il publish passa davvero da Rust).
_STATS = {
    'connections': 0,
    'publishes': 0,
    'declarations': 0,
    'retries': 0,
    'forks': 0,
}


def stats():
    """Copia dei contatori interni del percorso Rust."""
    return dict(_STATS)


def _enabled():
    value = os.environ.get(ENV_DISABLE)
    if value is None:
        return True
    return value.strip().lower() not in ('0', 'false', 'no', 'off')


def _scheme_is_fast(scheme):
    """Schema supportato e ancora mappato al trasporto di default di kombu."""
    if not isinstance(scheme, str):
        return False
    scheme = scheme.lower()
    if scheme not in _DEFAULT_ALIASES:
        return False
    # Nessun import: il valore di default è una stringa.
    return TRANSPORT_ALIASES.get(scheme) == _DEFAULT_ALIASES[scheme]


def driver_type_of(connection):
    """``transport.driver_type`` senza istanziare il trasporto."""
    return _SCHEMES[(connection.transport_cls or '').lower()]


def connection_driver_type(connection):
    """``transport.driver_type``, senza importare il trasporto per FastConnection."""
    if isinstance(connection, FastConnection):
        return connection.driver_type
    return connection.transport.driver_type


def _options_are_fast(driver, options):
    return not (set(options or ()) - _SUPPORTED_OPTIONS[driver])


def _ssl_is_fast(scheme, ssl):
    if not ssl:
        return True
    # TLS supportato solo con lo schema già sicuro e senza opzioni custom.
    if scheme not in _SECURE_SCHEMES:
        return False
    return not (isinstance(ssl, dict) and ssl)


def use_fast_producer(connection):
    """True se *connection* può essere sostituita dal producer Rust."""
    if not _enabled() or _celery_rs is None or connection is None:
        return False
    scheme = getattr(connection, 'transport_cls', None)
    if not _scheme_is_fast(scheme):
        return False
    # Failover su più URL: il producer Rust ne gestisce una sola.
    if getattr(connection, 'alt', None):
        return False
    if not _ssl_is_fast(scheme.lower(), getattr(connection, 'ssl', None)):
        return False
    if getattr(connection, 'login_method', None):
        # Il trasporto Rust usa solo PLAIN.
        return False
    driver = _SCHEMES[scheme.lower()]
    options = getattr(connection, 'transport_options', None) or {}
    return _options_are_fast(driver, options)


class FastConnection(KombuConnection):
    """``kombu.Connection`` costruita senza importare la classe del trasporto.

    ``kombu.Connection(url)`` importa il modulo del trasporto solo per leggere
    ``can_parse_url``: con i componenti già separati (host, porta, vhost)
    otteniamo la stessa connessione lazy senza pagare redis-py o py-amqp.

    ``send_task`` usa ``_reraise_as_library_errors`` anche quando pubblica dal
    percorso Rust: qui la mappatura non richiede il trasporto (basta
    ``OperationalError``, come fa kombu).
    """

    @property
    def driver_type(self):
        """``transport.driver_type`` senza importare il trasporto."""
        return _SCHEMES[(self.transport_cls or '').lower()]

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
    """Gli stessi argomenti che ``Celery._connection`` passa a kombu."""
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
    """Come ``app.connection_for_write()`` ma senza importare il trasporto.

    Restituisce ``None`` (e il chiamante usa kombu) se lo schema non è
    supportato, se c'è failover su più URL o se le opzioni non sono mappabili.
    """
    if not _enabled() or _celery_rs is None:
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
        # Ricostruisce il merge URL + configurazione fatto da kombu, senza la
        # stringa URL (che farebbe importare il trasporto per `can_parse_url`).
        params = dict(_connection_params(app, url))
        parsed = parse_url(url)
        params.update(dictfilter(parsed), hostname=parsed['hostname'])
        connection = FastConnection(**params)
    except Exception:
        # URL o configurazione non riconoscibili: decide il percorso kombu.
        return None
    if not use_fast_producer(connection):
        return None
    return connection


def pool_limit(app):
    """Limite effettivo del pool, come ``app.pool.limit`` ma senza crearlo."""
    return set_limit(app.conf.broker_pool_limit)


def _merge_query(url, extra):
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update({key: str(value) for key, value in extra.items()})
    return urlunsplit((parts.scheme, parts.netloc, parts.path,
                       urlencode(query), parts.fragment))


def _broker_url(connection):
    """URL con credenziali e parametri, ricostruito dai campi della Connection.

    Come ``Connection.as_uri()``, ma senza toccare ``connection.transport``
    (che importerebbe la libreria client).
    """
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
        # IPv6: `Connection.hostname` perde le parentesi, l'URL le richiede.
        parts.append(f'[{host_text}]')
    else:
        parts.append(quote(host_text, safe=''))
    if connection.port:
        parts.append(f':{connection.port}')
    # `as_url` antepone sempre '/' e salta le parti vuote: vhost '/' → '//'.
    parts.extend(['/', str(connection.virtual_host or '')])
    url = ''.join(part for part in parts if part)

    if scheme in ('amqp', 'amqps', 'pyamqp'):
        extra = {
            # py-amqp negozia heartbeat 0 quando non specificato; lapin userebbe
            # 60 e, con il producer a riposo, RabbitMQ chiuderebbe la
            # connessione dopo 2 intervalli.
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
    """Una connessione Rust per processo e per URL, più la cache delle dichiarazioni."""

    __slots__ = ('connection', 'declared')

    def __init__(self, connection):
        self.connection = connection
        self.declared = set()


#: (url, opzioni) → _NativeEntry. Svuotata dopo fork: i socket del padre non
#: si usano nel figlio.
_entries = {}

#: Connessioni ereditate dal padre. Nel figlio non si chiudono (il runtime
#: tokio che le possiede non esiste più): si tengono vive per non far partire
#: i distruttori su uno stato incoerente.
_orphaned = []


def _native_entry(connection, key=None):
    """Connessione Rust condivisa per (URL, opzioni), creata al primo uso.

    Niente lock: `dict.setdefault` è atomico sotto GIL, e un lock rimasto
    acquisito da un thread scomparso renderebbe il figlio di `fork()` muto.
    Se due thread creano insieme, uno chiude la copia perdente.
    """
    if key is None:
        key = _native_key(connection)
    entry = _entries.get(key)
    if entry is not None:
        return entry
    options = dict(getattr(connection, 'transport_options', None) or {})
    # Il retry è gestito qui come `kombu.Connection.ensure`: la connessione
    # non deve ritentare per conto suo.
    fresh = _NativeEntry(_celery_rs.Connection(
        key[0], options, retry_policy={'enabled': False}))
    entry = _entries.setdefault(key, fresh)
    if entry is not fresh:
        try:
            fresh.connection.close()
        except Exception:  # copia perdente della corsa: chiusura best effort
            pass
        return entry
    _STATS['connections'] += 1
    return entry


def _discard_entry(connection, entry):
    """Toglie *entry* dalla cache e chiude la connessione (best effort)."""
    key = _native_key(connection)
    if _entries.get(key) is entry:
        _entries.pop(key, None)
    try:
        entry.connection.close()
    except Exception:  # la connessione è già persa: la chiusura è best effort
        pass


_fork_pid = os.getpid()


def after_fork():
    """Nel figlio scarta connessioni e runtime Rust ereditati dal padre."""
    global _fork_pid
    pid = os.getpid()
    if _fork_pid == pid:
        return
    _fork_pid = pid
    # Nel figlio resta un solo thread: niente lock (potrebbe essere rimasto
    # acquisito da un thread scomparso con il fork).
    if _entries:
        _orphaned.extend(_entries.values())
        _entries.clear()
    _STATS['forks'] += 1
    if _celery_rs is not None:
        # Il runtime tokio del padre non è utilizzabile nel figlio: il modulo
        # nativo lo ricrea al primo uso.
        _celery_rs.after_fork()


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=after_fork)


def _prepare_body(body, serializer, content_type, content_encoding):
    """Come ``kombu.Producer._prepare``, con JSON e compressione in Rust."""
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
        # Metodo registrato dall'utente in kombu.compression: non è nel core.
        from kombu.compression import compress as kombu_compress
        body, headers['compression'] = kombu_compress(body, compression)
    return body


class RsProducer(KombuProducer):
    """Sostituto di :class:`kombu.Producer` che pubblica dal core Rust.

    Mantiene ``connection``/``channel``/``exchange`` come il producer originale;
    solo ``publish`` (e le dichiarazioni) passano dal trasporto Rust.
    """

    def __init__(self, channel=None, exchange=None, routing_key=None,
                 serializer=None, auto_declare=None, compression=None,
                 on_return=None):
        # Non si chiama `kombu.Producer.__init__`: legherebbe l'exchange a una
        # ChannelPromise e aprirebbe un canale kombu al primo uso. Qui il canale
        # si crea solo se qualcuno accede a `.channel`.
        self._channel = channel
        self.__connection__ = channel if is_connection(channel) else None
        self.exchange = exchange if exchange is not None else Exchange('')
        self.routing_key = routing_key or ''
        self.serializer = serializer or None
        self.compression = compression or None
        self.on_return = on_return or None
        # (url, opzioni) e entry Rust: calcolate una volta per connessione,
        # non a ogni messaggio. `revive` le invalida.
        self._rs_key = None
        self._rs_entry = None
        if auto_declare is not None:
            self.auto_declare = auto_declare

    @property
    def connection(self):
        """Il ``kombu.Connection`` lazy associato al producer."""
        if self.__connection__ is not None:
            return self.__connection__
        channel = self._channel
        if channel is None:
            return None
        return getattr(getattr(channel, 'connection', None), 'client', None)

    @property
    def channel(self):
        """Canale kombu, creato solo su richiesta (es. backend ``rpc://``)."""
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
        """Aggiorna il canale (kombu lo chiama dopo una riconnessione)."""
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
            # Errore permanente, non di trasporto: non va ritentato.
            raise OperationalError('producer senza connessione')
        return connection, self._entry()

    def _delivery_details(self, exchange, delivery_mode=None):
        if isinstance(exchange, Exchange):
            return exchange.name, maybe_delivery_mode(
                delivery_mode or exchange.delivery_mode,
            )
        return exchange, maybe_delivery_mode(
            delivery_mode or self.exchange.delivery_mode,
        )

    # ---------------------------------------------------------------- dichiarazioni

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
        """Binding aggiuntivo coda → exchange (``Queue.bindings``)."""
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
            # binding exchange→exchange: il trasporto Rust non lo supporta.
            raise NotImplementedError(
                'binding tra exchange non supportato dal producer Rust')
        else:
            # Meglio un errore esplicito che una dichiarazione dimenticata.
            raise TypeError(
                f'entità non dichiarabile dal producer Rust: {type(entity)!r}')

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
        """Dichiara l'exchange di default (se ha un nome)."""
        if self.exchange and self.exchange.name:
            _, entry = self._native()
            self._declare_exchange(entry, self.exchange)

    def maybe_declare(self, entity, retry=False, **retry_policy):
        """Dichiara *entity* con la cache di connessione, come ``kombu.common.maybe_declare``."""
        if not entity:
            return None
        if retry:
            return self._ensure(
                lambda: self._maybe_declare(self._native()[1], entity),
                retry_policy,
            )
        return self._call(lambda: self._maybe_declare(self._native()[1], entity))

    # --------------------------------------------------------------------- publish

    def _call(self, fun):
        """Esegue *fun* e, su errore di connessione, scarta la connessione Rust.

        Vale anche senza retry: la connessione rotta non deve restare in cache
        (il publish successivo riconnette), come fa `obj.revive` in kombu.
        """
        try:
            return fun()
        except _RETRY_ERRORS:
            self._drop_native()
            raise

    def _ensure(self, fun, retry_policy):
        """Stessa semantica di ``kombu.Connection.ensure`` (retry lineare)."""
        policy = dict(retry_policy or {})
        errback = policy.pop('errback', None)
        max_retries = policy.pop('max_retries', None)
        interval_start = policy.pop('interval_start', 1)
        interval_step = policy.pop('interval_step', 1)
        interval_max = policy.pop('interval_max', 1)
        retries = 0
        while True:
            # `ensure` chiama l'errback solo per errori dopo una connessione
            # riuscita: un broker irraggiungibile non lo invoca.
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
        """Scarta la connessione Rust corrente: il prossimo tentativo riconnette."""
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
        """Fallback per le opzioni che il trasporto Rust non gestisce."""
        properties = dict(kwargs.pop('properties'))
        # `delivery_mode` arriva come argomento esplicito, `expiration` è già
        # dentro le properties (stringa di millisecondi).
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
        """Come :meth:`kombu.Producer.publish`, con serializzazione e I/O in Rust."""
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
        """Nessuna risorsa da rilasciare: la connessione è condivisa per processo."""

    close = release


class FastProducerPool(ProducerPool):
    """``kombu.pools.ProducerPool`` che crea :class:`RsProducer` senza socket kombu."""

    Producer = RsProducer

    def __init__(self, connection, limit=None, driver_type=None):
        super().__init__(connection_pools[connection], limit=limit)
        # Evita l'accesso a `connection.transport.driver_type` in send_task
        # (importerebbe la libreria client) per la decisione su ETA.
        self.driver_type = driver_type or driver_type_of(connection)

    def create_producer(self):
        # La connessione kombu resta solo un descrittore lazy (URL, opzioni);
        # l'I/O passa dalla connessione Rust condivisa.
        return self.Producer(self.connections.connection)

    def release(self, resource):
        # Non si tocca ``__connection__``: è il descrittore condiviso, non una
        # PooledConnection acquisita dal pool.
        Resource.release(self, resource)
