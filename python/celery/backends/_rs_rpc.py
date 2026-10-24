"""Result consumer ``rpc://`` sul trasporto Rust.

Il backend ``rpc://`` pubblica i risultati con il producer dell'app e li
consuma da una coda di risposta dedicata.  Questa implementazione sostituisce
il ``kombu.Consumer`` con ``_celery_rs.Connection``: niente py-amqp nel
processo client, ``basic.get`` per ``get_task_meta`` e riconnessione con la
stessa semantica del riferimento.

La logica di stato (``BaseResultConsumer``) resta quella di
``celery.backends.asynchronous``: qui si forniscono solo trasporto e messaggi
compatibili con ``kombu.Message``.
"""
from __future__ import annotations

import logging
import os
import socket
import time
import weakref

import kombu
from kombu.exceptions import DecodeError
from kombu.message import Message as KombuMessage
from kombu.serialization import (
    TRUSTED_CONTENT,
    _reraise_errors,
    registry as serializer_registry,
)
from kombu.transport.base import to_rabbitmq_queue_arguments
from kombu.utils.json import object_hook as json_object_hook

from celery.app import _rs_producer

from .rpc import ResultConsumer

try:  # il pacchetto completo lo installa sempre; il riferimento no.
    import _celery_rs
except ImportError:  # pragma: no cover - solo senza estensione nativa
    _celery_rs = None

__all__ = ('RsMessage', 'RsResultConsumer', 'promote_to_rust',
           'use_rust_result_consumer')

#: Variabile d'ambiente per tornare al consumer di kombu.
ENV_DISABLE = 'CELERY_RS_BACKEND'

#: Stesso logger del riferimento: i messaggi di riconnessione sono identici.
logger = logging.getLogger('celery.backends.rpc')

#: Connessioni ereditate dal padre: nel figlio non si chiudono (il runtime
#: tokio del padre non esiste più) e si tengono vive per evitare i distruttori.
_orphaned_connections = []


def _enabled():
    value = os.environ.get(ENV_DISABLE)
    if value is None:
        return True
    return value.strip().lower() not in ('0', 'false', 'no', 'off')


def _app_connection_is_default(app):
    """True se ``app.connection`` non è stato sostituito (es. da un mock)."""
    from celery.app.base import Celery

    try:
        if 'connection' in vars(app):
            return False
        return getattr(type(app), 'connection', None) is Celery.connection
    except TypeError:  # pragma: no cover - app esotica
        return False


def use_rust_result_consumer(consumer):
    """True se *consumer* può usare il trasporto Rust.

    Solo AMQP: ``rpc://`` su broker Redis resta sul consumer di kombu in
    questa fase.  I test upstream che sostituiscono ``Consumer`` o
    ``app.connection`` continuano sul ramo originale.
    """
    if not _enabled() or _celery_rs is None:
        return False
    if consumer.Consumer is not kombu.Consumer:
        return False
    if not _app_connection_is_default(consumer.app):
        return False
    connection = _rs_producer.fast_connection(consumer.app)
    if connection is None:
        return False
    return _rs_producer.connection_driver_type(connection) == 'amqp'


def promote_to_rust(consumer):
    """Promuove il ``ResultConsumer`` di base a :class:`RsResultConsumer`.

    Restituisce sempre lo stesso oggetto: la classe cambia solo quando il
    percorso veloce è disponibile, così i mock dei test restano intatti.
    """
    if type(consumer) is not ResultConsumer:
        return consumer
    if not use_rust_result_consumer(consumer):
        return consumer
    consumer.__class__ = RsResultConsumer
    return consumer


class _RsChannel:
    """Canale minimo per ``kombu.Message.ack``/``reject``.

    ``delivery_tag`` è il tag del trasporto Rust (per AMQP codifica anche il
    canale), quindi ack e reject non richiedono una connessione kombu.
    """

    __slots__ = ('connection', 'no_ack_consumers')

    def __init__(self, connection, no_ack=False):
        self.connection = connection
        self.no_ack_consumers = set() if no_ack else None

    def basic_ack(self, delivery_tag, multiple=False):
        self.connection.ack(delivery_tag)

    def basic_reject(self, delivery_tag, requeue=False):
        self.connection.reject(delivery_tag, requeue=requeue)


class RsMessage(KombuMessage):
    """``kombu.Message`` costruito da una consegna del trasporto Rust."""

    def __init__(self, consumer, raw, accept=None, no_ack=False):
        body, headers, properties, delivery_info, delivery_tag = raw
        super().__init__(
            body=body,
            channel=_RsChannel(consumer._rs_connection, no_ack),
            delivery_tag=delivery_tag,
            content_type=properties.get('content_type'),
            content_encoding=properties.get('content_encoding'),
            delivery_info=delivery_info,
            properties=properties,
            headers=headers,
            accept=accept,
        )

    def _decode(self):
        content_type = self.content_type or 'application/data'
        # Stesse regole di ``kombu.serialization.loads``: ContentDisallowed
        # prima di qualunque decodifica.
        if self.accept is not None:
            if (content_type not in TRUSTED_CONTENT
                    and content_type not in self.accept):
                raise serializer_registry._for_untrusted_content(
                    content_type, 'untrusted')
        elif content_type in serializer_registry._disabled_content_types:
            raise serializer_registry._for_untrusted_content(
                content_type, 'disabled')

        if (content_type == 'application/json' and self.body
                and _celery_rs is not None):
            with _reraise_errors(DecodeError):
                return _celery_rs.json_loads(self.body, json_object_hook)
        return super()._decode()


class _RsConsumerSet:
    """Sostituto di ``kombu.Consumer`` per code e sottoscrizioni.

    Le code sono quelle kombu (``Queue``): il nome è l'unica cosa che serve al
    trasporto Rust, che aggiorna il consumatore con ``ConsumerControl``.
    """

    __slots__ = ('queues',)

    def __init__(self, queues=()):
        self.queues = list(queues)

    def consuming_from(self, queue):
        name = queue.name
        return any(known.name == name for known in self.queues)

    def add_queue(self, queue):
        if not self.consuming_from(queue):
            self.queues.append(queue)

    def cancel_by_queue(self, queue_name):
        self.queues = [q for q in self.queues if q.name != queue_name]

    def consume(self):
        """La sottoscrizione avviene al prossimo ``drain`` del trasporto."""

    def cancel(self):
        self.queues = []


class RsResultConsumer(ResultConsumer):
    """``rpc.ResultConsumer`` con I/O sul trasporto Rust."""

    #: Il binding Rust traduce gli errori di connessione in ``ConnectionError``
    #: (sottoclasse di ``OSError``); ``socket.timeout`` resta escluso da
    #: ``reconnect_on_error``.
    _connection_errors = (ConnectionError, OSError)

    _rs_connection = None
    _rs_declared = None
    _rs_config = None
    _rs_fork_registered = False
    #: Riconnessioni riuscite (usato dai test e per la diagnostica).
    _rs_reconnects = 0

    def start(self, initial_task_id, no_ack=True, **kwargs):
        self._no_ack = no_ack
        self._rs_connect()
        initial_queue = self._create_binding(initial_task_id)
        self._declare(initial_queue)
        self._consumer = _RsConsumerSet([initial_queue])
        self._consumer.consume()

    def drain_events(self, timeout=None):
        if self._rs_connection is not None:
            with self.reconnect_on_error():
                return self._drain_events(timeout)
        elif timeout:
            time.sleep(timeout)

    def _drain_events(self, timeout):
        deliveries = self._rs_connection.drain(
            self._queue_names(), timeout=timeout, prefetch=0,
            no_ack=self._no_ack)
        if not deliveries:
            if timeout is not None:
                # Stessa eccezione di ``Connection.drain_events`` di kombu.
                raise socket.timeout()
            return None
        for raw in deliveries:
            message = self._make_message(raw, accept=self.accept)
            self.on_state_change(message.payload, message)

    def _reconnect(self):
        logger.warning(
            'RPC result consumer: connection lost, attempting to reconnect...',
            exc_info=True,
        )
        old_queues = []
        if self._consumer is not None:
            old_queues = list(self._consumer.queues)
            try:
                self._consumer.cancel()
            except Exception:
                logger.debug(
                    'RPC result consumer: error while cancelling stale '
                    'consumer during reconnect',
                    exc_info=True,
                )

        if self._rs_connection is not None:
            try:
                self._rs_connection.close()
            except Exception:
                logger.debug(
                    'RPC result consumer: error while closing stale '
                    'connection during reconnect',
                    exc_info=True,
                )
            self._rs_connection = None
            self._rs_declared = set()

        # Nuova connessione e ri-sottoscrizione delle stesse code.
        self._rs_connect()
        for queue in old_queues:
            self._declare(queue)
        self._consumer = _RsConsumerSet(old_queues)
        self._consumer.consume()
        self._rs_reconnects += 1
        logger.info('RPC result consumer: reconnected successfully.')

    def stop(self):
        try:
            if self._consumer is not None:
                self._consumer.cancel()
                self._consumer = None
        finally:
            connection = self._rs_connection
            self._rs_connection = None
            self._rs_declared = set()
            if connection is not None:
                connection.close()

    def on_after_fork(self):
        # La connessione e il runtime Rust del padre non sono usabili nel
        # figlio: si ricreano al primo uso (`_celery_rs.after_fork` ha già
        # scartato il runtime condiviso).
        self._consumer = None
        if self._rs_connection is not None:
            _orphaned_connections.append(self._rs_connection)
            self._rs_connection = None
        self._rs_declared = set()
        # `thread_oid`/`oid` sono generati con il pid ma restano cachati nel
        # figlio: senza azzerarli padre e figlio consumerebbero la stessa coda
        # e un risultato potrebbe finire al consumer sbagliato.
        self.backend.__dict__.pop('oid', None)
        local = getattr(self.app, '_local', None)
        if local is not None:
            local.__dict__.pop('oid', None)

    def consume_from(self, task_id):
        if self._consumer is None:
            return self.start(task_id)
        queue = self._create_binding(task_id)
        if not self._consumer.consuming_from(queue):
            self._declare(queue)
            self._consumer.add_queue(queue)
            self._consumer.consume()

    def cancel_for(self, task_id):
        if self._consumer:
            self._consumer.cancel_by_queue(self._create_binding(task_id).name)

    def _slurp_from_queue(self, task_id, accept, limit=1000, no_ack=False):
        binding = self._create_binding(task_id)
        connection = self._rs_connect()
        self._declare(binding)
        queue = binding.name
        for _ in range(limit):
            raw = connection.get(queue, no_ack=no_ack)
            if not raw:
                break
            yield self._make_message(raw, accept=accept, no_ack=no_ack)
        else:
            raise self.backend.BacklogLimitExceeded(task_id)

    # ------------------------------------------------------------------ interni

    def _queue_names(self):
        if self._consumer is None:
            return []
        return [queue.name for queue in self._consumer.queues]

    def _make_message(self, raw, accept=None, no_ack=None):
        if no_ack is None:
            no_ack = self._no_ack
        return RsMessage(self, raw, accept=accept, no_ack=no_ack)

    def _rs_connect(self):
        if self._rs_connection is None:
            if self._rs_config is None:
                connection = _rs_producer.fast_connection(self.app)
                if connection is None:
                    raise RuntimeError(
                        'RPC result consumer: connessione al broker non '
                        'supportata dal trasporto Rust')
                self._rs_config = (
                    _rs_producer._broker_url(connection),
                    dict(getattr(connection, 'transport_options', None) or {}),
                )
            url, options = self._rs_config
            self._rs_connection = _celery_rs.Connection(
                url, options, retry_policy={'enabled': False})
            self._rs_declared = set()
            self._register_after_fork()
        return self._rs_connection

    def _register_after_fork(self):
        if self._rs_fork_registered or not hasattr(os, 'register_at_fork'):
            return
        self._rs_fork_registered = True
        reference = weakref.ref(self)

        def _reset():
            consumer = reference()
            if consumer is not None:
                consumer.on_after_fork()

        os.register_at_fork(after_in_child=_reset)

    def _declare(self, binding):
        connection = self._rs_connect()
        if binding.name in self._rs_declared:
            return
        exchange = binding.exchange
        if exchange is not None and exchange.name:
            connection.declare_exchange(
                exchange.name, exchange.type or 'direct',
                durable=exchange.durable,
                auto_delete=exchange.auto_delete,
                passive=getattr(exchange, 'passive', False),
                arguments=exchange.arguments,
            )
        arguments = to_rabbitmq_queue_arguments(
            binding.queue_arguments or {},
            expires=binding.expires,
            message_ttl=binding.message_ttl,
            max_length=binding.max_length,
            max_length_bytes=binding.max_length_bytes,
            max_priority=binding.max_priority,
        )
        connection.declare_queue(
            binding.name,
            durable=binding.durable,
            auto_delete=binding.auto_delete,
            exclusive=binding.exclusive,
            passive=getattr(binding, 'passive', False),
            arguments=arguments or None,
        )
        if exchange is not None and exchange.name:
            connection.bind(
                binding.name, exchange.name, binding.routing_key or '',
                arguments=binding.binding_arguments,
            )
        self._rs_declared.add(binding.name)
