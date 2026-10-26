"""Consumer del mailbox del worker (pidbox/gossip) sul trasporto Rust.

Sostituisce la connessione kombu che il master apriva solo per il remote
control e per gli eventi degli altri worker: exchange, coda, binding e consumo
``no_ack`` passano dal trasporto Rust, i messaggi arrivano al loop dal
``Waker`` condiviso e gli handler Python restano quelli del riferimento.

Le dichiarazioni (nomi, tipi fanout/topic, ``x-expires``/``x-message-ttl``)
sono costruite in Rust da :mod:`celery_worker::mailbox`: stessi byte sul
broker del worker di riferimento.
"""
from __future__ import annotations

import logging

from kombu.exceptions import ContentDisallowed, DecodeError
from kombu.serialization import prepare_accept_content
from kombu.utils.json import JSONEncoder

from celery._rs_message import RsMessage

try:  # il pacchetto completo lo installa sempre; il riferimento no.
    import _celery_rs
except ImportError:  # pragma: no cover - solo senza estensione nativa
    _celery_rs = None

__all__ = ('RsMailboxConsumer', 'pidbox_consumer', 'events_consumer')

logger = logging.getLogger('celery.worker.consumer')

#: default del JSON di kombu: legge `kombu.utils.json._encoders` a ogni uso.
_JSON_DEFAULT = JSONEncoder().default

#: Limite di consegne lette a ogni giro del loop (lo stesso del consumer).
DRAIN_BATCH = 1000


def _serialize(data, serializer):
    """``kombu.Producer._prepare`` con JSON nel core Rust."""
    if serializer in (None, 'json'):
        return _celery_rs.json_dumps(data, _JSON_DEFAULT), 'application/json', 'utf-8'
    from kombu.serialization import dumps
    content_type, content_encoding, payload = dumps(data, serializer=serializer)
    if isinstance(payload, str):
        payload = payload.encode(content_encoding)
    return payload, content_type, content_encoding


class RsMailboxConsumer:
    """Consumatore ``no_ack`` di una coda di servizio (pidbox o eventi).

    ``on_message(body, message)`` riceve il body già decodificato (per il
    pidbox) o il ``RsMessage`` con ``payload`` pigro (gossip); ``message`` è
    sempre un :class:`celery._rs_message.RsMessage`, con header e
    ``delivery_info`` come quelli di kombu.
    """

    __slots__ = (
        '_connection', '_worker', 'queue', '_callback', 'accept',
        '_on_decode_error', '_started', '_closed', '_declared',
    )

    def __init__(self, connection, queue, callback, accept=None,
                 on_decode_error=None, declared=None):
        self._connection = connection
        self._worker = None
        self.queue = queue
        self._callback = callback
        self.accept = prepare_accept_content(accept)
        self._on_decode_error = on_decode_error
        self._started = False
        self._closed = False
        self._declared = declared

    # ------------------------------------------------------------ ciclo di vita

    def start(self):
        if self._closed or self._started:
            return self
        worker = self._connection.new_worker()
        self._worker = worker
        if self._declared is not None:
            self._declared(worker)
        worker.consume_no_ack([self.queue])
        self._started = True
        self._connection.register_aux(self)
        return self

    def consume(self):
        """``Consumer.consume``: il consumer Rust parte alla creazione."""
        return self.start()

    def cancel(self):
        worker = self._worker
        if worker is not None:
            try:
                worker.cancel()
            except Exception:  # connessione già persa: best effort
                logger.debug('cancel del mailbox fallito', exc_info=True)
        self._started = False

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._connection.unregister_aux(self)
        worker, self._worker = self._worker, None
        if worker is not None:
            try:
                worker.close()
            except Exception:  # best effort in chiusura
                logger.debug('chiusura del mailbox fallita', exc_info=True)
        self._started = False

    release = close

    @property
    def channel(self):
        # `ConsumerStep._close` chiude i canali dei consumer: qui è il
        # consumer stesso a possedere la connessione Rust dedicata.
        return self

    @property
    def consuming(self):
        return self._started

    # ------------------------------------------------------------ messaggi

    def pending(self):
        worker = self._worker
        return worker.pending() if worker is not None else 0

    def take_error(self):
        worker = self._worker
        return worker.take_error() if worker is not None else None

    def drain(self, max_batch=DRAIN_BATCH):
        """Consegna gli handler le consegne già pronte (nessuna attesa)."""
        worker = self._worker
        if worker is None:
            return 0
        raws = worker.drain(max_batch)
        if raws:
            logger.debug('mailbox %s: %d consegne', self.queue, len(raws))
        count = 0
        for raw in raws:
            message = RsMessage(None, raw, accept=self.accept, no_ack=True,
                                connection=self)
            try:
                body = message.payload
            except (DecodeError, ContentDisallowed, TypeError, ValueError) as exc:
                self._decode_error(message, exc)
                continue
            self._callback(body, message)
            count += 1
        return count

    def _decode_error(self, message, exc):
        if self._on_decode_error is not None:
            self._on_decode_error(message, exc)
        else:
            logger.error('Cannot decode message: %r', exc, exc_info=True)

    # ------------------------------------------------------------ risposta pidbox

    def publish_reply(self, data, exchange, routing_key, ticket, clock,
                      serializer=None):
        """``Mailbox._publish_reply`` sul producer Rust."""
        worker = self._worker
        if worker is None:
            raise ConnectionError('mailbox chiuso')
        body, content_type, content_encoding = _serialize(data, serializer)
        headers = {'ticket': ticket, 'clock': clock.forward()}
        worker.declare_exchange(exchange, 'direct', durable=False,
                                auto_delete=False)
        worker.publish(
            body,
            exchange=exchange,
            routing_key=routing_key,
            headers=headers,
            content_type=content_type,
            content_encoding=content_encoding,
            # `Exchange(..., delivery_mode='transient')` del riferimento.
            delivery_mode=1,
            priority=0,
        )


def pidbox_consumer(connection, node, callback, accept=None,
                    on_decode_error=None, expires=None, message_ttl=None):
    """Consumer della coda ``{hostname}.{namespace}.pidbox`` del nodo."""
    mailbox = node.mailbox
    namespace = mailbox.namespace
    hostname = node.hostname

    def declared(worker):
        worker.declare_pidbox(hostname, namespace, expires, message_ttl)

    queue = f'{hostname}.{namespace}.pidbox'
    return RsMailboxConsumer(
        connection, queue, callback, accept=accept,
        on_decode_error=on_decode_error, declared=declared)


def events_consumer(connection, prefix, node_id, exchange, exchange_type,
                    routing_key, callback, accept=None, expires=None,
                    message_ttl=None, queue_exclusive=True, queue_durable=False):
    """Consumer ``worker.#`` della coda eventi del nodo."""
    queue = f'{prefix}.{node_id}'

    def declared(worker):
        worker.declare_events(
            prefix, node_id, exchange, exchange_type, routing_key, expires,
            message_ttl, queue_exclusive, queue_durable)

    return RsMailboxConsumer(
        connection, queue, callback, accept=accept, declared=declared)
