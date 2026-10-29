"""Rust mailbox consumers with Celery pidbox and gossip handlers."""
from __future__ import annotations

import logging

from kombu.exceptions import ContentDisallowed, DecodeError
from kombu.serialization import prepare_accept_content
from kombu.utils.json import JSONEncoder

from celery._rs_message import RsMessage

try:  # The full package installs the extension; the reference may not.
    import _celery_rs
except ImportError:  # pragma: no cover - native extension unavailable
    _celery_rs = None

__all__ = ('RsMailboxConsumer', 'pidbox_consumer', 'events_consumer')

logger = logging.getLogger('celery.worker.consumer')

# Kombu's JSON default reads its encoder registry on every call.
_JSON_DEFAULT = JSONEncoder().default

# Bound deliveries per loop iteration as the task consumer does.
DRAIN_BATCH = 1000


def _serialize(data, serializer):
    """Prepare messages as kombu does, with Rust JSON encoding."""
    if serializer in (None, 'json'):
        return _celery_rs.json_dumps(data, _JSON_DEFAULT), 'application/json', 'utf-8'
    from kombu.serialization import dumps
    content_type, content_encoding, payload = dumps(data, serializer=serializer)
    if isinstance(payload, str):
        payload = payload.encode(content_encoding)
    return payload, content_type, content_encoding


class RsMailboxConsumer:
    """Consume a service queue without acknowledgements, passing ``RsMessage`` objects to handlers."""

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
        """Start consumption at creation time."""
        return self.start()

    def cancel(self):
        worker = self._worker
        if worker is not None:
            try:
                worker.cancel()
            except Exception:  # The connection is already lost; ignore close errors.
                logger.debug('mailbox cancellation failed', exc_info=True)
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
            except Exception:  # Ignore close errors.
                logger.debug('mailbox close failed', exc_info=True)
        self._started = False

    release = close

    @property
    def channel(self):
        # ConsumerStep._close closes channels; this consumer owns its dedicated Rust connection.
        return self

    @property
    def consuming(self):
        return self._started


    def pending(self):
        worker = self._worker
        return worker.pending() if worker is not None else 0

    def take_error(self):
        worker = self._worker
        return worker.take_error() if worker is not None else None

    def drain(self, max_batch=DRAIN_BATCH):
        """Dispatch ready deliveries to handlers without waiting."""
        worker = self._worker
        if worker is None:
            return 0
        raws = worker.drain(max_batch)
        if raws:
            logger.debug('mailbox %s: %d deliveries', self.queue, len(raws))
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


    def publish_reply(self, data, exchange, routing_key, ticket, clock,
                      serializer=None):
        """Publish a mailbox reply through the Rust producer."""
        worker = self._worker
        if worker is None:
            raise ConnectionError('mailbox closed')
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
            # Match the reference exchange's transient delivery mode.
            delivery_mode=1,
            priority=0,
        )


def pidbox_consumer(connection, node, callback, accept=None,
                    on_decode_error=None, expires=None, message_ttl=None):
    """Consume the node's ``{hostname}.{namespace}.pidbox`` queue."""
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
    """Consume ``worker.#`` events from the node's event queue."""
    queue = f'{prefix}.{node_id}'

    def declared(worker):
        worker.declare_events(
            prefix, node_id, exchange, exchange_type, routing_key, expires,
            message_ttl, queue_exclusive, queue_durable)

    return RsMailboxConsumer(
        connection, queue, callback, accept=accept, declared=declared)
