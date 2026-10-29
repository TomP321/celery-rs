"""Remote control over Rust transport with kombu-compatible messages and replies."""
from __future__ import annotations

import time

from kombu.exceptions import OperationalError
from celery.utils.queue_arguments import to_rabbitmq_queue_arguments
from kombu.utils.json import JSONEncoder
from kombu.utils.uuid import uuid

from celery.utils.log import get_logger

__all__ = ('broadcast', 'fast_connection')

logger = get_logger(__name__)

# Kombu's JSON default reads its encoder registry on every call.
_JSON_DEFAULT = JSONEncoder().default


def fast_connection(app):
    """Return the broker's ``FastConnection``, or ``None`` if unsupported."""
    from . import _rs_producer
    if (not _rs_producer._enabled() or _rs_producer._celery_rs is None
            or not _rs_producer._connection_not_overridden(app)):
        return None
    # Keep the descriptor for the pool lifetime, as app.pool does in the reference.
    pool = app.amqp.producer_pool
    if isinstance(pool, _rs_producer.FastProducerPool):
        connection = pool.connections.connection
        if _rs_producer.use_fast_producer(connection):
            return connection
    return None


def _native(connection):
    """Return the shared native connection with kombu error mapping."""
    from . import _rs_producer
    try:
        return _rs_producer._native_entry(connection).connection
    except (ConnectionError, OSError) as exc:
        # Celery's connection_or_acquire raises OperationalError.
        raise OperationalError(str(exc)) from exc


def _serialize(body, serializer):
    """Prepare messages as kombu does, with Rust JSON encoding."""
    if serializer in (None, 'json'):
        from _celery_rs import json_dumps
        return json_dumps(body, _JSON_DEFAULT), 'application/json', 'utf-8'
    from kombu.serialization import dumps
    content_type, content_encoding, payload = dumps(body, serializer=serializer)
    if isinstance(payload, str):
        payload = payload.encode(content_encoding)
    return payload, content_type, content_encoding


def _decode(payload, content_type, content_encoding, accept):
    """Decode a message, enforcing ``accept`` as kombu does."""
    if accept and content_type not in accept:
        raise ValueError(
            f'refusing to deserialize untrusted content of type {content_type}')
    if content_type == 'application/json':
        from _celery_rs import json_loads
        return json_loads(payload)
    from kombu.serialization import loads
    return loads(payload, content_type, content_encoding, accept=accept)


def _queue_arguments(queue):
    """Build queue arguments as ``RsProducer._declare_queue`` does."""
    return to_rabbitmq_queue_arguments(
        queue.queue_arguments or {},
        expires=queue.expires,
        message_ttl=queue.message_ttl,
        max_length=queue.max_length,
        max_length_bytes=queue.max_length_bytes,
        max_priority=queue.max_priority,
    )


def _reply_route(queue):
    return {
        'exchange': queue.exchange.name,
        'exchange_type': queue.exchange.type or 'direct',
        'name': queue.name,
        'routing_key': queue.routing_key or '',
        'durable': queue.durable,
        'exclusive': queue.exclusive,
        'auto_delete': queue.auto_delete,
        'arguments': _queue_arguments(queue),
    }


def broadcast(control, connection, command, arguments=None, destination=None,
              reply=False, timeout=1.0, limit=None, callback=None,
              pattern=None, matcher=None):
    """Broadcast a control command through Rust transport."""
    mailbox = control.mailbox
    if destination is not None and not isinstance(destination, (list, tuple)):
        raise ValueError(
            'destination must be a list/tuple not {}'.format(
                type(destination)))
    if (pattern is not None and not isinstance(pattern, str) and
            matcher is not None and not isinstance(matcher, str)):
        raise ValueError(
            'pattern and matcher must be '
            'strings not {}, {}'.format(type(pattern), type(matcher))
        )

    arguments = arguments or {}
    reply_ticket = reply and uuid() or None

    # Set reply limit to number of destinations (if specified)
    if limit is None and destination:
        limit = destination and len(destination) or None
    if limit is not None:
        limit = int(limit)

    if reply_ticket:
        # Use replies already retained for an earlier ticket.
        cached = mailbox.unclaimed.pop(reply_ticket, None)
        if cached is not None:
            if callback:
                for item in cached:
                    callback(item)
            return list(cached)

    message = {'method': command,
               'arguments': arguments,
               'destination': destination,
               'pattern': pattern,
               'matcher': matcher}
    reply_route = None
    if reply_ticket:
        queue = mailbox.get_reply_queue()
        message.update(ticket=reply_ticket,
                       reply_to={'exchange': mailbox.reply_exchange.name,
                                 'routing_key': queue.routing_key})
        reply_route = _reply_route(queue)

    serializer = mailbox.serializer
    payload, content_type, content_encoding = _serialize(message, serializer)
    headers = {'clock': mailbox.clock.forward(),
               'expires': time.time() + timeout if timeout else 0}

    try:
        replies, unclaimed = _native(connection).control_request(
            mailbox.exchange.name,
            mailbox.exchange.type or mailbox.type,
            payload,
            content_type,
            content_encoding,
            headers,
            reply_route,
            timeout if reply_ticket else 0.0,
            limit,
            reply_ticket,
        )
    except (ConnectionError, OSError) as exc:
        raise OperationalError(str(exc)) from exc

    responses = []
    accept = mailbox.accept
    if accept:
        from kombu.serialization import prepare_accept_content
        # Convert Mailbox.accept serializer names to content types as kombu does.
        accept = prepare_accept_content(accept)
    for body, ctype, cencoding, reply_headers in replies:
        mailbox.clock.adjust(reply_headers.get('clock') or 0)
        try:
            decoded = _decode(body, ctype, cencoding, accept)
        except Exception as exc:  # noqa: BLE001 - match on_decode_error
            logger.error('Cannot decode message: %r', exc, exc_info=True)
            continue
        if callback:
            callback(decoded)
        responses.append(decoded)

    for ticket, body, ctype, cencoding, reply_headers in unclaimed:
        mailbox.clock.adjust(reply_headers.get('clock') or 0)
        try:
            decoded = _decode(body, ctype, cencoding, accept)
        except Exception as exc:  # noqa: BLE001 - match on_decode_error
            logger.error('Cannot decode message: %r', exc, exc_info=True)
            continue
        mailbox.unclaimed[ticket].append(decoded)

    if reply_ticket:
        return responses
