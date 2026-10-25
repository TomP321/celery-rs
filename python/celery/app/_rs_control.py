"""Client remote-control con il trasporto Rust.

Replica :meth:`kombu.pidbox.Mailbox._broadcast` / ``_collect``: il comando
viene serializzato come in kombu (stesso JSON, stessi header) e pubblicato
sull'exchange ``*.pidbox``; le risposte si raccolgono dalla coda temporanea
``{oid}.reply.*.pidbox`` con il trasporto Rust, quindi senza aprire connessioni
kombu né importare redis-py/py-amqp.

Il percorso veloce è usato solo se il producer Rust è idoneo (stesse condizioni
e stessa variabile ``CELERY_RS_PRODUCER=0`` di :mod:`celery.app._rs_producer`) e
l'utente non ha passato ``connection=``/``channel=``: in quel caso si resta sul
mailbox di kombu. ``Control.mailbox`` resta un ``kombu.pidbox.Mailbox``
utilizzabile direttamente.
"""
from __future__ import annotations

import time

from kombu.exceptions import OperationalError
from kombu.transport.base import to_rabbitmq_queue_arguments
from kombu.utils.json import JSONEncoder
from kombu.utils.uuid import uuid

from celery.utils.log import get_logger

__all__ = ('broadcast', 'fast_connection')

logger = get_logger(__name__)

#: default del JSON di kombu: legge `kombu.utils.json._encoders` a ogni uso.
_JSON_DEFAULT = JSONEncoder().default


def fast_connection(app):
    """La ``FastConnection`` del broker, o ``None`` se non è idonea."""
    from . import _rs_producer
    return _rs_producer.fast_connection(app)


def _native(connection):
    """Connessione nativa condivisa per *connection*, con gli errori di kombu."""
    from . import _rs_producer
    try:
        return _rs_producer._native_entry(connection).connection
    except (ConnectionError, OSError) as exc:
        # `connection_or_acquire` di Celery solleva OperationalError.
        raise OperationalError(str(exc)) from exc


def _serialize(body, serializer):
    """``kombu.Producer._prepare`` con JSON nel core Rust."""
    if serializer in (None, 'json'):
        from _celery_rs import json_dumps
        return json_dumps(body, _JSON_DEFAULT), 'application/json', 'utf-8'
    from kombu.serialization import dumps
    content_type, content_encoding, payload = dumps(body, serializer=serializer)
    if isinstance(payload, str):
        payload = payload.encode(content_encoding)
    return payload, content_type, content_encoding


def _decode(payload, content_type, content_encoding, accept):
    """``Message.decode``: rispetta ``accept`` come il consumer di kombu."""
    if accept and content_type not in accept:
        raise ValueError(
            f'refusing to deserialize untrusted content of type {content_type}')
    if content_type == 'application/json':
        from _celery_rs import json_loads
        return json_loads(payload)
    from kombu.serialization import loads
    return loads(payload, content_type, content_encoding, accept=accept)


def _queue_arguments(queue):
    """Gli stessi argomenti che ``RsProducer._declare_queue`` manda al broker."""
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
    """``Control.broadcast`` sul trasporto Rust."""
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
        # Risposte di un ticket precedente già conservate (`unclaimed`).
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
            timeout,
            limit,
            reply_ticket,
        )
    except (ConnectionError, OSError) as exc:
        raise OperationalError(str(exc)) from exc

    responses = []
    accept = mailbox.accept
    if accept:
        from kombu.serialization import prepare_accept_content
        # `Mailbox.accept` contiene nomi di serializer; il consumer di kombu
        # li converte in content-type (`prepare_accept_content`).
        accept = prepare_accept_content(accept)
    for body, ctype, cencoding, reply_headers in replies:
        mailbox.clock.adjust(reply_headers.get('clock') or 0)
        try:
            decoded = _decode(body, ctype, cencoding, accept)
        except Exception as exc:  # noqa: BLE001 - come on_decode_error
            logger.error('Cannot decode message: %r', exc, exc_info=True)
            continue
        if callback:
            callback(decoded)
        responses.append(decoded)

    for ticket, body, ctype, cencoding, reply_headers in unclaimed:
        mailbox.clock.adjust(reply_headers.get('clock') or 0)
        try:
            decoded = _decode(body, ctype, cencoding, accept)
        except Exception as exc:  # noqa: BLE001 - come on_decode_error
            logger.error('Cannot decode message: %r', exc, exc_info=True)
            continue
        mailbox.unclaimed[ticket].append(decoded)

    if reply_ticket:
        return responses
