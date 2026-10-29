"""Kombu messages shared by the Rust worker and RPC consumers."""
from __future__ import annotations

from kombu.exceptions import DecodeError
from kombu.message import Message as KombuMessage
from kombu.serialization import (
    TRUSTED_CONTENT,
    _reraise_errors,
    registry as serializer_registry,
)
from kombu.utils.json import object_hook as json_object_hook

try:  # The full package installs the extension; the reference may not.
    import _celery_rs
except ImportError:  # pragma: no cover - native extension unavailable
    _celery_rs = None

__all__ = ('RsChannel', 'RsMessage')


class RsChannel:
    """Acknowledge Rust deliveries without a kombu connection."""

    __slots__ = ('connection', 'no_ack_consumers')

    def __init__(self, connection, no_ack=False):
        self.connection = connection
        self.no_ack_consumers = set() if no_ack else None

    def basic_ack(self, delivery_tag, multiple=False):
        self.connection.ack(delivery_tag)

    def basic_reject(self, delivery_tag, requeue=False):
        self.connection.reject(delivery_tag, requeue=requeue)


class RsMessage(KombuMessage):
    """Build a kombu message from a Rust delivery."""

    def __init__(self, consumer, raw, accept=None, no_ack=False,
                 connection=None):
        body, headers, properties, delivery_info, delivery_tag = raw
        if connection is None:
            connection = consumer._rs_connection
        super().__init__(
            body=body,
            channel=RsChannel(connection, no_ack),
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
        # Match kombu: reject disallowed content before decoding.
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
