"""Queue argument conversion without importing ``kombu.transport``."""
from __future__ import annotations

__all__ = ('to_rabbitmq_queue_arguments',)

# Argument names and converters from RABBITMQ_QUEUE_ARGUMENTS.
_RABBITMQ_QUEUE_ARGUMENTS = {
    'expires': ('x-expires', lambda value: int(float(value) * 1000)),
    'message_ttl': ('x-message-ttl', lambda value: int(float(value) * 1000)),
    'max_length': ('x-max-length', int),
    'max_length_bytes': ('x-max-length-bytes', int),
    'max_priority': ('x-max-priority', int),
}


def to_rabbitmq_queue_arguments(arguments, **options):
    """Convert RabbitMQ queue arguments as kombu does."""
    prepared = {}
    for key, value in options.items():
        name, convert = _RABBITMQ_QUEUE_ARGUMENTS[key]
        if value is not None:
            prepared[name] = convert(value)
    return dict(arguments, **prepared) if prepared else arguments
