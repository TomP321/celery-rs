"""RPC result consumer backed by the Rust transport."""
from __future__ import annotations

import logging
import os
import socket
import time
import weakref

import kombu
from celery.utils.queue_arguments import to_rabbitmq_queue_arguments

from celery._rs_message import RsMessage
from celery.app import _rs_producer

from .rpc import ResultConsumer

try:  # The full package installs the extension; the reference may not.
    import _celery_rs
except ImportError:  # pragma: no cover - native extension unavailable
    _celery_rs = None

__all__ = ('RsMessage', 'RsResultConsumer', 'promote_to_rust',
           'use_rust_result_consumer')

# Environment switch to restore kombu consumption.
ENV_DISABLE = 'CELERY_RS_BACKEND'

# Use the reference logger for identical reconnect messages.
logger = logging.getLogger('celery.backends.rpc')

# Keep inherited connections alive to avoid destructors accessing the parent's absent tokio runtime.
_orphaned_connections = []


def _enabled():
    value = os.environ.get(ENV_DISABLE)
    if value is None:
        return True
    return value.strip().lower() not in ('0', 'false', 'no', 'off')


def _app_connection_is_default(app):
    """Return whether ``app.connection`` retains its original implementation."""
    from celery.app.base import Celery

    try:
        if 'connection' in vars(app):
            return False
        return getattr(type(app), 'connection', None) is Celery.connection
    except TypeError:  # pragma: no cover - custom app
        return False


def use_rust_result_consumer(consumer):
    """Return whether the consumer supports Rust transport (AMQP only)."""
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
    """Promote a supported result consumer in place, preserving object identity."""
    if type(consumer) is not ResultConsumer:
        return consumer
    if not use_rust_result_consumer(consumer):
        return consumer
    consumer.__class__ = RsResultConsumer
    return consumer


class _RsConsumerSet:
    """Provide kombu queue subscriptions through Rust ``ConsumerControl``."""

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
        """Defer subscription until the next transport drain."""

    def cancel(self):
        self.queues = []


class RsResultConsumer(ResultConsumer):
    """RPC result consumer with Rust transport I/O."""

    # Rust maps connection failures to ConnectionError; reconnect_on_error must still exclude socket.timeout.
    _connection_errors = (ConnectionError, OSError)

    _rs_connection = None
    _rs_declared = None
    _rs_config = None
    _rs_fork_registered = False
    # Successful reconnections for tests and diagnostics.
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
                # Match kombu's Connection.drain_events exception.
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

        # Resubscribe to the same queues on the new connection.
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
        # Recreate inherited connections lazily after _celery_rs.after_fork resets the runtime.
        self._consumer = None
        if self._rs_connection is not None:
            _orphaned_connections.append(self._rs_connection)
            self._rs_connection = None
        self._rs_declared = set()
        # Clear cached pid-based ids so parent and child do not compete for the same reply queue.
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
                        'RPC result consumer: broker connection not '
                        'supported by Rust transport')
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
