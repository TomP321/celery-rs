"""Worker Pidbox (remote control)."""
import socket
import threading

from kombu.common import ignore_errors
from kombu.utils.encoding import safe_str

from celery.utils.collections import AttributeDict
from celery.utils.functional import pass1
from celery.utils.log import get_logger

from . import control

__all__ = ('Pidbox', 'gPidbox')

logger = get_logger(__name__)
debug, error, info = logger.debug, logger.error, logger.info


class _RsNode:
    """Pidbox node that replies through the Rust producer."""

    hostname = None
    mailbox = None
    handlers = None
    state = None
    channel = None

    def __init__(self, hostname, state=None, channel=None, handlers=None,
                 mailbox=None):
        self.channel = channel
        self.mailbox = mailbox
        self.hostname = hostname
        self.state = state
        self.adjust_clock = mailbox.clock.adjust
        self.handlers = {} if handlers is None else handlers

    def handle_message(self, body, message=None):
        destination = body.get('destination')
        pattern = body.get('pattern')
        matcher = body.get('matcher')
        if message is not None:
            self.adjust_clock(message.headers.get('clock') or 0)
        hostname = self.hostname
        run_dispatch = False
        if destination:
            if hostname in destination:
                run_dispatch = True
        elif pattern and matcher:
            from kombu.matcher import match
            if match(hostname, pattern, matcher):
                run_dispatch = True
        else:
            run_dispatch = True
        if run_dispatch:
            return self.dispatch(**body)

    dispatch_from_message = handle_message

    def dispatch(self, method, arguments=None,
                 reply_to=None, ticket=None, **kwargs):
        arguments = arguments or {}
        debug('pidbox received method %s [reply_to:%s ticket:%s]',
              method, reply_to, ticket)
        handle = reply_to and self.handle_call or self.handle_cast
        try:
            reply = handle(method, arguments)
        except SystemExit:
            raise
        except Exception as exc:  # pylint: disable=broad-except
            error('pidbox command error: %r', exc, exc_info=1)
            reply = {'error': repr(exc)}

        if reply_to:
            self.reply({self.hostname: reply},
                       exchange=reply_to['exchange'],
                       routing_key=reply_to['routing_key'],
                       ticket=ticket)
        return reply

    def handle(self, method, arguments=None):
        arguments = {} if not arguments else arguments
        return self.handlers[method](self.state, **arguments)

    def handle_call(self, method, arguments):
        return self.handle(method, arguments)

    def handle_cast(self, method, arguments):
        return self.handle(method, arguments)

    def reply(self, data, exchange, routing_key, ticket, **kwargs):
        publish = getattr(self.channel, 'publish_reply', None)
        if publish is not None:
            publish(data, exchange, routing_key, ticket, self.mailbox.clock)
            return
        self.mailbox._publish_reply(  # pragma: no cover - kombu path
            data, exchange, routing_key, ticket,
            channel=self.channel, serializer=self.mailbox.serializer)


class Pidbox:
    """Worker mailbox."""

    consumer = None

    def __init__(self, c):
        from celery.worker import _rs_consumer

        self.c = c
        self.hostname = c.hostname
        state = AttributeDict(
            app=c.app,
            hostname=c.hostname,
            consumer=c,
            tset=pass1 if c.controller.use_eventloop else set,
        )
        if _rs_consumer.use_rs_worker(c):
            mailbox = c.app.control.mailbox
            self.node = _RsNode(
                safe_str(c.hostname), handlers=control.Panel.data,
                state=state, mailbox=mailbox)
        else:
            self.node = c.app.control.mailbox.Node(
                safe_str(c.hostname),
                handlers=control.Panel.data,
                state=state,
            )
        self._forward_clock = self.c.app.clock.forward

    def on_message(self, body, message):
        # just increase clock as clients usually don't
        # have a valid clock to adjust with.
        self._forward_clock()
        try:
            self.node.handle_message(body, message)
        except KeyError as exc:
            error('No such control command: %s', exc)
        except Exception as exc:  # pylint: disable=broad-except
            error('Control command error: %r', exc, exc_info=True)
            self.reset()

    def start(self, c):
        from celery.worker import _rs_consumer
        from celery.worker import _rs_mailbox

        if _rs_consumer.is_rs_connection(getattr(c, 'connection', None)):
            mailbox = self.node.mailbox
            self.consumer = _rs_mailbox.pidbox_consumer(
                c.connection, self.node, self.on_message,
                accept=mailbox.accept,
                on_decode_error=c.on_decode_error,
                expires=mailbox.queue_expires,
                message_ttl=mailbox.queue_ttl,
            )
            self.node.channel = self.consumer
            self.consumer.start()
            return
        self.node.channel = c.connection.channel()
        self.consumer = self.node.listen(callback=self.on_message)
        self.consumer.on_decode_error = c.on_decode_error

    def on_stop(self):
        pass

    def stop(self, c):
        self.on_stop()
        self.consumer = self._close_channel(c)

    def reset(self):
        self.stop(self.c)
        self.start(self.c)

    def _close_channel(self, c):
        consumer = self.consumer
        if consumer:
            from celery.worker._rs_mailbox import RsMailboxConsumer
            if isinstance(consumer, RsMailboxConsumer):
                ignore_errors(c, consumer.cancel)
                ignore_errors(c, consumer.close)
            else:
                ignore_errors(c, consumer.cancel)
            self.consumer = None
        if self.node and self.node.channel:
            from celery.worker._rs_mailbox import RsMailboxConsumer
            if isinstance(self.node.channel, RsMailboxConsumer):
                ignore_errors(c, self.node.channel.close)
            else:
                ignore_errors(c, self.node.channel.close)

    def shutdown(self, c):
        self.on_stop()
        if self.consumer:
            debug('Canceling broadcast consumer...')
            ignore_errors(c, self.consumer.cancel)
            self.consumer = None
        self.stop(self.c)


class gPidbox(Pidbox):
    """Worker pidbox (greenlet)."""

    _node_shutdown = None
    _node_stopped = None
    _resets = 0

    def start(self, c):
        c.pool.spawn_n(self.loop, c)

    def on_stop(self):
        if self._node_stopped:
            self._node_shutdown.set()
            debug('Waiting for broadcast thread to shutdown...')
            self._node_stopped.wait()
            self._node_stopped = self._node_shutdown = None

    def reset(self):
        self._resets += 1

    def _do_reset(self, c, connection):
        self._close_channel(c)
        self.node.channel = connection.channel()
        self.consumer = self.node.listen(callback=self.on_message)
        self.consumer.consume()

    def loop(self, c):
        resets = [self._resets]
        shutdown = self._node_shutdown = threading.Event()
        stopped = self._node_stopped = threading.Event()
        try:
            with c.connection_for_read() as connection:
                info('pidbox: Connected to %s.', connection.as_uri())
                self._do_reset(c, connection)
                while not shutdown.is_set() and c.connection:
                    if resets[0] < self._resets:
                        resets[0] += 1
                        self._do_reset(c, connection)
                    try:
                        connection.drain_events(timeout=1.0)
                    except socket.timeout:
                        pass
        finally:
            stopped.set()
