"""Worker <-> Worker communication Bootstep."""
from collections import defaultdict
from functools import partial
from heapq import heappush
from operator import itemgetter

from kombu import Consumer
from kombu.asynchronous.semaphore import DummyLock
from kombu.exceptions import ContentDisallowed, DecodeError

from celery import bootsteps, uuid
from celery.events.receiver import EventReceiver
from celery.utils.log import get_logger
from celery.utils.objects import Bunch

from .mingle import Mingle

__all__ = ('Gossip',)

logger = get_logger(__name__)
debug, info = logger.debug, logger.info


class _RsReceiver:
    """Provide ``event_from_message`` without an EventReceiver channel."""

    __slots__ = ('app', 'clock', 'adjust_clock', 'forward_clock')

    event_from_message = EventReceiver.event_from_message

    def __init__(self, app):
        self.app = app
        self.clock = app.clock
        self.adjust_clock = self.clock.adjust
        self.forward_clock = self.clock.forward


class Gossip(bootsteps.ConsumerStep):
    """Bootstep consuming events from other workers.

    This keeps the logical clock value up to date.
    """

    label = 'Gossip'
    requires = (Mingle,)
    _cons_stamp_fields = itemgetter(
        'id', 'clock', 'hostname', 'pid', 'topic', 'action', 'cver',
    )
    compatible_transports = {'amqp', 'redis'}

    def __init__(self, c, without_gossip=False,
                 interval=5.0, heartbeat_interval=2.0, **kwargs):
        self.enabled = not without_gossip and self._compatible(c)
        self.app = c.app
        c.gossip = self
        self.Receiver = c.app.events.Receiver
        self.hostname = c.hostname
        self.full_hostname = '.'.join([self.hostname, str(c.pid)])
        self.on = Bunch(
            node_join=set(),
            node_leave=set(),
            node_lost=set(),
        )

        self.timer = c.timer
        self._rs_consumer = None
        if self.enabled:
            self.state = c.app.events.State(
                on_node_join=self.on_node_join,
                on_node_leave=self.on_node_leave,
                max_tasks_in_memory=1,
            )
            if c.hub:
                c._mutex = DummyLock()
            self.update_state = self.state.event
        self.interval = interval
        self.heartbeat_interval = heartbeat_interval
        self._tref = None
        self.consensus_requests = defaultdict(list)
        self.consensus_replies = {}
        self.event_handlers = {
            'worker.elect': self.on_elect,
            'worker.elect.ack': self.on_elect_ack,
        }
        self.clock = c.app.clock

        self.election_handlers = {
            'task': self.call_task
        }

        super().__init__(c, **kwargs)

    def _compatible(self, c):
        """Check gossip support without opening a kombu transport on Rust."""
        from celery.worker import _rs_consumer
        if _rs_consumer.use_rs_worker(c):
            # Read driver_type from the scheme without importing broker clients.
            from celery.app._rs_producer import driver_type_of
            return driver_type_of(c.conninfo) in self.compatible_transports
        return self.compatible_transport(c.app)

    def compatible_transport(self, app):
        with app.connection_for_read() as conn:
            return conn.transport.driver_type in self.compatible_transports

    def election(self, id, topic, action=None):
        self.consensus_replies[id] = []
        self.dispatcher.send(
            'worker-elect',
            id=id, topic=topic, action=action, cver=1,
        )

    def call_task(self, task):
        try:
            self.app.signature(task).apply_async()
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception('Could not call task: %r', exc)

    def on_elect(self, event):
        try:
            (id_, clock, hostname, pid,
             topic, action, _) = self._cons_stamp_fields(event)
        except KeyError as exc:
            return logger.exception('election request missing field %s', exc)
        heappush(
            self.consensus_requests[id_],
            (clock, f'{hostname}.{pid}', topic, action),
        )
        self.dispatcher.send('worker-elect-ack', id=id_)

    def start(self, c):
        from celery.worker import _rs_consumer
        if _rs_consumer.is_rs_connection(getattr(c, 'connection', None)):
            self._start_rs(c)
            self.dispatcher = c.event_dispatcher
            return
        super().start(c)
        self.dispatcher = c.event_dispatcher


    def _start_rs(self, c):
        """Create Rust consumers with the original Python handlers."""
        from celery.worker import _rs_mailbox
        if self._rs_consumer is not None:
            self._rs_consumer.close()
        self.register_timer()
        driver = c.connection.driver_type
        exchange = c.app.conf.event_exchange
        exchange_type = 'fanout' if driver in {'redis', 'gcpubsub'} else 'topic'
        self._rs_receiver = _RsReceiver(c.app)
        self._rs_prepare = partial(
            self._rs_receiver.event_from_message, localize=False)
        consumer = _rs_mailbox.events_consumer(
            c.connection,
            c.app.conf.event_queue_prefix,
            uuid(),
            exchange,
            exchange_type,
            'worker.#',
            self._on_rs_message,
            accept={c.app.conf.event_serializer, 'json'},
            expires=c.app.conf.event_queue_expires,
            message_ttl=self.heartbeat_interval,
            queue_exclusive=c.app.conf.event_queue_exclusive,
            queue_durable=c.app.conf.event_queue_durable,
        )
        self._rs_consumer = consumer
        self.consumers = [consumer]
        consumer.start()

    def _on_rs_message(self, body, message):
        self.on_message(self._rs_prepare, message)

    def on_elect_ack(self, event):
        id = event['id']
        try:
            replies = self.consensus_replies[id]
        except KeyError:
            return  # not for us
        alive_workers = set(self.state.alive_workers())
        replies.append(event['hostname'])

        if len(replies) >= len(alive_workers):
            _, leader, topic, action = self.clock.sort_heap(
                self.consensus_requests[id],
            )
            if leader == self.full_hostname:
                info('I won the election %r', id)
                try:
                    handler = self.election_handlers[topic]
                except KeyError:
                    logger.exception('Unknown election topic %r', topic)
                else:
                    handler(action)
            else:
                info('node %s elected for %r', leader, id)
            self.consensus_requests.pop(id, None)
            self.consensus_replies.pop(id, None)

    def on_node_join(self, worker):
        debug('%s joined the party', worker.hostname)
        self._call_handlers(self.on.node_join, worker)

    def on_node_leave(self, worker):
        debug('%s left', worker.hostname)
        self._call_handlers(self.on.node_leave, worker)

    def on_node_lost(self, worker):
        info('missed heartbeat from %s', worker.hostname)
        self._call_handlers(self.on.node_lost, worker)

    def _call_handlers(self, handlers, *args, **kwargs):
        for handler in handlers:
            try:
                handler(*args, **kwargs)
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception(
                    'Ignored error from handler %r: %r', handler, exc)

    def register_timer(self):
        if self._tref is not None:
            self._tref.cancel()
        self._tref = self.timer.call_repeatedly(self.interval, self.periodic)

    def periodic(self):
        workers = self.state.workers
        dirty = set()
        for worker in workers.values():
            if not worker.alive:
                dirty.add(worker)
                self.on_node_lost(worker)
        for worker in dirty:
            workers.pop(worker.hostname, None)

    def get_consumers(self, channel):
        self.register_timer()
        ev = self.Receiver(channel, routing_key='worker.#',
                           queue_ttl=self.heartbeat_interval)
        return [Consumer(
            channel,
            queues=[ev.queue],
            on_message=partial(self.on_message, partial(ev.event_from_message, localize=False)),
            accept=ev.accept,
            no_ack=True
        )]

    def on_message(self, prepare, message):
        _type = message.delivery_info['routing_key']

        # For redis when `fanout_patterns=False` (See Issue #1882)
        if _type.split('.', 1)[0] == 'task':
            return
        try:
            handler = self.event_handlers[_type]
        except KeyError:
            pass
        else:
            return handler(message.payload)

        # proto2: hostname in header; proto1: in body
        hostname = (message.headers.get('hostname') or
                    message.payload['hostname'])
        if hostname != self.hostname:
            try:
                _, event = prepare(message.payload)
                self.update_state(event)
            except (DecodeError, ContentDisallowed, TypeError) as exc:
                logger.error(exc)
        else:
            self.clock.forward()
