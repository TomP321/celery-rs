"""Rust broker consumer with Celery bootsteps, strategies, signals and timers."""
from __future__ import annotations

import json
import logging
import os
import time
from types import SimpleNamespace

from kombu.exceptions import OperationalError
from kombu.serialization import prepare_accept_content
from celery.utils.queue_arguments import to_rabbitmq_queue_arguments

from celery._rs_message import RsMessage
from celery.app import _rs_producer

try:  # The full package installs the extension; the reference may not.
    import _celery_rs
except ImportError:  # pragma: no cover - native extension unavailable
    _celery_rs = None

__all__ = ('RsConnection', 'RsTaskConsumer', 'rsloop', 'use_rs_worker',
           'use_rs_pool', 'connection', 'is_rs_connection', 'stats',
           'new_waker')

# Environment switch to restore kombu consumption.
ENV_DISABLE = 'CELERY_RS_WORKER'

# Optional JSON consumer diagnostics for tests.
ENV_STATE = 'CELERY_RS_WORKER_STATE'

# Match the reference consumer logger names.
logger = logging.getLogger('celery.worker.consumer')

# Counters let tests verify that consumption uses Rust.
_STATS = {
    'connections': 0,
    'consumers': 0,
    'messages': 0,
    'reconnects': 0,
    'queue_adds': 0,
    'queue_cancels': 0,
    'prefetch_updates': 0,
    'closes': 0,
}


def stats():
    """Copy internal Rust consumer counters."""
    return dict(_STATS)


def _enabled():
    value = os.environ.get(ENV_DISABLE)
    if value is None:
        return True
    return value.strip().lower() not in ('0', 'false', 'no', 'off')


def new_waker():
    """Create a ``_celery_rs.Waker`` for the Rust prefork pool."""
    return _celery_rs.Waker()


def _write_state():
    path = os.environ.get(ENV_STATE)
    if not path:
        return
    import sys
    prefixes = ('kombu', 'amqp', 'billiard', 'click', 'redis', 'yaml',
                'msgpack', 'greenlet', 'cffi', 'ssl', 'multiprocessing',
                'importlib.metadata', 'email', 'decimal', 'dateutil',
                'tzlocal', 'pygments', 'celery.worker', 'celery.events')
    payload = {
        'pid': os.getpid(), 'consumer': 'rust', 'modules': len(sys.modules),
        'loaded': sorted(m for m in sys.modules
                         if any(m == p or m.startswith(p + '.')
                                for p in prefixes)),
    }
    payload.update(_STATS)
    try:
        temporary = f'{path}.{os.getpid()}.tmp'
        with open(temporary, 'w', encoding='utf-8') as handle:
            json.dump(payload, handle)
        os.replace(temporary, path)
    except OSError:  # Diagnostics must never stop the worker.
        pass


def _pool_is_solo(consumer):
    from celery.concurrency.solo import TaskPool
    return isinstance(getattr(consumer, 'pool', None), TaskPool)


def _pool_is_rs_prefork(consumer):
    try:
        from celery.concurrency.rs_prefork import RsTaskPool
    except ImportError:  # pragma: no cover - native extension unavailable
        return False
    return isinstance(getattr(consumer, 'pool', None), RsTaskPool)


def _pool_is_rs(consumer):
    """Return whether the pool uses the Rust consumer."""
    return _pool_is_solo(consumer) or _pool_is_rs_prefork(consumer)


def transport_is_eligible(connection):
    """Check transport support using ``use_fast_producer`` rules."""
    scheme = getattr(connection, 'transport_cls', None)
    if not _rs_producer._scheme_is_fast(scheme):
        return False
    if getattr(connection, 'alt', None):
        return False
    if not _rs_producer._ssl_is_fast(
            scheme.lower(), getattr(connection, 'ssl', None)):
        return False
    if getattr(connection, 'login_method', None):
        return False
    driver = _rs_producer._SCHEMES[scheme.lower()]
    options = getattr(connection, 'transport_options', None) or {}
    return _rs_producer._options_are_fast(driver, options)


def use_rs_worker(consumer):
    """Return whether the consumer can use Rust."""
    if not _enabled() or _celery_rs is None:
        return False
    # worker_disable_prefetch modifies kombu channel QoS, so it requires the kombu consumer.
    if consumer.app.conf.worker_disable_prefetch:
        return False
    if not _pool_is_rs(consumer):
        return False
    # Keep unsupported transports such as memory:// on the reference path.
    conninfo = getattr(consumer, 'conninfo', None)
    return conninfo is None or transport_is_eligible(conninfo)


def use_rs_pool(worker):
    """Return whether the default prefork pool can use ``RsTaskPool`` with the asynchronous Rust consumer."""
    if not _enabled() or _celery_rs is None:
        return False
    if not getattr(worker, 'use_eventloop', False):
        return False
    if worker.app.conf.worker_disable_prefetch:
        return False
    import sys
    prefork = sys.modules.get('celery.concurrency.prefork')
    # A resolved prefork pool requires its module to be loaded already.
    if prefork is None or getattr(worker, 'pool_cls', None) is not prefork.TaskPool:
        return False
    try:
        from celery.app import _rs_producer
        connection = (
            _rs_producer.fast_connection(
                worker.app, worker.app.conf.broker_read_url)
            or worker.app.connection_for_read())
        return transport_is_eligible(connection)
    except Exception:  # Keep the reference path for invalid configuration.
        return False


def is_rs_connection(connection):
    return isinstance(connection, RsConnection)


def is_fast_connection(connection):
    return isinstance(connection, _rs_producer.FastConnection)


class RsTransport:
    """Transport attributes used by the consumer and bootsteps."""

    __slots__ = ('_rs',)

    def __init__(self, rs):
        self._rs = rs

    @property
    def driver_type(self):
        return self._rs.driver_type

    @property
    def implements(self):
        # The Rust consumer supplies asynchrony, not the kombu transport.
        return SimpleNamespace(asynchronous=False, heartbeats=False)

    def register_with_event_loop(self, connection, hub):
        self._rs.register_with_event_loop(hub)


class _RsChannel:
    """Handle QoS in Rust and open a kombu channel on demand for other operations."""

    __slots__ = ('_rs', '_channel')

    def __init__(self, rs, channel=None):
        self._rs = rs
        self._channel = channel

    def basic_qos(self, prefetch_size=0, prefetch_count=0, apply_global=False):
        self._rs.set_default_prefetch(prefetch_count)

    def __getattr__(self, name):
        channel = self._channel
        if channel is None:
            channel = self._rs.kombu_connection().default_channel
            self._channel = channel
        return getattr(channel, name)


class RsConnection:
    """Provide the kombu connection interface used by consumers and bootsteps."""

    def __init__(self, consumer, conninfo, url, options, waker=None):
        self.app = consumer.app
        self._conninfo = conninfo
        self._url = url
        self._options = dict(options or {})
        self.driver_type = _rs_producer._SCHEMES[
            (conninfo.transport_cls or '').lower()]
        self.connection_errors = conninfo.connection_errors
        self.channel_errors = conninfo.channel_errors
        self._worker = None
        # Share the pool Waker so the loop registers one descriptor.
        self._waker = waker or _celery_rs.Waker()
        self._default_prefetch = 0
        self._hub = None
        self._aux = []
        self._kombu_open = False
        self._closed = False
        # bound_open_broker_sockets reads the private attribute.
        self._connection = None
        self.transport = RsTransport(self)


    def connect(self):
        self._connect()
        return self

    def _connect(self):
        if self._worker is not None:
            return self._worker
        if self._closed:
            raise ConnectionError('connection closed')
        worker = _celery_rs.WorkerConsumer(
            self._url, self._waker, self._options,
            retry_policy={'enabled': False})
        self._worker = worker
        _STATS['connections'] += 1
        if self._default_prefetch:
            worker.set_prefetch(self._default_prefetch)
        _write_state()
        return worker

    def worker(self):
        """Create the Rust ``WorkerConsumer`` on first use."""
        return self._worker if self._worker is not None else self._connect()

    def ensure_connection(self, errback=None, max_retries=None,
                          interval_start=2, interval_step=2,
                          interval_max=30, callback=None, **kwargs):
        """Ensure a connection with kombu-compatible linear retry."""
        retries = 0
        while True:
            if callback is not None:
                callback()
            try:
                self._connect()
                return self
            except OSError as exc:
                if max_retries is not None and retries >= max_retries:
                    raise OperationalError(str(exc)) from exc
                interval = min(interval_start + interval_step * retries,
                               interval_max)
                if errback is not None:
                    errback(exc, interval)
                if interval:
                    time.sleep(interval)
                retries += 1

    def register_with_event_loop(self, hub):
        self._hub = hub
        if self._kombu_open:
            self._register_kombu(hub)

    def _register_kombu(self, hub):
        try:
            self._conninfo.transport.register_with_event_loop(
                self._conninfo.connection, hub)
        except Exception as exc:  # pragma: no cover - custom transport
            logger.debug('failed to register kombu connection with hub: %r',
                         exc)

    def kombu_connection(self):
        """Open a kombu connection on demand for pidbox, mingle or gossip."""
        if not self._kombu_open:
            self._conninfo.connect()
            self._kombu_open = True
            if self._hub is not None:
                self._register_kombu(self._hub)
        return self._conninfo

    def kombu_channel(self):
        return self.kombu_connection().default_channel

    @property
    def waker(self):
        """Return the ``_celery_rs.Waker`` shared with the pool."""
        return self._waker

    @property
    def default_channel(self):
        # Only basic_qos stays in Rust; other operations open the kombu channel on demand.
        return _RsChannel(self)

    def channel(self):
        """Open a new kombu channel for the reference consumer path."""
        return _RsChannel(self, self.kombu_connection().channel())


    def new_worker(self):
        """Create an auxiliary Rust consumer sharing the task connection."""
        if self._worker is None and not self._closed:
            # Auxiliary consumers may start before tasks, so they must create the shared connection.
            self._connect()
        if self._worker is not None:
            return self._worker.aux_consumer()
        return _celery_rs.WorkerConsumer(
            self._url, self._waker, self._options,
            retry_policy={'enabled': False})

    def register_aux(self, consumer):
        """Register an auxiliary consumer to drain in the master loop."""
        if consumer not in self._aux:
            self._aux.append(consumer)

    def unregister_aux(self, consumer):
        try:
            self._aux.remove(consumer)
        except ValueError:
            pass

    def drain_aux(self):
        """Dispatch ready pidbox and gossip messages to their handlers."""
        for consumer in tuple(self._aux):
            consumer.drain()

    def set_default_prefetch(self, prefetch_count):
        logger.debug('RsConnection: initial prefetch %s (worker=%r)',
                     prefetch_count, self._worker is not None)
        self._default_prefetch = prefetch_count
        if self._worker is not None:
            self._worker.set_prefetch(prefetch_count)
            _STATS['prefetch_updates'] += 1


    def drain_events(self, timeout=None):
        """Drain the lazy kombu connection."""
        return self.kombu_connection().drain_events(timeout=timeout)

    def ack(self, delivery_tag):
        """Acknowledge a Rust consumer delivery."""
        return self.worker().ack(delivery_tag)

    def reject(self, delivery_tag, requeue=False):
        """Reject or requeue a Rust consumer delivery."""
        return self.worker().reject(delivery_tag, requeue=requeue)

    def wait(self, timeout=None):
        """Wait for a delivery or deadline with the GIL released."""
        return self._waker.wait(timeout)

    def get_heartbeat_interval(self):
        # The Rust lapin runtime handles AMQP heartbeats.
        return 0.0

    @property
    def supports_heartbeats(self):
        return False

    def heartbeat_check(self, rate=2):
        return None

    @property
    def qos_semantics_matches_spec(self):
        # RabbitMQ 4.x and virtual transports use per-consumer QoS.
        return True

    @property
    def recoverable_connection_errors(self):
        return self._conninfo.recoverable_connection_errors

    @property
    def recoverable_channel_errors(self):
        return self._conninfo.recoverable_channel_errors

    def as_uri(self):
        return self._conninfo.as_uri()

    def info(self):
        return dict(self._conninfo.info())

    def collect(self, socket_timeout=None):
        if self._kombu_open:
            try:
                self._conninfo.collect(socket_timeout=socket_timeout)
            except Exception:  # The connection is already broken; ignore close errors.
                pass
            self._kombu_open = False

    @property
    def connected(self):
        return not self._closed

    def close(self):
        if self._closed:
            return
        self._closed = True
        worker, self._worker = self._worker, None
        try:
            if worker is not None:
                worker.close()
                _STATS['closes'] += 1
                _write_state()
        finally:
            if self._kombu_open:
                try:
                    self._conninfo.close()
                except Exception:  # Ignore close errors.
                    pass
                self._kombu_open = False

    release = close

    def __bool__(self):
        return True

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


class RsTaskConsumer:
    """Provide the kombu consumer interface used by the Tasks bootstep."""

    def __init__(self, channel, queues=None, accept=None,
                 on_decode_error=None, **kwargs):
        self._rs = channel
        self._queues = list(queues or ())
        self.accept = prepare_accept_content(accept)
        self.on_decode_error = on_decode_error
        self.on_message = None
        self._consuming = False
        self._active = set()
        self._declared = set()
        for queue in self._queues:
            self._declare(queue)


    @property
    def queues(self):
        return list(self._queues)

    @property
    def channel(self):
        return self._rs.kombu_channel()

    def consuming_from(self, queue):
        name = getattr(queue, 'name', queue)
        return any(known.name == name for known in self._queues)

    def add_queue(self, queue):
        queue = queue(self._rs.default_channel)
        if not self.consuming_from(queue):
            self._queues.append(queue)
        self._declare(queue)
        if self._consuming and queue.name not in self._active:
            self._rs.worker().add_queue(queue.name)
            self._active.add(queue.name)
            _STATS['queue_adds'] += 1
            _write_state()
        return queue

    def cancel_by_queue(self, queue_name):
        self._queues = [q for q in self._queues if q.name != queue_name]
        self._active.discard(queue_name)
        if self._consuming:
            self._rs.worker().cancel_queue(queue_name)
            _STATS['queue_cancels'] += 1
            _write_state()

    def consume(self, no_ack=None):
        names = [queue.name for queue in self._queues]
        worker = self._rs.worker()
        if not self._consuming:
            # Apply initial prefetch once; later add_consumer calls must preserve QoS increments for ETA tasks.
            if self._rs._default_prefetch:
                worker.set_prefetch(self._rs._default_prefetch)
            worker.consume(names)
            self._consuming = True
            self._active = set(names)
            _STATS['consumers'] += 1
            _write_state()
            return
        for name in names:
            if name not in self._active:
                worker.add_queue(name)
                self._active.add(name)
                _STATS['queue_adds'] += 1
                _write_state()

    def qos(self, prefetch_count=0, apply_global=False):
        worker = self._rs._worker
        if worker is None:
            self._rs._default_prefetch = prefetch_count
            return
        worker.set_prefetch(prefetch_count)
        _STATS['prefetch_updates'] += 1
        _write_state()

    def cancel(self):
        """Cancel consumption, keeping the connection open for acknowledgements."""
        if self._rs._worker is not None:
            self._rs._worker.cancel()
        self._consuming = False
        self._active.clear()

    def close(self):
        self.cancel()


    def drain_messages(self, max_batch=1000):
        """Return ready deliveries as ``RsMessage`` objects without waiting."""
        worker = self._rs._worker
        if worker is None:
            return []
        raws = worker.drain(max_batch)
        if not raws:
            return []
        _STATS['messages'] += len(raws)
        _write_state()
        return [RsMessage(None, raw, accept=self.accept,
                          connection=self._rs) for raw in raws]

    def pending(self):
        worker = self._rs._worker
        return worker.pending() if worker is not None else 0

    def take_error(self):
        worker = self._rs._worker
        return worker.take_error() if worker is not None else None


    def _declare(self, queue):
        if queue is None or queue.name in self._declared:
            return
        if not getattr(queue, 'no_declare', False):
            exchange = queue.exchange
            if exchange is not None and exchange.name:
                self._rs.worker().declare_exchange(
                    exchange.name, exchange.type or 'direct',
                    durable=exchange.durable,
                    auto_delete=exchange.auto_delete,
                    passive=getattr(exchange, 'passive', False),
                    arguments=exchange.arguments,
                )
            arguments = to_rabbitmq_queue_arguments(
                queue.queue_arguments or {},
                expires=queue.expires,
                message_ttl=queue.message_ttl,
                max_length=queue.max_length,
                max_length_bytes=queue.max_length_bytes,
                max_priority=queue.max_priority,
            )
            self._rs.worker().declare_queue(
                queue.name,
                durable=queue.durable,
                auto_delete=queue.auto_delete,
                exclusive=queue.exclusive,
                passive=getattr(queue, 'passive', False),
                arguments=arguments or None,
            )
            if exchange is not None and exchange.name:
                self._rs.worker().bind(
                    queue.name, exchange.name, queue.routing_key or '',
                    arguments=queue.binding_arguments,
                )
            for extra in getattr(queue, 'bindings', ()) or ():
                extra_exchange = extra.exchange
                if extra_exchange is None:
                    continue
                if extra_exchange.name:
                    self._rs.worker().declare_exchange(
                        extra_exchange.name, extra_exchange.type or 'direct',
                        durable=extra_exchange.durable,
                        auto_delete=extra_exchange.auto_delete,
                        passive=getattr(extra_exchange, 'passive', False),
                        arguments=extra_exchange.arguments,
                    )
                    self._rs.worker().bind(
                        queue.name, extra_exchange.name,
                        extra.routing_key or '',
                        arguments=extra.arguments,
                    )
        self._declared.add(queue.name)


def connection(consumer, heartbeat=None):
    """Return an ``RsConnection``, or ``None`` for unsupported transports."""
    from celery.app import _rs_producer
    # The descriptor only needs URL, options and error classes; avoid loading the transport.
    conninfo = (
        _rs_producer.fast_connection(
            consumer.app, consumer.app.conf.broker_read_url)
        or consumer.app.connection_for_read(heartbeat=heartbeat))
    if not transport_is_eligible(conninfo):
        return None
    url = _rs_producer._broker_url(conninfo)
    options = dict(getattr(conninfo, 'transport_options', None) or {})
    if (getattr(consumer, 'restart_count', 0) > 0
            and conninfo.transport_cls in ('redis', 'rediss')):
        import sys
        backend = sys.modules.get('celery.backends._rs_redis')
        if backend is not None:
            backend._reset_on_broker_restart(consumer.app, url)
        if _pool_is_rs_prefork(consumer):
            consumer.pool.reset_backend(url)
    if getattr(consumer, 'restart_count', 0) > 0:
        _STATS['reconnects'] += 1
        _write_state()
    # Keep the pool's existing Waker across consumer reconnections because the hub already registers it.
    waker = getattr(getattr(consumer, 'controller', None), '_rs_waker', None)
    return RsConnection(consumer, conninfo, url, options, waker=waker)



def _enable_heartbeats(timer, connection, rate):
    """Let the Rust runtime handle AMQP heartbeats on Rust connections."""
    if connection is None or is_rs_connection(connection) or is_fast_connection(connection):
        return [None]
    from celery.worker import loops
    return loops._enable_amqheartbeats(timer, connection, rate=rate)


def _hub_tick(hub):
    """Run ready callbacks, timers and hub I/O without blocking when the Waker has no descriptor."""
    poll = hub.poller.poll

    def _poll_now(_timeout):
        return poll(0)

    hub.poller.poll = _poll_now
    try:
        next(hub.create_loop())
    except StopIteration:
        pass
    finally:
        hub.poller.poll = poll


def _run_ready(hub):
    """Run ready callbacks immediately so acknowledgements precede shutdown and reduce prefetch latency."""
    for callback in hub._pop_ready():
        if callback:
            callback()


def _process_pool(obj):
    """Dispatch ready Rust pool events to task callbacks."""
    pool = getattr(obj, 'pool', None)
    process = getattr(pool, 'process_events', None)
    if process is not None:
        process()


def _run_pool_ready(obj, hub):
    _process_pool(obj)
    _run_ready(hub)


def _pool_fatal_exitcode(obj):
    """Return the exit code if the pool has lost its zygote."""
    pool = getattr(obj, 'pool', None)
    return getattr(pool, 'fatal_exitcode', None)


def _install_sentinel(hub):
    """Register a sentinel descriptor to make ``create_loop`` call ``poll(0)``."""
    read_fd, write_fd = os.pipe()
    try:
        os.set_blocking(read_fd, False)
        os.set_blocking(write_fd, False)
        hub.add_reader(read_fd, lambda: None)
    except Exception:  # pragma: no cover - unsupported platform
        os.close(read_fd)
        os.close(write_fd)
        return None
    return read_fd, write_fd


def _remove_sentinel(hub, sentinel):
    if sentinel is None:
        return
    read_fd, write_fd = sentinel
    try:
        hub.remove(read_fd)
    except Exception:  # hub.reset() may have removed the descriptor already.
        pass
    for fd in (read_fd, write_fd):
        try:
            os.close(fd)
        except OSError:
            pass


# Maximum fallback wait when the Waker has no descriptor.
HUB_POLL_INTERVAL = 0.25


def _next_timeout(hub, maximum=HUB_POLL_INTERVAL):
    """Return seconds until the next kombu timer deadline, capped at 0.25 seconds."""
    timer = getattr(hub, 'timer', None)
    queue = getattr(timer, '_queue', None) if timer is not None else None
    if queue:
        delay = queue[0][0] - time.monotonic()
        maximum = min(maximum, delay)
    return max(0.0, maximum)


def _register_waker(hub, waker):
    """Register the Waker descriptor with the hub poller, or return ``None`` if unavailable."""
    fd = waker.fileno() if waker is not None else -1
    if fd < 0:
        return None
    hub.add_reader(fd, waker.drain)
    return fd


def _unregister_waker(hub, fd):
    if fd is None:
        return
    try:
        hub.remove(fd)
    except Exception:  # hub.reset() may have removed the descriptor already.
        pass


def rsloop(obj, connection, consumer, blueprint, hub, qos, heartbeat, clock,
           hbrate=2.0):
    """Run the Celery worker loop using the shared Rust Waker and the hub timer deadlines."""
    from celery import bootsteps
    from celery.exceptions import WorkerLostError
    from celery.worker import loops, state

    RUN = bootsteps.RUN
    update_qos = qos.update
    on_task_received = obj.create_task_handler()

    timer_entries_before = obj.timer_entries_before_start
    if timer_entries_before is None:
        timer_entries_before = loops._timer_entries(hub.timer)

    heartbeat_error = _enable_heartbeats(hub.timer, connection, hbrate)
    dispatcher_heartbeat_error = [None]
    if obj.event_dispatcher:
        dispatcher_heartbeat_error = _enable_heartbeats(
            hub.timer, obj.event_dispatcher.connection, hbrate)

    consumer.on_message = on_task_received
    obj.controller.register_with_event_loop(hub)
    obj.register_with_event_loop(hub)
    registered_timer_entries = (
        loops._timer_entries(hub.timer) - timer_entries_before)
    obj.timer_entries_registered = registered_timer_entries
    consumer.consume()
    obj.on_ready()
    # Return free bootstrap pages before the loop without collecting user objects or changing GC settings.
    _celery_rs._release_task_memory(force=True)

    if not obj.restart_count and not obj.pool.did_start_ok():
        raise WorkerLostError('Could not start worker processes')

    hub.propagate_errors = connection.connection_errors
    waker = getattr(connection, 'waker', None)
    waker_fd = _register_waker(hub, waker)
    pool_waker = getattr(getattr(obj, 'pool', None), 'waker', None)
    logger.debug('rsloop: pool=%s waker=%s pool_waker=%s fd=%s',
                 type(getattr(obj, 'pool', None)).__name__,
                 id(waker), id(pool_waker), waker_fd)
    sentinel = _install_sentinel(hub) if waker_fd is None else None
    loop = hub.create_loop()
    try:
        while blueprint.state == RUN and obj.connection:
            state.maybe_shutdown()
            if heartbeat_error[0] is not None:
                raise heartbeat_error[0]
            if dispatcher_heartbeat_error[0] is not None:
                raise dispatcher_heartbeat_error[0]

            # Match asynloop: batch QoS updates and prioritize control commands.
            if qos.prev != qos.value:
                update_qos()

            # Process pool events before deliveries because callbacks may queue hub acknowledgements.
            _run_pool_ready(obj, hub)

            # Pidbox and gossip share the Waker; control commands and peer events also wake this poll.
            drain_aux = getattr(connection, 'drain_aux', None)
            if drain_aux is not None:
                drain_aux()

            deliveries = consumer.drain_messages()
            if deliveries:
                logger.debug('rsloop: %d deliveries from Rust consumer',
                             len(deliveries))
            for message in deliveries:
                try:
                    consumer.on_message(message)
                finally:
                    # Send acknowledgements before any shutdown requested during task execution.
                    _run_ready(hub)
            if deliveries:
                _run_pool_ready(obj, hub)

            error = consumer.take_error()
            if error is not None:
                raise ConnectionError(error)

            fatal_exitcode = _pool_fatal_exitcode(obj)
            if fatal_exitcode is not None:
                raise SystemExit(fatal_exitcode)

            if waker_fd is None:
                # Without a Waker descriptor, the fallback must not block.
                _hub_tick(hub)
                if consumer.pending():
                    continue
                connection.wait(_next_timeout(hub))
                continue

            # One hub iteration waits for timers, mailbox descriptors or the shared Waker.
            try:
                next(loop)
            except StopIteration:
                loop = hub.create_loop()
    except Exception:
        # Clear the broken connection's hub entries, preserving the timer shared with active task time limits.
        try:
            hub.reset()
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception(
                'Error cleaning up after event loop: %r', exc)
        loops._cancel_timer_entries(hub.timer, registered_timer_entries)
        raise
    finally:
        _unregister_waker(hub, waker_fd)
        _remove_sentinel(hub, sentinel)
