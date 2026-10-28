"""Consumer del broker del worker nel core Rust.

Sostituisce, per il worker con pool Rust (``-P solo`` e ``-P prefork``), la
parte kombu del consumer: la connessione di lettura, la sottoscrizione alle
code, il prefetch, ack/reject e il loop principale. Il master (bootstep,
strategie, ``Request``, segnali) resta il codice Python del riferimento.

Cosa resta su kombu (su una connessione lazy aperta solo quando serve):
remote control/pidbox, mingle, gossip ed eventi in *lettura*. I loro fd sono
registrati sull'hub del worker e vengono serviti dal poll di :func:`rsloop`,
che dorme su epoll col fd del ``Waker`` Rust e il timeout del prossimo timer
kombu. I timer (ETA/countdown, rate limit, heartbeat degli eventi e la
manutenzione periodica delle binding Redis) restano sul timer kombu.

Gli eventi del worker (``worker-online``, ``task-received``, ...) escono dal
producer Rust: ``Consumer.connection_for_write`` restituisce una
``FastConnection`` e il dispatcher usa ``RsProducer``.

``CELERY_RS_WORKER=0`` riporta tutto al percorso del riferimento.
"""
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

try:  # il pacchetto completo lo installa sempre; il riferimento no.
    import _celery_rs
except ImportError:  # pragma: no cover - solo senza estensione nativa
    _celery_rs = None

__all__ = ('RsConnection', 'RsTaskConsumer', 'rsloop', 'use_rs_worker',
           'use_rs_pool', 'connection', 'is_rs_connection', 'stats',
           'new_waker')

#: Variabile d'ambiente per tornare al consumer di kombu.
ENV_DISABLE = 'CELERY_RS_WORKER'

#: Se impostata, il worker scrive qui un JSON con lo stato del consumer
#: (usato dai test per verificare che sia davvero quello Rust).
ENV_STATE = 'CELERY_RS_WORKER_STATE'

#: Logger del consumer: gli stessi nomi del riferimento.
logger = logging.getLogger('celery.worker.consumer')

#: Contatori interni (verificati dai test: il worker usa davvero Rust).
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
    """Copia dei contatori interni del consumer Rust."""
    return dict(_STATS)


def _enabled():
    value = os.environ.get(ENV_DISABLE)
    if value is None:
        return True
    return value.strip().lower() not in ('0', 'false', 'no', 'off')


def new_waker():
    """Un ``_celery_rs.Waker`` nuovo (usato dal pool prefork Rust)."""
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
    except OSError:  # la diagnostica non deve mai fermare il worker
        pass


def _pool_is_solo(consumer):
    from celery.concurrency.solo import TaskPool
    return isinstance(getattr(consumer, 'pool', None), TaskPool)


def _pool_is_rs_prefork(consumer):
    try:
        from celery.concurrency.rs_prefork import RsTaskPool
    except ImportError:  # pragma: no cover - solo senza estensione nativa
        return False
    return isinstance(getattr(consumer, 'pool', None), RsTaskPool)


def _pool_is_rs(consumer):
    """True per i pool serviti dal consumer Rust (``solo`` o prefork Rust)."""
    return _pool_is_solo(consumer) or _pool_is_rs_prefork(consumer)


def transport_is_eligible(connection):
    """Stesse regole di ``use_fast_producer`` sul trasporto della connessione."""
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
    """True se *consumer* può usare il consumer Rust."""
    if not _enabled() or _celery_rs is None:
        return False
    # `worker_disable_prefetch` (solo Redis) modifica `can_consume` del QoS del
    # canale kombu: il consumer Rust non lo userebbe, quindi si resta su kombu.
    if consumer.app.conf.worker_disable_prefetch:
        return False
    if not _pool_is_rs(consumer):
        return False
    # Trasporto non coperto dal core (es. `memory://` nei test): il consumer
    # Rust non saprebbe parlare col broker, si resta sul riferimento.
    conninfo = getattr(consumer, 'conninfo', None)
    return conninfo is None or transport_is_eligible(conninfo)


def use_rs_pool(worker):
    """True se il pool di *worker* può essere ``RsTaskPool``.

    Stesse condizioni del consumer Rust (trasporto, ``CELERY_RS_WORKER``,
    ``worker_disable_prefetch``) più: solo con il loop asincrono e solo per il
    pool prefork di default. Un pool scelto dall'utente resta quello richiesto.
    """
    if not _enabled() or _celery_rs is None:
        return False
    if not getattr(worker, 'use_eventloop', False):
        return False
    if worker.app.conf.worker_disable_prefetch:
        return False
    import sys
    prefork = sys.modules.get('celery.concurrency.prefork')
    # Un pool già risolto può essere quello prefork solo se il modulo esiste.
    if prefork is None or getattr(worker, 'pool_cls', None) is not prefork.TaskPool:
        return False
    try:
        from celery.app import _rs_producer
        connection = (
            _rs_producer.fast_connection(
                worker.app, worker.app.conf.broker_read_url)
            or worker.app.connection_for_read())
        return transport_is_eligible(connection)
    except Exception:  # configurazione non valida: si resta sul riferimento
        return False


def is_rs_connection(connection):
    return isinstance(connection, RsConnection)


def is_fast_connection(connection):
    return isinstance(connection, _rs_producer.FastConnection)


class RsTransport:
    """Sottoinsieme di ``connection.transport`` usato dal consumer e dai bootstep."""

    __slots__ = ('_rs',)

    def __init__(self, rs):
        self._rs = rs

    @property
    def driver_type(self):
        return self._rs.driver_type

    @property
    def implements(self):
        # L'asincronia è del consumer Rust, non del trasporto kombu.
        return SimpleNamespace(asynchronous=False, heartbeats=False)

    def register_with_event_loop(self, connection, hub):
        self._rs.register_with_event_loop(hub)


class _RsChannel:
    """Canale presentato al codice Python che chiede un canale kombu.

    Solo ``basic_qos`` è gestito dal consumer Rust; il canale kombu viene
    aperto alla prima operazione che lo richiede davvero (pidbox/gossip hanno
    il loro consumer Rust). Sul percorso caldo nessuno lo tocca: il master non
    importa il trasporto kombu.
    """

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
    """Il sottoinsieme di ``kombu.Connection`` usato da ``Consumer`` e bootstep.

    La connessione al broker è quella del consumer Rust
    (``_celery_rs.WorkerConsumer``); ``kombu_connection``/``channel`` aprono una
    connessione kombu separata solo se qualcuno li chiede (pidbox, mingle,
    gossip).
    """

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
        # Stesso waker del pool Rust: un solo fd da registrare nel loop.
        self._waker = waker or _celery_rs.Waker()
        self._default_prefetch = 0
        self._hub = None
        self._aux = []
        self._kombu_open = False
        self._closed = False
        # `bound_open_broker_sockets` legge l'attributo privato.
        self._connection = None
        self.transport = RsTransport(self)

    # ------------------------------------------------------------ connessione

    def connect(self):
        self._connect()
        return self

    def _connect(self):
        if self._worker is not None:
            return self._worker
        if self._closed:
            raise ConnectionError('connessione chiusa')
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
        """Il ``WorkerConsumer`` Rust, creato al primo uso."""
        return self._worker if self._worker is not None else self._connect()

    def ensure_connection(self, errback=None, max_retries=None,
                          interval_start=2, interval_step=2,
                          interval_max=30, callback=None, **kwargs):
        """Come ``kombu.Connection.ensure_connection`` (retry lineare)."""
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
        except Exception as exc:  # pragma: no cover - trasporto esotico
            logger.debug('registrazione hub della connessione kombu fallita: %r',
                         exc)

    def kombu_connection(self):
        """Connessione kombu lazy per pidbox/mingle/gossip."""
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
        """Il ``_celery_rs.Waker`` condiviso con il pool (fd per l'hub)."""
        return self._waker

    @property
    def default_channel(self):
        # Il consumer dei task resta su Rust: il canale serve solo a
        # basic_qos (prefetch Rust); qualunque altra operazione apre il canale
        # kombu al primo uso.
        return _RsChannel(self)

    def channel(self):
        """Nuovo canale kombu, come ``kombu.Connection.channel``.

        Solo il percorso del riferimento (``CELERY_RS_WORKER=0``) lo usa
        davvero: pidbox e gossip hanno il loro consumer Rust.
        """
        return _RsChannel(self, self.kombu_connection().channel())

    # --------------------------------------------------------- consumer ausiliari

    def new_worker(self):
        """Un consumer ausiliario Rust che condivide la connessione dei task."""
        if self._worker is None and not self._closed:
            # Pidbox/gossip possono partire prima del `consume` dei task: la
            # connessione è la stessa, quindi il consumer ausiliario la crea.
            self._connect()
        if self._worker is not None:
            return self._worker.aux_consumer()
        return _celery_rs.WorkerConsumer(
            self._url, self._waker, self._options,
            retry_policy={'enabled': False})

    def register_aux(self, consumer):
        """Registra un consumer ausiliario drenato dal loop del master."""
        if consumer not in self._aux:
            self._aux.append(consumer)

    def unregister_aux(self, consumer):
        try:
            self._aux.remove(consumer)
        except ValueError:
            pass

    def drain_aux(self):
        """Consegna gli handler i messaggi pronti di pidbox/gossip."""
        for consumer in tuple(self._aux):
            consumer.drain()

    def set_default_prefetch(self, prefetch_count):
        logger.debug('RsConnection: prefetch iniziale %s (worker=%r)',
                     prefetch_count, self._worker is not None)
        self._default_prefetch = prefetch_count
        if self._worker is not None:
            self._worker.set_prefetch(prefetch_count)
            _STATS['prefetch_updates'] += 1

    # ------------------------------------------------------------ API kombu

    def drain_events(self, timeout=None):
        """Drena la connessione kombu lazy (usata da mingle/pidbox)."""
        return self.kombu_connection().drain_events(timeout=timeout)

    def ack(self, delivery_tag):
        """Conferma una consegna del consumer Rust (``RsMessage``)."""
        return self.worker().ack(delivery_tag)

    def reject(self, delivery_tag, requeue=False):
        """Scarta o rimette in coda una consegna del consumer Rust."""
        return self.worker().reject(delivery_tag, requeue=requeue)

    def wait(self, timeout=None):
        """Dorme fino alla prossima consegna/scadenza; GIL rilasciato."""
        return self._waker.wait(timeout)

    def get_heartbeat_interval(self):
        # Gli heartbeat AMQP sono gestiti dal runtime Rust (lapin).
        return 0.0

    @property
    def supports_heartbeats(self):
        return False

    def heartbeat_check(self, rate=2):
        return None

    @property
    def qos_semantics_matches_spec(self):
        # RabbitMQ 4.x (e i trasporti virtuali) usano il QoS per-consumer.
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
            except Exception:  # connessione già rotta: best effort
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
                except Exception:  # best effort in chiusura
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
    """Il sottoinsieme di ``kombu.Consumer`` usato dal bootstep ``Tasks``."""

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

    # ------------------------------------------------------------ code

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
            # Il prefetch impostato da `Tasks.start` (basic_qos) vale anche se
            # il consumer viene creato dopo. Non si riapplica ai `consume()`
            # successivi (add_consumer), che non devono annullare un QoS
            # incrementato per un task ETA.
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
        """Cancella il consumatore; la connessione resta aperta per gli ack."""
        if self._rs._worker is not None:
            self._rs._worker.cancel()
        self._consuming = False
        self._active.clear()

    def close(self):
        self.cancel()

    # ------------------------------------------------------------ messaggi

    def drain_messages(self, max_batch=1000):
        """Consegne pronte come ``RsMessage`` (nessuna attesa)."""
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

    # ------------------------------------------------------------ dichiarazioni

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
    """``RsConnection`` per *consumer*, o ``None`` se il trasporto non è idoneo."""
    from celery.app import _rs_producer
    # Il descrittore della connessione non deve importare il trasporto kombu:
    # servono solo URL, opzioni e classi di errore.
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
    # Il pool Rust (se attivo) è stato creato prima del consumer: il suo waker
    # è già quello che il loop registra nell'hub, e deve restare lo stesso
    # anche dopo una riconnessione.
    waker = getattr(getattr(consumer, 'controller', None), '_rs_waker', None)
    return RsConnection(consumer, conninfo, url, options, waker=waker)


# ---------------------------------------------------------------------------
# loop principale
# ---------------------------------------------------------------------------

def _enable_heartbeats(timer, connection, rate):
    """Heartbeat AMQP: per le connessioni Rust li gestisce il runtime."""
    if connection is None or is_rs_connection(connection) or is_fast_connection(connection):
        return [None]
    from celery.worker import loops
    return loops._enable_amqheartbeats(timer, connection, rate=rate)


def _hub_tick(hub):
    """Esegue callback pronti, timer e I/O dell'hub senza mai bloccare.

    Usato solo come ripiego quando il ``Waker`` non espone un fd: il poll
    viene forzato a 0 e un fd sentinella evita il ramo ``sleep``.
    """
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
    """Esegue subito i callback già pronti (ack/reject accodati dai task).

    Il riferimento li esegue al giro successivo dell'hub; farlo subito dopo
    ogni consegna manda l'ack prima di qualunque shutdown e riduce la latenza
    con prefetch piccolo. Gli errori seguono lo stesso percorso del loop hub.
    """
    for callback in hub._pop_ready():
        if callback:
            callback()


def _process_pool(obj):
    """Consegna al pool Rust i suoi eventi pronti (callback dei task)."""
    pool = getattr(obj, 'pool', None)
    process = getattr(pool, 'process_events', None)
    if process is not None:
        process()


def _run_pool_ready(obj, hub):
    _process_pool(obj)
    _run_ready(hub)


def _pool_fatal_exitcode(obj):
    """Exit code da usare se il pool è diventato inutilizzabile (zygote)."""
    pool = getattr(obj, 'pool', None)
    return getattr(pool, 'fatal_exitcode', None)


def _install_sentinel(hub):
    """fd registrato sull'hub: garantisce che ``create_loop`` faccia poll(0)."""
    read_fd, write_fd = os.pipe()
    try:
        os.set_blocking(read_fd, False)
        os.set_blocking(write_fd, False)
        hub.add_reader(read_fd, lambda: None)
    except Exception:  # pragma: no cover - piattaforma esotica
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
    except Exception:  # dopo `hub.reset()` il fd non è più registrato
        pass
    for fd in (read_fd, write_fd):
        try:
            os.close(fd)
        except OSError:
            pass


#: Attesa massima del ripiego senza fd del Waker.
HUB_POLL_INTERVAL = 0.25


def _next_timeout(hub, maximum=HUB_POLL_INTERVAL):
    """Secondi fino alla prossima scadenza del timer kombu (max 0,25s)."""
    timer = getattr(hub, 'timer', None)
    queue = getattr(timer, '_queue', None) if timer is not None else None
    if queue:
        delay = queue[0][0] - time.monotonic()
        maximum = min(maximum, delay)
    return max(0.0, maximum)


def _register_waker(hub, waker):
    """Registra il fd del Waker nel poller dell'hub; ``None`` se assente."""
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
    except Exception:  # dopo `hub.reset()` il fd non è più registrato
        pass


def rsloop(obj, connection, consumer, blueprint, hub, qos, heartbeat, clock,
           hbrate=2.0):
    """Loop del worker con le consegne dal consumer Rust.

    Stessa semantica di :func:`celery.worker.loops.asynloop` per ordine di
    ``on_ready``, ``did_start_ok``, ``maybe_shutdown``, aggiornamento del QoS,
    errori di heartbeat e uscita su ``blueprint.state``. La differenza è
    l'attesa: il loop dorme nel poller dell'hub (epoll) con il fd del ``Waker``
    condiviso da consumer Rust e pool Rust; una consegna, un evento del pool o
    un comando di controllo svegliano il loop subito, senza polling. Il timeout
    del poll è quello del prossimo timer kombu, quindi la manutenzione
    periodica (binding Redis, ETA, rate limit, autoscale) gira come in
    ``asynloop``. I fd di pidbox/gossip sono registrati sullo stesso hub.
    """
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
    # Import e bootstep lasciano pagine libere: restituirle prima del loop
    # riduce il residente senza raccogliere oggetti o cambiare il GC utente.
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

            # Come asynloop: raggruppa le chiamate QoS e dà priorità ai
            # comandi di controllo.
            if qos.prev != qos.value:
                update_qos()

            # Eventi del pool (ack, risultati, timeout, figli persi) prima
            # delle consegne: i callback possono accodare ack sull'hub.
            _run_pool_ready(obj, hub)

            # Pidbox e gossip: i loro consumer Rust condividono il Waker, quindi
            # un comando o un evento di un altro worker ha svegliato il poll.
            drain_aux = getattr(connection, 'drain_aux', None)
            if drain_aux is not None:
                drain_aux()

            deliveries = consumer.drain_messages()
            if deliveries:
                logger.debug('rsloop: %d consegne dal consumer Rust',
                             len(deliveries))
            for message in deliveries:
                try:
                    consumer.on_message(message)
                finally:
                    # Ack/reject del task escono subito, prima di un eventuale
                    # shutdown richiesto durante l'esecuzione.
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
                # Ripiego (nessun fd disponibile): non bloccare il loop.
                _hub_tick(hub)
                if consumer.pending():
                    continue
                connection.wait(_next_timeout(hub))
                continue

            # Dorme nel poll dell'hub: timer, fd di pidbox/gossip e fd del
            # Waker (consegne, eventi del pool). `next` esegue un giro.
            try:
                next(loop)
            except StopIteration:
                loop = hub.create_loop()
    except Exception:
        # Come asynloop: ripulisce fd e callback della connessione rotta e
        # annulla le entry registrate per questa connessione. Il timer non si
        # azzera: è condiviso con i time limit dei task in corso.
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
