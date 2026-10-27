"""Pool prefork Rust: zygote + figli Python supervisionati dal core Rust.

Sostituisce billiard/``AsynPool``: la stessa semantica di
``celery.concurrency.prefork.TaskPool``, con il supervisore in Rust
(``crates/celery-worker/src/pool``) e i figli forkati dallo zygote.

``apply_async`` restituisce un oggetto con l'interfaccia di
``billiard.pool.ApplyResult`` usata da Celery. I callback vengono invocati nel
thread del loop del master quando chiama :meth:`RsTaskPool.process_events`
(non bloccante), tipicamente dopo un risveglio del ``Waker`` passato al
costruttore (``waker=``). Per pilotare il pool nei test::

    while pool.alive_work():
        pool.wait(0.1)

dove :meth:`RsTaskPool.wait` è ``waker.wait`` + ``process_events``.
"""

import itertools
import os
import pickle
import signal
import socket
import threading
import time

from billiard.common import TERM_SIGNAL, human_status
from billiard.einfo import ExceptionInfo
from billiard.exceptions import Terminated, TimeLimitExceeded, WorkerLostError

from celery.concurrency.base import BasePool
from celery.platforms import EX_FAILURE
from celery.utils.log import get_logger

from . import _rs_zygote

try:
    import _celery_rs
except ImportError as exc:  # pragma: no cover - solo senza estensione nativa
    raise ImportError(
        'celery.concurrency.rs_prefork richiede il modulo nativo _celery_rs'
    ) from exc

__all__ = ('RsTaskPool', 'RsApplyResult')

logger = get_logger(__name__)

#: Come il contatore globale di billiard: l'id del job compare nei messaggi
#: WorkerLostError ("Job: N").
_job_counter = itertools.count(1)


class RsApplyResult:
    """Interfaccia di ``billiard.pool.ApplyResult`` usata da Celery."""

    _worker_lost = None
    _write_to = None
    _scheduled_for = None

    def __init__(self, cache, job, callback=None, accept_callback=None,
                 timeout_callback=None, error_callback=None,
                 soft_timeout=None, timeout=None, correlation_id=None,
                 callbacks_propagate=(), on_timeout_set=None,
                 on_timeout_cancel=None):
        self._job = job
        self._cache = cache
        self._callback = callback
        self._accept_callback = accept_callback
        self._timeout_callback = timeout_callback
        self._error_callback = error_callback
        self._soft_timeout = soft_timeout
        self._timeout = timeout
        self._correlation_id = correlation_id
        self._callbacks_propagate = callbacks_propagate or ()
        self._on_timeout_set = on_timeout_set
        self._on_timeout_cancel = on_timeout_cancel

        self._mutex = threading.Lock()
        self._event = threading.Event()
        self._accepted = False
        self._cancelled = False
        self._worker_pid = None
        self._time_accepted = None
        self._terminated = None
        self._success = False
        self._value = None
        cache[self._job] = self

    @property
    def correlation_id(self):
        return self._correlation_id

    def __repr__(self):
        return '<{name}: {id} ack:{ack} ready:{ready}>'.format(
            name=type(self).__name__, id=self._job,
            ack=self._accepted, ready=self.ready(),
        )

    def ready(self):
        return self._event.is_set()

    def accepted(self):
        return self._accepted

    def successful(self):
        assert self.ready()
        return self._success

    def _cancel(self):
        self._cancelled = True

    def discard(self):
        self._cache.pop(self._job, None)

    def terminate(self, signum):
        self._terminated = signum

    def _set_terminated(self, signum=None):
        try:
            raise Terminated(-(signum or 0))
        except Terminated:
            self._set(None, (False, ExceptionInfo()))

    def worker_pids(self):
        return [self._worker_pid] if self._worker_pid else []

    def wait(self, timeout=None):
        self._event.wait(timeout)

    def get(self, timeout=None):
        self.wait(timeout)
        if not self.ready():
            raise TimeoutError
        if self._success:
            return self._value
        raise self._value.exception

    def safe_apply_callback(self, fun, *args, **kwargs):
        if fun:
            try:
                fun(*args, **kwargs)
            except MemoryError:
                raise
            except self._callbacks_propagate:
                raise
            except Exception as exc:
                logger.error('Pool callback raised exception: %r', exc,
                             exc_info=True)

    def handle_timeout(self, soft=False):
        if self._timeout_callback is not None:
            self.safe_apply_callback(
                self._timeout_callback, soft=soft,
                timeout=self._soft_timeout if soft else self._timeout,
            )

    def _set(self, i, obj):
        with self._mutex:
            if self._on_timeout_cancel:
                self._on_timeout_cancel(self)
            self._success, self._value = obj
            self._event.set()
            if self._accepted:
                # Se il risultato arriva prima dell'ack, l'ack farà la pulizia.
                self._cache.pop(self._job, None)
            if self._callback and self._success:
                self.safe_apply_callback(self._callback, self._value)
            if (self._value is not None
                    and self._error_callback and not self._success):
                self.safe_apply_callback(self._error_callback, self._value)

    def _ack(self, i, time_accepted, pid):
        with self._mutex:
            if self._cancelled:
                self._accepted = True
                return
            self._accepted = True
            self._time_accepted = time_accepted
            self._worker_pid = pid
            if self.ready():
                self._cache.pop(self._job, None)
            if self._on_timeout_set:
                self._on_timeout_set(self, self._soft_timeout, self._timeout)
            if self._accept_callback:
                try:
                    self._accept_callback(pid, time_accepted)
                except self._callbacks_propagate:
                    raise
                except Exception:
                    pass  # come AsynPool: NACK non supportato, si ignora


def _exception_info(exc):
    """``ExceptionInfo`` con la stessa forma di billiard per ``exc``."""
    try:
        raise exc
    except BaseException:
        return ExceptionInfo()


class RsTaskPool(BasePool):
    """Pool prefork con supervisore Rust (vedi spec 05b)."""

    signal_safe = True
    is_green = False
    uses_semaphore = True
    task_join_will_block = True
    body_can_be_buffer = False

    def __init__(self, limit=None, putlocks=True, forking_enable=True,
                 callbacks_propagate=(), app=None, **options):
        self.waker = options.pop('waker', None) or _celery_rs.Waker()
        super().__init__(
            limit=limit, putlocks=putlocks, forking_enable=forking_enable,
            callbacks_propagate=callbacks_propagate, app=app, **options,
        )
        self._cache = {}
        self._pool = None
        self._control_sock = None
        self._zygote_pid = 0
        self._pool_created = False
        self._zygote_lost = False
        #: Putlock condiviso con `Worker._process_task_sem`: billiard lo
        #: rilascia quando un job è pronto; senza, dopo `-c` task il master
        #: smetterebbe di consegnare lavoro al pool.
        self._putlock = self.options.get('semaphore')
        #: Exit code con cui il worker deve uscire se il pool non è più
        #: utilizzabile (zygote perso). ``None`` finché il pool è sano.
        self._fatal_exitcode = None

    # -- lifecycle ---------------------------------------------------------

    def on_start(self):
        app = self.app
        if app is None:
            raise RuntimeError('RsTaskPool richiede app=')
        limit = self.limit or 1
        self._control_sock, self._zygote_pid = _rs_zygote.create(
            app, self._hostname())
        try:
            self._pool = _celery_rs.Pool(
                self._control_sock.fileno(), self._zygote_pid, limit,
                _positive_int(self.options.get('maxtasksperchild')),
                _positive_int(self.options.get('max_memory_per_child')),
                self.waker,
            )
        except BaseException:
            # Lo zygote esiste ma nessun supervisore lo chiuderà.
            self._finish_zygote()
            raise
        self._pool_created = True

    def on_stop(self):
        """Chiusura ordinata: completa i job e chiude i figli."""
        pool = self._pool
        if pool is not None:
            pool.close()
            while not pool.stopped():
                self.wait(0.1)
            self.process_events()
            self._pool = None
        self._finish_zygote()
        self._clear_putlock()

    def on_terminate(self):
        """Terminazione immediata (SIGTERM, poi SIGKILL)."""
        pool = self._pool
        if pool is not None:
            pool.terminate()
            deadline = time.monotonic() + 5.0
            while not pool.stopped() and time.monotonic() < deadline:
                self.wait(0.05)
            self.process_events()
            if not pool.stopped():  # pragma: no cover - difensivo
                logger.error('pool Rust non terminato entro il timeout')
            self._pool = None
        self._finish_zygote()
        self._clear_putlock()

    def on_close(self):
        if self._pool is not None:
            self._pool.close()
        self._clear_putlock()

    def _release_putlock(self):
        """Rilascia uno slot del semaforo di `_process_task_sem` (come billiard)."""
        if self._putlock is not None:
            try:
                self._putlock.release()
            except Exception:  # pragma: no cover - semaforo in stato strano
                logger.exception('Error releasing pool putlock')

    def _clear_putlock(self):
        # billiard lo azzera alla chiusura: i callback in attesa non devono
        # essere richiamati dopo lo shutdown.
        if self._putlock is not None:
            try:
                self._putlock.clear()
            except Exception:  # pragma: no cover
                pass

    def _finish_zygote(self):
        sock, pid = self._control_sock, self._zygote_pid
        self._control_sock = None
        self._zygote_pid = 0
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
        if pid:
            if not self._pool_created:  # pragma: no cover - errore in on_start
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
            try:
                os.waitpid(pid, 0)
            except (ChildProcessError, OSError):
                # Il supervisore Rust lo ha già raccolto.
                pass
        self._pool_created = False

    # -- esecuzione --------------------------------------------------------

    def on_apply(self, target, args, kwargs, waitforslot=False,
                 callbacks_propagate=(), **options):
        pool = self._pool
        if pool is None:
            raise RuntimeError('pool non avviato')
        job = next(_job_counter)
        result = RsApplyResult(
            self._cache, job,
            callback=options.get('callback'),
            accept_callback=options.get('accept_callback'),
            timeout_callback=options.get('timeout_callback'),
            error_callback=options.get('error_callback'),
            soft_timeout=options.get('soft_timeout'),
            timeout=options.get('timeout'),
            correlation_id=options.get('correlation_id'),
            callbacks_propagate=callbacks_propagate,
        )
        payload = pickle.dumps(
            (target, tuple(args or ()), dict(kwargs or {})),
            protocol=pickle.HIGHEST_PROTOCOL,
        )
        try:
            pool.submit(job, payload, options.get('soft_timeout'),
                        options.get('timeout'))
        except BaseException:
            self._cache.pop(job, None)
            raise
        return result

    # -- eventi ------------------------------------------------------------

    def process_events(self):
        """Esegue i callback degli eventi pronti; non blocca."""
        pool = self._pool
        if pool is None:
            return 0
        events = pool.drain_events()
        for event in events:
            self._handle_event(event)
        return len(events)

    def _handle_event(self, event):
        kind = event.kind
        if kind == 'zygote-lost':
            self._zygote_lost = True
            if self._fatal_exitcode is None:
                self._fatal_exitcode = EX_FAILURE
                # Nessun altro figlio può nascere: i job in corso finiscono
                # con WorkerLostError e i messaggi tornano al broker secondo
                # acks_late/reject_on_worker_lost. Il master deve uscire con
                # un exit code != 0 perché systemd/supervisor lo riavvii.
                logger.critical(
                    'Pool zygote process %s exited: no new child processes '
                    'can be created. Shutting down the worker with exit '
                    'code %s.', self._zygote_pid or '?', EX_FAILURE)
                if self._pool is not None:
                    self._pool.abandon_children()
            return
        result = self._cache.get(event.job)
        if result is None:
            return
        if kind in ('ready', 'worker-lost', 'terminated', 'hard-timeout',
                    'pool-failure'):
            # Come billiard: lo slot si libera appena il job ha un esito.
            if not result.ready():
                self._release_putlock()
        if kind == 'accepted':
            result._ack(event.job, event.time_accepted, event.pid)
        elif kind == 'ready':
            try:
                value = pickle.loads(event.payload)
            except Exception as exc:  # payload corrotto: errore di protocollo
                value = (False, _exception_info(exc))
            result._set(event.job, value)
        elif kind == 'worker-lost':
            result._set(event.job, (False, self._worker_lost_info(
                event.job, event.exitcode)))
        elif kind == 'terminated':
            result._set_terminated(event.exitcode)
        elif kind == 'soft-timeout':
            result.handle_timeout(soft=True)
        elif kind == 'hard-timeout':
            result._set(event.job, (False, self._time_limit_info(event.timeout)))
            result.handle_timeout(soft=False)
        elif kind == 'pool-failure':
            result._set(event.job, (False, _exception_info(
                WorkerLostError(event.message))))

    @staticmethod
    def _worker_lost_info(job, exitcode):
        return _exception_info(WorkerLostError(
            'Worker exited prematurely: {0} Job: {1}.'.format(
                human_status(exitcode), job)))

    @staticmethod
    def _time_limit_info(timeout):
        return _exception_info(TimeLimitExceeded(timeout))

    def wait(self, timeout=None):
        """Attende un evento del pool e lo processa (driver dei test)."""
        notified = self.waker.wait(timeout)
        self.process_events()
        return notified

    def alive_work(self):
        """True finché ci sono job non ancora conclusi."""
        return any(not result.ready() for result in self._cache.values())

    # -- API BasePool ------------------------------------------------------

    def did_start_ok(self):
        return self._pool is not None and self._pool.did_start_ok()

    @property
    def fatal_exitcode(self):
        """Exit code richiesto al worker se il pool è inutilizzabile."""
        return self._fatal_exitcode

    def flush(self):
        pool = self._pool
        if pool is None or self._state == self.TERMINATE:
            return
        for job in pool.flush():
            result = self._cache.pop(job, None)
            if result is not None:
                result.discard()

    def maintain_pool(self, *args, **kwargs):
        # Il supervisore rimpiazza i figli da solo.
        self.process_events()

    def terminate_job(self, pid, signal=None):
        if self._pool is not None:
            self._pool.terminate_job(pid, signal or TERM_SIGNAL)

    def restart(self):
        if self._pool is not None:
            self._pool.restart()

    def reset_backend(self, broker):
        if self._pool is not None:
            self._pool.reset_backend(broker)

    def grow(self, n=1):
        if self._pool is not None:
            self._pool.grow(n)
        if self._putlock is not None:
            self._putlock.grow(n)

    def shrink(self, n=1):
        if self._pool is not None:
            self._pool.shrink(n)
        if self._putlock is not None:
            self._putlock.shrink(n)

    def _get_info(self):
        info = super()._get_info()
        pool = self._pool
        if pool is None:
            info.update({
                'max-concurrency': self.limit,
                'processes': [],
                'max-tasks-per-child': self.options.get('maxtasksperchild') or 'N/A',
                'put-guarded-by-semaphore': self.putlocks,
                'timeouts': (self.options.get('soft_timeout') or 0,
                             self.options.get('timeout') or 0),
                'writes': 'N/A',
            })
            return info
        total, per_child = pool.write_stats()
        info.update({
            'max-concurrency': self.limit,
            'processes': pool.pids(),
            'max-tasks-per-child': self.options.get('maxtasksperchild') or 'N/A',
            'put-guarded-by-semaphore': self.putlocks,
            'timeouts': (self.options.get('soft_timeout') or 0,
                         self.options.get('timeout') or 0),
            'writes': self._write_stats(total, per_child),
        })
        return info

    @staticmethod
    def _write_stats(total, per_child):
        def per(value):
            return f'{(float(value) / total) if total else 0:.2f}'

        return {
            'total': total,
            'avg': per(total / len(per_child) if per_child else 0),
            'all': ', '.join(per(value) for value in per_child),
            'raw': ', '.join(str(value) for value in per_child),
            'strategy': 'default',
            'inqueues': {'total': len(per_child), 'active': 0},
        }

    @property
    def num_processes(self):
        return self._pool.num_processes() if self._pool is not None else (
            self.limit or 1)

    def _hostname(self):
        initargs = self.options.get('initargs') or ()
        if len(initargs) > 1 and initargs[1]:
            return initargs[1]
        return socket.gethostname()


def _positive_int(value):
    if value is None:
        return None
    value = int(value)
    return value if value > 0 else None
