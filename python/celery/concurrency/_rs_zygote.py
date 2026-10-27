"""Zygote del pool Rust.

Il master forka questo processo **prima** di avviare i thread Rust del pool
(ma può avere già il runtime del producer: gli ``after_fork`` del pacchetto
scartano i runtime/socket ereditati). Lo zygote:

1. chiude i descrittori ereditati dal padre, conservando controllo e log;
2. importa i moduli pesanti (app, task, ``celery.concurrency.prefork``) e fa
   ``gc.freeze()``: le pagine diventano condivise in copy-on-write da tutti i
   figli;
3. resta in attesa sul socket di controllo e per ogni richiesta crea un
   ``socketpair``, fa ``fork()``, lascia al figlio l'estremità ricevuta e
   riporta il pid al supervisore Rust. Non esegue mai un task.
"""

import array
import gc
import logging
import os
import socket
import struct
import traceback
from itertools import chain

__all__ = ('create',)

#: `[tipo][a][b]`: tipo 1 = crea figlio (`a`=maxtasks, `b`=max_memory_kb),
#: tipo 2 = esci, tipo 3 = raccogli il figlio `a` (pid).
_CONTROL = struct.Struct('>BII')
#: `[stato][pid][lunghezza errore]` + errore.
_REPLY = struct.Struct('>BiI')
_REPLY_LEN = struct.Struct('>I')

CONTROL_SPAWN = 1
CONTROL_SHUTDOWN = 2
CONTROL_REAP = 3


def create(app, hostname):
    """Forka lo zygote e restituisce ``(socket di controllo, pid)``.

    Va chiamata dal master prima di avviare thread propri del pool.
    """
    parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    # I callback after_fork e gli import possono avviare il GC prima che
    # l'heap ereditato sia congelato; i finalizer dei thread non sono sicuri.
    gc_enabled = gc.isenabled()
    gc.disable()
    try:
        pid = os.fork()
    except BaseException:
        if gc_enabled:
            gc.enable()
        parent.close()
        child.close()
        raise
    if pid == 0:
        gc.freeze()
        if gc_enabled:
            gc.enable()
        try:
            parent.close()
            _main(child, app, hostname)
        except BaseException:
            traceback.print_exc()
        finally:
            os._exit(0)
    if gc_enabled:
        gc.enable()
    child.close()
    return parent, pid


def _close_inherited_fds(keep):
    """Chiude tutto tranne stdio e i fd in ``keep``.

    Linux: enumerazione di ``/proc/self/fd``. Gli fd del runtime Rust del
    padre (socket, epoll) non sono utilizzabili dopo il fork e non devono
    restare aperti nello zygote/figli. Si tengono anche gli fd di
    ``sys.stdout``/``sys.stderr``/``sys.stdin``: con la cattura di pytest o un
    log rediretto possono non essere 0/1/2. Conserva anche gli stream e le
    connessioni delle code posseduti dagli handler di logging.
    """
    import sys as _sys

    keep = set(keep)
    for stream in (_sys.stdout, _sys.stderr, _sys.stdin):
        try:
            keep.add(stream.fileno())
        except (AttributeError, OSError, ValueError):
            pass
    # Gli handler restano vivi nei figli: un fd chiuso qui può essere
    # riutilizzato dal broker e ricevere successivamente i byte dei log.
    for logger in chain((logging.getLogger(),),
                        logging.Logger.manager.loggerDict.values()):
        if not isinstance(logger, logging.Logger):
            continue
        for handler in logger.handlers:
            queue = getattr(handler, 'queue', None)
            for stream in (getattr(handler, 'stream', None),
                           getattr(queue, '_reader', None),
                           getattr(queue, '_writer', None)):
                try:
                    keep.add(stream.fileno())
                except (AttributeError, OSError, ValueError):
                    pass
    try:
        entries = [int(entry) for entry in os.listdir('/proc/self/fd')
                   if entry.isdigit()]
    except OSError:
        entries = []
    for fd in entries:
        if fd < 3 or fd in keep:
            continue
        try:
            os.close(fd)
        except OSError:
            pass


def _recv_control(control):
    """Legge un comando di controllo e gli eventuali fd ``SCM_RIGHTS``."""
    data = b''
    fds = []
    needed = _CONTROL.size
    while len(data) < needed:
        chunk, ancdata, _flags, _addr = control.recvmsg(
            needed - len(data), socket.CMSG_SPACE(array.array('i').itemsize))
        if not chunk:
            for fd in fds:
                os.close(fd)
            return None, []
        data += chunk
        for level, kind, payload in ancdata:
            if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                received = array.array('i')
                usable = len(payload) - (len(payload) % received.itemsize)
                received.frombytes(payload[:usable])
                fds.extend(received)
    return data, fds


def _reply(control, status, pid=0, message=b''):
    body = _REPLY.pack(status, pid, len(message)) + message
    control.sendall(_REPLY_LEN.pack(len(body)) + body)


def _warm_cffi():
    # Billiard usa cdef per prctl in ogni initializer: il solo import non
    # crea il parser globale di pycparser, che va condiviso prima del fork.
    try:
        from cffi import FFI
    except ImportError:  # pragma: no cover - billiard può usare ctypes
        return
    ffi = FFI()
    ffi.cdef('int prctl (int __option, ...);')


def _warm_backend_errors(app):
    # Il primo errore di rete importa redis-py e kombu (~15 MB): questi
    # oggetti devono nascere condivisi, prima del freeze e del fork dei figli.
    from celery.backends.redis import RedisBackend

    backend = app.backend
    if isinstance(backend, RedisBackend):
        backend.connection_errors
        backend.channel_errors


def _main(control, app, hostname):
    control.setblocking(True)
    # Importa presto: dopo `gc.freeze()` queste pagine restano condivise.
    from celery.concurrency import _rs_child
    __import__('celery.concurrency.prefork')

    try:
        app.loader.import_default_modules()
    except Exception:
        # Il master ha già importato l'app; se il loader non è disponibile
        # (test/uso diretto) si prosegue con il registry esistente.
        traceback.print_exc()

    # Moduli che ogni figlio usa al primo task: importarli qui (e non nel
    # figlio) li rende condivisi in copy-on-write con lo zygote.
    for module in (
            'celery.app.task', 'celery.canvas', 'celery.result',
            'celery.backends', 'celery.backends.base',
            'celery.backends.asynchronous', 'celery.backends.redis',
            'celery.app.trace', 'celery.app._rs_producer', 'celery.app.amqp',
            # `billiard.util.set_pdeathsig` compila un wrapper cffi all'avvio
            # di ogni figlio (`cffi.FFI()` → cparser/pycparser): qui le pagine
            # restano condivise con tutti i figli.
            'billiard.util', 'cffi.cparser'):
        try:
            __import__(module)
        except Exception:  # pragma: no cover - dipendenze opzionali
            traceback.print_exc()

    _warm_cffi()
    _warm_backend_errors(app)
    _close_inherited_fds({control.fileno()})
    # `gc.freeze()` (non `collect`): nel figlio di un master multi-thread i
    # finalizer di oggetti ereditati possono toccare stato non valido dopo il
    # fork; congelare le pagine basta per la condivisione copy-on-write.
    gc.freeze()

    while True:
        command, fds = _recv_control(control)
        if command is None:
            break
        kind, maxtasks, max_memory = _CONTROL.unpack(command)
        if kind == CONTROL_SHUTDOWN:
            for fd in fds:
                os.close(fd)
            break
        if kind == CONTROL_REAP:
            for fd in fds:
                os.close(fd)
            pid = int(maxtasks)  # il campo maxtasks porta il pid da raccogliere
            try:
                _, status = os.waitpid(pid, 0)
            except (ChildProcessError, OSError):
                _reply(control, 0, -1)
                continue
            if os.WIFEXITED(status):
                exitcode = os.WEXITSTATUS(status)
            elif os.WIFSIGNALED(status):
                exitcode = -os.WTERMSIG(status)
            else:
                exitcode = -1
            _reply(control, 0, exitcode)
            continue
        if kind != CONTROL_SPAWN:
            for fd in fds:
                os.close(fd)
            _reply(control, 1, 0, b'comando di controllo sconosciuto')
            continue
        if len(fds) != 1:
            for fd in fds:
                os.close(fd)
            _reply(control, 1, 0, b'atteso un descrittore per il figlio')
            continue
        child_fd = fds[0]
        try:
            pid = os.fork()
        except OSError as exc:
            os.close(child_fd)
            _reply(control, 1, 0, str(exc).encode('utf-8', 'replace'))
            continue
        if pid == 0:
            # Figlio del pool: chiude il controllo, tiene solo il suo fd.
            try:
                control.close()
            except OSError:
                pass
            try:
                _close_inherited_fds({child_fd})
                code = _rs_child.child_main(
                    child_fd, app, hostname, maxtasks, max_memory)
            except BaseException:
                traceback.print_exc()
                code = 1
            os._exit(code & 0xFF)
        # Zygote: il fd è del figlio, la nostra copia va chiusa.
        try:
            os.close(child_fd)
        except OSError:
            pass
        _reply(control, 0, pid)
