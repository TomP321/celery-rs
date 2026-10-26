"""Figlio del pool Rust: esegue i job ricevuti dallo ``socketpair``.

Il processo nasce da ``fork()`` nello zygote (``_rs_zygote``), quindi ha già
l'app e i task importati e condivide in copy-on-write le pagine dello zygote.
Qui si replica il loop di ``billiard.pool.Worker``:

* ``process_initializer(app, hostname)`` del riferimento (reset/ignore dei
  segnali, ``worker_process_init``, logging, ``_set_task_join_will_block``);
* ``reset_signals(full=True)`` e handler SIGUSR1 → ``SoftTimeLimitExceeded``
  come billiard;
* per ogni job: ACK con ``time.monotonic()``, esecuzione del target, READY con
  il risultato serializzato in ``pickle`` (``ExceptionInfo`` per gli errori);
* ``maxtasksperchild`` e ``max_memory_per_child`` (``resource.getrusage``)
  fanno uscire il figlio dopo il job, come billiard;
* ``process_destructor`` / ``worker_process_shutdown`` all'uscita.
"""

import os
import pickle
import signal
import socket
import struct
import time
import traceback

from billiard.common import (
    clear_terminate_handler_fired,
    reset_signals,
    terminate_handler_fired,
)
from billiard.compat import mem_rss
from billiard.einfo import ExceptionInfo
from billiard.pool import EX_FAILURE, EX_OK, EX_RECYCLE, soft_timeout_sighandler
from celery.utils.log import get_logger

__all__ = ('child_main', 'MAXMEM_USED_FMT')

logger = get_logger(__name__)

#: Messaggio di billiard quando un figlio supera ``max_memory_per_child``.
MAXMEM_USED_FMT = """\
child process exiting after exceeding memory limit ({0}KiB / {1}KiB)
"""

#: Tipi di frame (devono combaciare con ``crates/celery-worker/src/pool/frame.rs``).
TASK = 1
ACK = 2
READY = 3

_META = struct.Struct('>BQ')


def _read_exact(sock, size):
    """Legge esattamente ``size`` byte; ``None`` su EOF a inizio frame."""
    buffer = bytearray(size)
    view = memoryview(buffer)
    offset = 0
    while offset < size:
        try:
            received = sock.recv_into(view[offset:], size - offset)
        except InterruptedError:
            continue
        if not received:
            if offset == 0:
                return None
            raise EOFError('frame troncato')
        offset += received
    return buffer


def _write_frame(sock, kind, job, payload):
    header = struct.pack('>I', _META.size + len(payload))
    sock.sendall(b''.join((header, kind.to_bytes(1, 'big'),
                           job.to_bytes(8, 'big'), payload)))


def _write_ready(sock, job, result, retire=False):
    """READY con flag: bit 0 = successo, bit 1 = il figlio si ritira."""
    flags = (1 if result[0] else 0) | (2 if retire else 0)
    _write_frame(sock, READY, job,
                 bytes((flags,)) +
                 pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL))


def child_main(fd, app, hostname, maxtasks=0, max_memory_per_child=0):
    """Loop del figlio; ritorna l'exit code (stile billiard)."""
    from celery.concurrency.prefork import process_destructor, process_initializer

    sock = socket.socket(fileno=fd)
    sock.setblocking(True)
    pid = os.getpid()
    exitcode = EX_OK
    # Billiard azzera il flag ereditato dal padre prima dell'initializer.
    clear_terminate_handler_fired()
    process_initializer(app, hostname)
    reset_signals(full=True)
    signal.signal(signal.SIGUSR1, soft_timeout_sighandler)

    completed = 0
    try:
        while not maxtasks or completed < maxtasks:
            header = _read_exact(sock, 4)
            if header is None:
                break
            length = struct.unpack('>I', header)[0]
            body = _read_exact(sock, length)
            if body is None:
                raise EOFError('frame troncato')
            kind, job = _META.unpack_from(body)
            if kind != TASK:
                continue
            target, args, kwargs = pickle.loads(memoryview(body)[_META.size:])
            _write_frame(sock, ACK, job,
                         struct.pack('>d', time.monotonic()))
            try:
                result = (True, target(*args, **kwargs))
            except BaseException as exc:
                if terminate_handler_fired():
                    # Segnale di shutdown: nessun risultato, come billiard.
                    raise
                result = (False, ExceptionInfo())
                if isinstance(exc, (SystemExit, KeyboardInterrupt)):
                    # Il figlio esce: non deve ricevere altri job.
                    _write_ready(sock, job, result, retire=True)
                    raise
            completed += 1
            # Decidere il ritiro *prima* del READY: così il supervisore non
            # assegna un altro job a un figlio che sta per uscire.
            retire = False
            if max_memory_per_child and max_memory_per_child > 0:
                used_kb = mem_rss()
                if used_kb <= 0:
                    logger.error('worker unable to determine memory usage')
                elif used_kb > max_memory_per_child:
                    logger.warning(
                        MAXMEM_USED_FMT.format(used_kb, max_memory_per_child))
                    exitcode = EX_RECYCLE
                    retire = True
            if maxtasks and completed >= maxtasks:
                exitcode = EX_RECYCLE
                retire = True
            _write_ready(sock, job, result, retire=retire)
            if retire:
                break
    except SystemExit as exc:
        code = exc.code
        if isinstance(code, int):
            exitcode = code
        else:
            exitcode = EX_FAILURE
    except BaseException:
        traceback.print_exc()
        exitcode = EX_FAILURE
    finally:
        try:
            process_destructor(pid, exitcode)
        except BaseException:
            traceback.print_exc()
        try:
            sock.close()
        except OSError:
            pass
    return exitcode
