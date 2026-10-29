"""Rust pool child with billiard-compatible job execution and shutdown."""

import os
import pickle
import signal
import socket
import struct
import sys
import time
import traceback

from _celery_rs import _release_task_memory

from billiard.common import (
    clear_terminate_handler_fired,
    reset_signals,
    terminate_handler_fired,
)
from billiard.compat import mem_rss
from billiard.einfo import ExceptionInfo, ExceptionWithTraceback
from billiard.pool import EX_FAILURE, EX_OK, EX_RECYCLE, soft_timeout_sighandler
from celery.utils.log import get_logger

__all__ = ('child_main', 'MAXMEM_USED_FMT')

logger = get_logger(__name__)

# Billiard's message when a child exceeds max_memory_per_child.
MAXMEM_USED_FMT = """\
child process exiting after exceeding memory limit ({0}KiB / {1}KiB)
"""

# Frame types must match crates/celery-worker/src/pool/frame.rs.
TASK = 1
ACK = 2
READY = 3
RESET_BACKEND = 4

_META = struct.Struct('>BQ')


def _read_exact(sock, size):
    """Read exactly ``size`` bytes, returning ``None`` on EOF before a frame."""
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
            raise EOFError('truncated frame')
        offset += received
    return buffer


def _write_frame(sock, kind, job, payload):
    header = struct.pack('>I', _META.size + len(payload))
    sock.sendall(b''.join((header, kind.to_bytes(1, 'big'),
                           job.to_bytes(8, 'big'), payload)))


def _write_ready(sock, job, result, retire=False):
    """Encode READY flags: bit 0 for success, bit 1 for child retirement."""
    flags = (1 if result[0] else 0) | (2 if retire else 0)
    _write_frame(sock, READY, job,
                 bytes((flags,)) +
                 pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL))


def _clear_result_traceback(result):
    # After READY, frames have finished; clearing them releases ExceptionInfo cycles without waiting for GC.
    value = result[1]
    if result[0] and isinstance(value, tuple) and len(value) == 3:
        value = value[1]
    if isinstance(value, ExceptionInfo):
        exception = value.exception
        if isinstance(exception, ExceptionWithTraceback):
            exception = exception.exc
        if isinstance(exception, BaseException) and exception.__traceback__ is not None:
            tb = exception.__traceback__
            traceback.clear_frames(tb)
            # clear_frames follows tb_next only; f_back retains fast_trace_task and its result cycle.
            frame = tb.tb_frame.f_back
            while frame is not None:
                try:
                    frame.clear()
                except RuntimeError:
                    break
                frame = frame.f_back


def child_main(fd, app, hostname, maxtasks=0, max_memory_per_child=0):
    """Run the child loop and return its exit code."""
    from celery.concurrency.prefork import process_destructor, process_initializer

    sock = socket.socket(fileno=fd)
    sock.setblocking(True)
    pid = os.getpid()
    exitcode = EX_OK
    probe = None
    if os.environ.get('CELERY_RS_MEMORY_PROBE'):
        from celery.concurrency._rs_memory_probe import MemoryProbe
        probe = MemoryProbe(os.environ['CELERY_RS_MEMORY_PROBE'])
    # Billiard resets the inherited flag before initialization.
    clear_terminate_handler_fired()
    process_initializer(app, hostname)
    reset_signals(full=True)
    signal.signal(signal.SIGUSR1, soft_timeout_sighandler)
    # Release free initializer pages before they become private.
    _release_task_memory(force=True)

    completed = 0
    try:
        while not maxtasks or completed < maxtasks:
            header = _read_exact(sock, 4)
            if header is None:
                break
            length = struct.unpack('>I', header)[0]
            body = _read_exact(sock, length)
            if body is None:
                raise EOFError('truncated frame')
            kind, job = _META.unpack_from(body)
            if kind == RESET_BACKEND:
                backend = sys.modules.get('celery.backends._rs_redis')
                if backend is not None:
                    backend._reset_on_broker_restart(
                        app, body[_META.size:].decode())
                continue
            if kind != TASK:
                continue
            target, args, kwargs = pickle.loads(memoryview(body)[_META.size:])
            _write_frame(sock, ACK, job,
                         struct.pack('>d', time.monotonic()))
            try:
                result = (True, target(*args, **kwargs))
            except BaseException as exc:
                if terminate_handler_fired():
                    # Match billiard: shutdown signals produce no result.
                    raise
                result = (False, ExceptionInfo())
                if isinstance(exc, (SystemExit, KeyboardInterrupt)):
                    # An exiting child must not receive another job.
                    _write_ready(sock, job, result, retire=True)
                    raise
            completed += 1
            # Decide retirement before READY so the supervisor cannot assign another job.
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
            _clear_result_traceback(result)
            # An idle child must not retain the previous job and its buffers.
            del result, target, args, kwargs, body
            # Release free buffers without collecting user objects.
            _release_task_memory(force=True)
            if probe is not None:
                probe.sample(completed)
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
