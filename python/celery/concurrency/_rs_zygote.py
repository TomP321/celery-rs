"""Single-threaded zygote that forks Python children for the Rust pool."""

import array
import gc
import os
import socket
import struct
import traceback

__all__ = ('create',)

# [type][a][b]: 1 creates a child (maxtasks, max_memory_kb), 2 exits, 3 reaps pid a.
_CONTROL = struct.Struct('>BII')
# [status][pid][error length] followed by the error.
_REPLY = struct.Struct('>BiI')
_REPLY_LEN = struct.Struct('>I')

CONTROL_SPAWN = 1
CONTROL_SHUTDOWN = 2
CONTROL_REAP = 3


def create(app, hostname):
    """Fork the zygote before starting pool threads; return its control socket and pid."""
    from _celery_rs import _runtime_fds

    # Collect temporary bootstrap cycles before immortalizing or freezing them.
    gc.collect()
    runtime_fds = _snapshot_runtime_fds(_runtime_fds())
    parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    # Disable GC until inherited objects are frozen; fork callbacks can trigger unsafe thread finalizers.
    gc_enabled = gc.isenabled()
    gc.disable()
    try:
        _immortalize_heap()
        # The master must also avoid rewriting shared GC headers.
        gc.freeze()
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
            _close_inherited_fds({child.fileno()}, runtime_fds)
            _main(child, app, hostname)
        except BaseException:
            traceback.print_exc()
        finally:
            os._exit(0)
    if gc_enabled:
        gc.enable()
    child.close()
    return parent, pid


def _immortalize_heap():
    if os.environ.get("CELERY_RS_IMMORTALIZE") == "0":
        return
    from _celery_rs import _immortalize, _release_task_memory

    _immortalize(gc.get_objects())
    _release_task_memory(force=True)


def _fd_identity(fd):
    try:
        stat = os.fstat(fd)
        return stat.st_dev, stat.st_ino
    except OSError:
        return None


def _snapshot_runtime_fds(fds):
    result = {}
    for fd in fds:
        identity = _fd_identity(fd)
        if identity is None or (hasattr(fds, 'items') and fds[fd] != identity):
            continue
        result[fd] = identity
        try:
            with open(f'/proc/self/fdinfo/{fd}') as info:
                for line in info:
                    if line.startswith('tfd:'):
                        fields = line.replace(':', ': ').split()
                        target = int(fields[1])
                        expected = (int(fields[fields.index('sdev:') + 1], 16),
                                    int(fields[fields.index('ino:') + 1], 16))
                        # epoll is shared; new parent connections may reuse Python descriptor numbers.
                        if _fd_identity(target) == expected:
                            result[target] = expected
        except FileNotFoundError:
            pass
    return result


def _close_inherited_fds(keep, runtime_fds=None):
    """Close Rust descriptors without inherited locks or changes to shared epoll state.

    Match descriptor identities to avoid closing recycled Python descriptors.
    The pre-fork snapshot and child fdinfo also cover connections closed or
    opened by the parent around the snapshot.
    """
    runtime_fds = runtime_fds or {}
    inherited = {fd: identity for fd, identity in runtime_fds.items()
                 if _fd_identity(fd) == identity}
    close = _snapshot_runtime_fds(inherited)
    for fd, identity in close.items():
        if fd >= 3 and fd not in keep and _fd_identity(fd) == identity:
            try:
                os.close(fd)
            except OSError:
                pass


def _recv_control(control):
    """Read a control command and any ``SCM_RIGHTS`` descriptors."""
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
    # Preload billiard's cdef parser before fork so its pages remain shared.
    try:
        from cffi import FFI
    except ImportError:  # pragma: no cover - billiard may use ctypes
        return
    ffi = FFI()
    ffi.cdef('int prctl (int __option, ...);')


def _warm_backend_errors(app):
    # Preload network error dependencies before freeze so children share their pages.
    import sys
    backend = app.backend
    # RPC and memory backends do not need Redis modules in the zygote.
    redis = sys.modules.get('celery.backends.redis')
    if redis is not None and isinstance(backend, redis.RedisBackend):
        backend.connection_errors
        backend.channel_errors


def _main(control, app, hostname):
    control.setblocking(True)
    # Import before freeze to keep these pages shared.
    from celery.concurrency import _rs_child
    __import__('celery.concurrency.prefork')

    try:
        app.loader.import_default_modules()
    except Exception:
        # The master loaded the app; direct pool use may have only the existing registry.
        traceback.print_exc()

    # Preload first-task modules so children share their pages with the zygote.
    for module in (
            'celery.app.task', 'celery.canvas', 'celery.result',
            'celery.backends', 'celery.backends.base',
            'celery.app.trace', 'celery.app._rs_producer', 'celery.app.amqp',
            'billiard.util'):
        try:
            __import__(module)
        except Exception:  # pragma: no cover - optional dependencies
            traceback.print_exc()

    try:
        _warm_backend_errors(app)
    except Exception:
        # Preloading is optional; missing dependencies must still fail at first use as in the reference.
        traceback.print_exc()
    try:
        # Prepare common logging and tracers here to share their pages.
        from celery.concurrency.prefork import prepare_process_initializer
        prepare_process_initializer(app, hostname)
    except Exception:
        # Children repeat initialization as in the reference.
        traceback.print_exc()
    # Freeze without collecting: inherited finalizers may touch invalid state after a multithreaded fork.
    _immortalize_heap()
    gc.freeze()
    _rs_child._release_task_memory(force=True)

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
            pid = int(maxtasks)  # The maxtasks field carries the pid to reap.
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
            _reply(control, 1, 0, b'unknown control command')
            continue
        if len(fds) != 1:
            for fd in fds:
                os.close(fd)
            _reply(control, 1, 0, b'expected a descriptor for the child')
            continue
        child_fd = fds[0]
        try:
            pid = os.fork()
        except OSError as exc:
            os.close(child_fd)
            _reply(control, 1, 0, str(exc).encode('utf-8', 'replace'))
            continue
        if pid == 0:
            # The pool child retains only its own descriptor.
            try:
                control.close()
            except OSError:
                pass
            try:
                # Python descriptors remain inherited; rescanning would dirty copy-on-write pages.
                code = _rs_child.child_main(
                    child_fd, app, hostname, maxtasks, max_memory)
            except BaseException:
                traceback.print_exc()
                code = 1
            os._exit(code & 0xFF)
        # Close the zygote's copy of the child's descriptor.
        try:
            os.close(child_fd)
        except OSError:
            pass
        _reply(control, 0, pid)
