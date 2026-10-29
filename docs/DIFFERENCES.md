# Known differences compared to Celery 5.7.0rc1

celery-rs keeps API, behavior and traffic on the broker identical to Celery: `./compat api|wire|wire-worker|wire-beat|cli|upstream|integration` verifies this. The intentional differences below concern introspection and internals; task results are unchanged.

## Static introspection of some class attributes

To avoid loading redis-py, billiard, and asynpool into processes that don't use them, four class attributes are descriptors resolved on first access:

| attribute | upon first access it becomes |
|---|---|
| `celery.backends.redis.RedisBackend.connection_class_ssl` | `redis.SSLConnection` |
| `celery.backends.redis.SentinelBackend.connection_class_ssl` | `SentinelManagedSSLConnection` |
| `celery.concurrency.prefork.TaskPool.Pool` | `celery.concurrency.asynpool.AsynPool` |
| `celery.concurrency.prefork.TaskPool.BlockingPool` | `billiard.pool.Pool` |

Accessing from the class or an instance returns the actual object; before first access, `vars(cls)` / `inspect.getattr_static` see the descriptor. Listed in `tools/compat` among the accepted differences.

## `celery.utils.nodenames.worker_direct` annotations

`kombu.entity.Queue` is imported only for type checking (not at runtime, so as not to load kombu): `typing.get_type_hints(worker_direct)` raises `NameError`, while `inspect.signature` and annotations as strings are identical to the reference.

## Traceback of failed tasks (Rust prefork pool)

After the child sends the result of a failed task, the now finished traceback frames are emptied (`frame.clear()`), as Celery already tries to do during tracing. This avoids reference cycles that retain memory until garbage collection. Signals (`task_failure`, …) and errbacks receive full traceback as in the reference; only those who keep a traceback beyond the end of the task (e.g. in a global variable to read it later) find the local variables empty.

## Garbage collector in children

The zygote executes `gc.freeze()` before creating the children (the pages remain shared in copy-on-write): in the children `gc.get_freeze_count()` is non-zero. `gc.isenabled()` and `gc.get_threshold()` seen by the tasks are those of the reference.

## Memory release (Linux/glibc)

After startup and after tasks with payload ≥ 1 MiB the processes return the free pages of the allocator (`malloc_trim`) to the system. No functional effect; CPU cost proportional to the heap.

## Processes

The prefork pool has one more process: the zygote, from which children are born (the master contains the Rust runtime threads and cannot safely fork). With `-c 10`: master, zygote, 10 children.

## Immortal objects in the Rust prefork pool

On CPython 3.12–3.14 with the GIL, on 64-bit little-endian platforms, the master and the zygote immortalize the objects already present before the fork and their referents. This reduces page copies caused by refcount updates in children. The traversal uses `tp_traverse` also on untracked containers, with maximum depth 128; the refcount itself breaks the loops, without large temporary structures. Already immortal objects, those with `tp_finalize`/`tp_del` and native `_celery_rs` instances are excluded (they must be able to close sockets and runtime).

These objects are no longer freed: weakrefs, `WeakValueDictionary`s and weak signal receivers can remain alive until the end of the process; `sys.getrefcount()` returns huge values. Instances with type finalizer are excluded, but can remain alive if held by an immortal container; a `__del__` added to the type after the traversal will not free the already immortal instances. Objects created afterward remain normal. This compromise is accepted for the prefork pool, where the initial objects are preloaded for the lifetime of the worker and RAM saving is prioritized. `CELERY_RS_IMMORTALIZE=0` restores the previous behavior. On other interpreters, free-threaded builds, architectures and versions the feature is a no-op.

## Child initializer prepared in the zygote

With the Rust prefork pool, the parts of `process_initializer` that give the same state in each child (`app.log.setup`, `set_current`/`finalize`, the task tracers) are executed once in the zygote before the fork: children inherit the shared state instead of recreating it (approximately −1 private MB per child). The following still run in the child, in reference order: `set_pdeathsig`, signals, process title, `init_worker`/`init_worker_process` of the loader, `reset_state` and `worker_process_init`. The child rebuilds the tracers of the tasks added or with tracer replaced by the loader hook, and repeats `log.setup` if the logging parameters have changed or if the log file contains `%i` (one file per child). Only difference: an `on_worker_process_init` of the loader that replaces `task.run` of an already registered task is not seen by the prepared tracer.
