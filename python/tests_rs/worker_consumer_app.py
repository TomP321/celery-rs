"""Shared worker consumer test app configured through the environment."""
import os
import time

from celery import Celery, signals

BROKER = os.environ['CELERY_RS_TEST_WORKER_BROKER']
BACKEND = os.environ.get(
    'CELERY_RS_TEST_WORKER_BACKEND', os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/5')
QUEUE = os.environ.get('CELERY_RS_TEST_WORKER_QUEUE', 'rs-worker-tests')


def _bool(name, default):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in ('0', 'false', 'no', 'off')


# Accelerate Redis binding maintenance for tests.
# Configure before the worker opens a kombu connection.
if _bool('CELERY_RS_TEST_BINDING_SWEEP', False):
    import kombu.transport.redis as _redis_transport
    _redis_transport.MIN_BINDING_TTL = int(os.environ.get(
        'CELERY_RS_TEST_BINDING_TTL_MS', '3000'))
    _redis_transport.BINDING_SWEEP_INTERVAL = float(os.environ.get(
        'CELERY_RS_TEST_BINDING_SWEEP_INTERVAL', '1'))
    _redis_transport.BINDING_EXPIRES_FACTOR = float(os.environ.get(
        'CELERY_RS_TEST_BINDING_EXPIRES_FACTOR', '0.01'))


app = Celery('worker_consumer_app', broker=BROKER, backend=BACKEND)
app.conf.update(
    task_default_queue=QUEUE,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    task_track_started=True,
    result_expires=120,
    worker_hijack_root_logger=False,
    broker_connection_retry_on_startup=True,
    worker_prefetch_multiplier=int(
        os.environ.get('CELERY_RS_TEST_PREFETCH_MULTIPLIER', '1')),
    task_queue_max_priority=10,
    task_acks_on_failure_or_timeout=_bool(
        'CELERY_RS_TEST_ACKS_ON_FAILURE', True),
    worker_soft_shutdown_timeout=float(
        os.environ.get('CELERY_RS_TEST_SOFT_SHUTDOWN', '0')),
    worker_cancel_long_running_tasks_on_connection_loss=_bool(
        'CELERY_RS_TEST_CANCEL_LONG', False),
    broker_transport_options={},
)

# Pool child init/shutdown signal log.
_CHILD_LIFECYCLE = os.environ.get('CELERY_RS_TEST_CHILD_LIFECYCLE')
# Master worker_ready/worker_shutting_down signal log.
_WORKER_LIFECYCLE = os.environ.get('CELERY_RS_TEST_WORKER_LIFECYCLE')
# Gossip join/leave/lost probe log.
_GOSSIP_LOG = os.environ.get('CELERY_RS_TEST_GOSSIP_LOG')


if _GOSSIP_LOG:
    from celery import bootsteps

    def _record_gossip(kind, hostname):
        with open(_GOSSIP_LOG, 'a', encoding='utf-8') as handle:
            handle.write(f'{kind} {hostname}\n')

    class _GossipProbe(bootsteps.StartStopStep):
        """Register Gossip bootstep ``on.node_*`` handlers."""

        requires = ('celery.worker.consumer.gossip:Gossip',)

        def start(self, c):
            gossip = c.gossip
            for kind in ('node_join', 'node_leave', 'node_lost'):
                getattr(gossip.on, kind).add(
                    lambda worker, kind=kind: _record_gossip(
                        kind, worker.hostname))

    app.steps['consumer'].add(_GossipProbe)


if _CHILD_LIFECYCLE:
    def _record(kind):
        with open(_CHILD_LIFECYCLE, 'a', encoding='utf-8') as handle:
            handle.write(f'{kind} {os.getpid()}\n')

    @signals.worker_process_init.connect(weak=False)
    def _child_init(**_kwargs):
        _record('init')

    @signals.worker_process_shutdown.connect(weak=False)
    def _child_shutdown(**_kwargs):
        _record('shutdown')


if _WORKER_LIFECYCLE:
    def _record_worker(kind):
        with open(_WORKER_LIFECYCLE, 'a', encoding='utf-8') as handle:
            handle.write(f'{kind} {os.getpid()}\n')

    @signals.worker_ready.connect(weak=False)
    def _worker_ready(**_kwargs):
        _record_worker('ready')

    @signals.worker_shutting_down.connect(weak=False)
    def _worker_shutting_down(**_kwargs):
        _record_worker('shutting_down')


# Imported module log written by the master.
_MASTER_MODULES = os.environ.get('CELERY_RS_TEST_MASTER_MODULES')

if _MASTER_MODULES:
    import json
    import sys

    @signals.worker_ready.connect(weak=False)
    def _dump_master_modules(**_kwargs):
        with open(_MASTER_MODULES, 'w', encoding='utf-8') as handle:
            json.dump({'pid': os.getpid(), 'modules': sorted(sys.modules)},
                      handle)



@app.task(name='rs_worker.add')
def add(x, y):
    return x + y


@app.task(name='rs_worker.big')
def big(payload, **kwargs):
    """Return large arguments to check body transport integrity."""
    return {
        'len': len(payload),
        'keys': sorted(kwargs),
        'checksum': sum(payload) if isinstance(payload, list) else 0,
    }


@app.task(name='rs_worker.marker')
def marker(path, label='marker', delay=0.0):
    """Write *label* to *path* to check task execution order."""
    with open(path, 'a', encoding='utf-8') as handle:
        handle.write(f'{label}\n')
    if delay:
        time.sleep(delay)
    return label


@app.task(name='rs_worker.sleep_add')
def sleep_add(seconds, x, y):
    time.sleep(seconds)
    return x + y


@app.task(name='rs_worker.fail')
def fail(message):
    raise ValueError(message)


@app.task(name='rs_worker.retry', bind=True, max_retries=3,
          default_retry_delay=0.2)
def retry_task(self, x):
    if self.request.retries < 2:
        raise self.retry(countdown=0.2)
    return x


@app.task(name='rs_worker.rate', rate_limit='2/s')
def rate(x):
    return x


@app.task(name='rs_worker.acks_late_fail_once', bind=True, acks_late=True,
          reject_on_worker_lost=True, max_retries=0)
def acks_late_fail_once(self, path, x):
    """Fail on the first attempt so acks_late triggers redelivery."""
    if os.path.exists(path):
        return x
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write('first\n')
    raise RuntimeError('first attempt')


@app.task(name='rs_worker.wait_file', acks_late=True)
def wait_file(path, seconds=60):
    deadline = time.monotonic() + seconds
    while not os.path.exists(path) and time.monotonic() < deadline:
        time.sleep(0.02)
    return os.path.exists(path)


@app.task(name='rs_worker.acks_late_slow', acks_late=True,
          reject_on_worker_lost=True)
def acks_late_slow(seconds, x):
    time.sleep(seconds)
    return x


@app.task(name='rs_worker.pid')
def pid():
    return os.getpid()


@app.task(name='rs_worker.allocate')
def allocate(mb):
    """Allocate *mb* MiB and return pid and size to test per-child memory limits."""
    return os.getpid(), len(bytearray(mb * 1024 * 1024))


@app.task(name='rs_worker.reject_once', bind=True, acks_late=True)
def reject_once(self, path, x):
    """Reject and requeue the message on the first attempt."""
    from celery.exceptions import Reject
    if os.path.exists(path):
        return x
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write('first\n')
    raise Reject('retry', requeue=True)


@app.task(name='rs_worker.marker_slow', acks_late=True,
          reject_on_worker_lost=True)
def marker_slow(path, seconds, x):
    """Create a start marker and sleep for shutdown tests."""
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write('started\n')
    time.sleep(seconds)
    return x
