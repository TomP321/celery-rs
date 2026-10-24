"""Sonda di memoria/throughput per il pool (usata da ``test_worker_pool.py``).

Eseguita in un processo separato: ``python _pool_probe.py rust|prefork``.
Stampa un JSON con PSS a riposo/dopo 1000 job e job/s con task vuoti.

Importa ~30 MB di moduli **prima** di avviare il pool, così le pagine sono
condivise in copy-on-write da master, zygote (solo Rust) e figli, come in un
worker reale con l'app già caricata. Il pool del riferimento è
``concurrency.prefork.TaskPool`` con ``threads=True`` (billiard ``Pool``
sincrono, senza event loop kombu).
"""

import gc
import importlib
import json
import os
import sys
import time

CONCURRENCY = 4
JOBS = 1000


def import_heavy_modules():
    """Importa moduli pesanti (≈30 MB) e riporta il delta PSS del master."""
    candidates = (('moto', 'mypy'), ('pytest', 'redis'), ('pygments',))
    before = pss(os.getpid())
    loaded = []
    for group in candidates:
        try:
            for name in group:
                importlib.import_module(name)
        except Exception:
            continue
        loaded.extend(group)
        if pss(os.getpid()) - before >= 30_000:
            break
    gc.collect()
    return pss(os.getpid()) - before, loaded


def pss(pid):
    total = 0
    try:
        with open(f'/proc/{pid}/smaps_rollup') as handle:
            for line in handle:
                if line.startswith('Pss:'):
                    total += int(line.split()[1])
    except OSError:
        return 0
    return total


def total_pss(pids):
    return sum(pss(pid) for pid in pids)


def noop():
    return None


def main(mode):
    delta, loaded = import_heavy_modules()

    from celery import Celery

    app = Celery('probe', broker='memory://')
    app.conf.update(task_ignore_result=True)

    @app.task(shared=False, name='probe.noop')
    def task_noop():
        return None

    if mode == 'rust':
        from celery.concurrency.rs_prefork import RsTaskPool
        pool = RsTaskPool(limit=CONCURRENCY, app=app, initargs=(app, 'w1@probe'))
        pool.start()
        children = list(pool._pool.pids())
        extra = [pool._zygote_pid] if pool._zygote_pid else []
        wake = lambda: pool.wait(0.005)
        submit = lambda: pool.apply_async(task_noop, ())
    else:
        from celery.concurrency.prefork import TaskPool
        pool = TaskPool(limit=CONCURRENCY, app=app,
                        initargs=(app, 'w1@probe'), threads=True)
        pool.start()
        children = list(pool.info['processes'])
        extra = []
        wake = lambda: time.sleep(0.002)
        submit = lambda: pool.apply_async(task_noop, ())

    time.sleep(0.5)
    idle = total_pss([os.getpid(), *extra, *children])

    started = time.monotonic()
    results = [submit() for _ in range(JOBS)]
    while True:
        pending = [result for result in results if not result.ready()]
        if not pending:
            break
        wake()
        if time.monotonic() - started > 300:
            raise SystemExit('timeout: job non completati')
    elapsed = time.monotonic() - started
    time.sleep(0.3)
    loaded_pss = total_pss([os.getpid(), *extra, *children])

    pool.stop()
    print(json.dumps({
        'mode': mode,
        'module_pss_delta_kb': delta,
        'modules': loaded,
        'concurrency': CONCURRENCY,
        'jobs': JOBS,
        'jobs_per_s': JOBS / elapsed,
        'elapsed_s': elapsed,
        'pss_idle_kb': idle,
        'pss_loaded_kb': loaded_pss,
        'processes': 1 + len(extra) + len(children),
    }))


if __name__ == '__main__':
    main(sys.argv[1])
