"""Sonda isolata c10: stato GC e crescita del pool a regime."""
import gc
import json
import os
import sys
import time
from pathlib import Path

from celery import Celery


def probe(payload=b''):
    # Una breve pausa distribuisce le sonde su tutti i figli del pool.
    if not payload:
        time.sleep(0.02)
    return os.getpid(), gc.isenabled(), gc.get_threshold(), len(payload)


def pss(pid):
    for line in Path(f'/proc/{pid}/smaps_rollup').read_text().splitlines():
        if line.startswith('Pss:'):
            return int(line.split()[1])
    raise AssertionError(pid)


def main():
    rust = sys.argv[1] == 'rs'
    if len(sys.argv) > 2:
        gc.set_threshold(500, 7, 3)
        gc.disable()
    if rust:
        from celery.concurrency.rs_prefork import RsTaskPool as Pool
    else:
        from celery.concurrency.prefork import TaskPool as Pool
    app = Celery('c10-probe', broker='memory://')
    pool = Pool(limit=10, app=app, initargs=(app, 'c10@probe'), threads=True)
    pool.start()

    def batch(count, payload=b''):
        results = []
        for offset in range(0, count, 40):
            pending = [pool.apply_async(probe, (payload,))
                       for _ in range(min(40, count - offset))]
            deadline = time.monotonic() + 60
            while not all(result.ready() for result in pending):
                if time.monotonic() > deadline:
                    raise AssertionError('timeout pool c10')
                if rust:
                    pool.wait(0.01)
                else:
                    time.sleep(0.001)
            if not payload:
                results.extend(result.get() for result in pending)
            else:
                assert all(result.get()[3] == len(payload) for result in pending)
        return results

    try:
        samples = batch(100)
        pids = sorted({sample[0] for sample in samples})
        assert len(pids) == 10, pids
        batch(2000, b'x' * 1024)
        before = {pid: pss(pid) for pid in pids}
        batch(30000, b'x' * 1024)
        after = {pid: pss(pid) for pid in pids}
        print(json.dumps({'gc': sorted({(row[1], tuple(row[2])) for row in samples}),
                          'before_kb': before, 'after_kb': after}))
    finally:
        pool.stop()


if __name__ == '__main__':
    main()
