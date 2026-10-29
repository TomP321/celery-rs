"""Compare child initialization prepared in the zygote with the reference."""
import json
import os
import subprocess
import sys

import pytest

SOURCE = '''
import json, os, time
from celery import Celery, signals
from celery.concurrency import prefork
from celery.concurrency.rs_prefork import RsTaskPool
from celery.loaders.app import AppLoader

class Loader(AppLoader):
    def on_worker_process_init(self):
        # User hook registering a task in the child after preparation.
        @self.app.task(name='late')
        def late():
            return 'late'

app = Celery('prepared', broker='memory://', loader=Loader)

@app.task(name='early')
def early():
    return 'early'

app.finalize()
init_pid = None

@signals.worker_process_init.connect(weak=False)
def initialized(**kwargs):
    global init_pid
    init_pid = os.getpid()

def state():
    prepared = prefork._prepared
    return dict(pid=os.getpid(), init_pid=init_pid,
                prepared=prepared is not None,
                early_shared=(prepared is not None and
                              app.tasks['early'].__trace__ is prepared[2]['early']),
                late_traced=callable(getattr(app.tasks['late'], '__trace__', None)))

pool = RsTaskPool(1, app=app, initargs=(app, 'prepared@test'))
pool.start()
try:
    result = pool.apply_async(state)
    deadline = time.monotonic() + 15
    while not result.ready():
        assert time.monotonic() < deadline
        pool.wait(.01)
    print(json.dumps(dict(parent=os.getpid(), child=result.get())))
finally:
    pool.stop()
'''


def run(**environment):
    env = {k: v for k, v in os.environ.items() if not k.startswith('CELERY_LOG')}
    env.update(environment)
    process = subprocess.run([sys.executable, '-c', SOURCE], env=env,
                             capture_output=True, text=True, timeout=60)
    assert process.returncode == 0, process.stdout + process.stderr
    return json.loads(process.stdout.splitlines()[-1])


def test_prepared_in_zygote_keeps_child_initializer_semantics():
    data = run()
    child = data['child']
    assert child['pid'] != data['parent']
    assert child['init_pid'] == child['pid']
    assert child['prepared']
    assert child['early_shared']
    assert child['late_traced']


@pytest.mark.parametrize('logfile', ['worker-%i.log', 'worker-%I.log'])
def test_per_child_logfile_is_configured_in_child(tmp_path, logfile):
    data = run(CELERY_LOG_FILE=str(tmp_path / logfile))
    child = data['child']
    assert child['init_pid'] == child['pid']
    assert not child['prepared']
    assert child['late_traced']
