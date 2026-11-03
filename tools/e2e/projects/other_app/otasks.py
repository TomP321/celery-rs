import os, time
from celery import Celery
app = Celery('o', broker=os.environ['E2E_BROKER'], backend=os.environ['E2E_RESULT'] or None)
app.conf.update(task_default_queue=os.environ['E2E_QUEUE'], broker_connection_retry_on_startup=True, worker_prefetch_multiplier=1)
for k, v in __import__('json').loads(os.environ.get('E2E_CONF', '{}')).items():
    setattr(app.conf, k, v)
@app.task
def add(x, y): return x + y
@app.task
def total(v): return sum(v)
@app.task
def boom(): raise ValueError('kaboom')
@app.task(bind=True, max_retries=2, default_retry_delay=0)
def retry_once(self):
    if self.request.retries < 1:
        raise self.retry(exc=RuntimeError('again'))
    return self.request.retries
@app.task
def nap(s):
    time.sleep(s); return s
@app.task(bind=True, max_retries=2)
def retry_cd(self, cd):
    if self.request.retries < 1:
        raise self.retry(exc=RuntimeError('again'), countdown=cd)
    return self.request.retries
