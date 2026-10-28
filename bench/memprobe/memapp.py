import os
from celery import Celery
app = Celery('memapp', broker=os.environ['BROKER'])
@app.task
def add(x, y): return x + y
