import json, os, sys
sys.path.insert(0, os.path.dirname(__file__))
import otasks as T
from celery.result import AsyncResult
T.add.delay(2, 3).get(timeout=20)
r = T.retry_once.delay()
try:
    value = r.get(timeout=6)
except Exception as e:
    value = type(e).__name__
print('JSON' + json.dumps({'get': value, 'state': AsyncResult(r.id, app=T.app).state}))
