"""Measure RAM (PSS) and task/s of one worker -c 10 on a real project: bench.py VENV {django,fastapi} {add,orm} N."""
import json, os, re, subprocess, sys, time
from common import session_pss
venv, fw, kind, N = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
here = os.path.dirname(os.path.abspath(__file__))
vb = f'{venv}/bin'
cwd = f'{here}/projects/' + ('django_app' if fw == 'django' else 'fastapi_app')
sys.path.insert(0, cwd)
os.environ['PYTHONPATH'] = f'{here}:{cwd}'
if fw == 'django':
    import psycopg
    if os.environ.get('E2E_CREATE'):
        with psycopg.connect('host=127.0.0.1 port=55432 user=postgres password=e2e dbname=postgres', autocommit=True) as c:
            c.execute(f"DROP DATABASE IF EXISTS {os.environ['E2E_DB']} WITH (FORCE)"); c.execute(f"CREATE DATABASE {os.environ['E2E_DB']}")
    subprocess.run([f'{vb}/python', 'manage.py', 'migrate', '--noinput'], cwd=cwd, capture_output=True)
    os.environ['DJANGO_SETTINGS_MODULE'] = 'proj.settings'
    import django; django.setup()
    from shop import tasks as T
    from proj.celery import app
    sig = {'add': T.add.s(1, 1), 'orm': T.count_items.s('x')}[kind]; A = 'proj'
else:
    import tasks as T
    app = T.app; sig = T.add.s(1, 1); A = 'tasks'
import redis
rc = redis.Redis.from_url(os.environ['E2E_RESULT']); rc.flushdb(); redis.Redis.from_url(os.environ['E2E_BROKER']).flushdb()
q = os.environ['E2E_QUEUE']
p = subprocess.Popen([f'{vb}/celery', '-A', A, 'worker', '-c', '10', '-P', 'prefork', '-l', 'ERROR', '-Q', q + '.idle', '-n', 'b@%h'], cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
time.sleep(10)
for _ in range(N):
    sig.clone().apply_async(queue=q)
idle = session_pss(p.pid)
app.control.add_consumer(q, destination=[f'b@{os.uname().nodename}'], reply=False)
t0 = time.time(); peak = 0; samples = 0
while rc.dbsize() < N and time.time() - t0 < 120:
    if samples % 3 == 0:
        peak = max(peak, session_pss(p.pid)[0])
    samples += 1; time.sleep(0.05)
dt = time.time() - t0
peak = max(peak, session_pss(p.pid)[0]); time.sleep(1); after = session_pss(p.pid)[0]
print(json.dumps({'v': os.path.basename(venv), 'fw': fw, 'kind': kind, 'task_s': round(N / dt), 'idle': idle[0], 'peak': peak, 'after': after}), flush=True)
if os.environ.get('E2E_DETAIL'):  # per-process breakdown
    for d in os.listdir('/proc'):
        if d.isdigit():
            try:
                if int(open(f'/proc/{d}/stat').read().rsplit(')', 1)[1].split()[3]) == p.pid:
                    s = open(f'/proc/{d}/smaps_rollup').read(); g = lambda k: int(re.search(rf'^{k}:\s+(\d+)', s, re.M).group(1)) // 1024
                    print(f"  {open(f'/proc/{d}/cmdline').read()[:40].replace(chr(0), ' '):40} rss={g('Rss')} pss={g('Pss')} priv={g('Private_Dirty')} shared_clean={g('Shared_Clean')} anon={g('Anonymous')}", flush=True)
            except OSError: pass
p.terminate()
try: p.wait(20)
except Exception: pass
try: os.killpg(p.pid, 9)
except OSError: pass
