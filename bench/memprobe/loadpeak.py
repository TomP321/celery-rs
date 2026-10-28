"""Worker -c N sotto carico continuo: PSS massima per ruolo durante l'elaborazione."""
import os, subprocess, sys, time, signal, threading
venv, conc, app_dir, ntasks = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
q = f'mlp{os.getpid()}'
redis = os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/5'
env = {k: v for k, v in os.environ.items() if not k.startswith('PYTHON')}; env['BROKER'] = redis
p = subprocess.Popen([f'{venv}/bin/celery', '-A', 'memapp', 'worker', '-P', 'prefork', '-c', conc, '-l', 'warning',
                      '-Q', q, '--without-mingle', '--without-gossip'], cwd=app_dir, env=env,
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
time.sleep(8)
def kids(pid):
    out=[]
    for t in os.listdir(f'/proc/{pid}/task'):
        try: out += [int(x) for x in open(f'/proc/{pid}/task/{t}/children').read().split()]
        except OSError: pass
    return out
def pss(pid):
    try:
        for l in open(f'/proc/{pid}/smaps_rollup'):
            if l.startswith('Pss:'): return int(l.split()[1])
    except OSError: return 0
    return 0
def sample():
    m = pss(p.pid); zs = kids(p.pid); z = sum(pss(x) for x in zs)
    ch = [c for x in zs for c in kids(x)]; c = sum(pss(x) for x in ch)
    return m, z, c, len(ch)
idle = sample()
sys.path.insert(0, app_dir); os.environ['BROKER'] = redis
from memapp import app
import redis as R
r = R.Redis.from_url(redis)
def produce():
    for i in range(ntasks): app.send_task('memapp.add', (i, i), queue=q)
th = threading.Thread(target=produce); t0 = time.time(); th.start()
peak = [0, 0, 0, 0, 0]
while True:
    m, z, c, n = sample(); tot = m + z + c
    if tot > peak[0]: peak = [tot, m, z, c, n]
    if not th.is_alive() and r.llen(q) == 0: break
    time.sleep(0.2)
el = time.time() - t0; time.sleep(3); after = sample()
f = lambda kb: f'{kb/1024:.1f}'
print(f'{venv.split("/")[-1]} idle tot={f(sum(idle[:3]))} | PICCO tot={f(peak[0])} master={f(peak[1])} zygote={f(peak[2])} figli={f(peak[3])} ({peak[4]}) | dopo tot={f(sum(after[:3]))} | {ntasks/el:.0f} task/s')
os.killpg(p.pid, signal.SIGTERM); p.wait(30)
