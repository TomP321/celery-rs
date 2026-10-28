"""Worker -c N sotto carico continuo: PSS massima per ruolo durante l'elaborazione."""
import os, subprocess, sys, time, signal, threading, argparse
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('venv')
parser.add_argument('conc')
parser.add_argument('app_dir')
parser.add_argument('ntasks', type=int)
parser.add_argument('--payload-bytes', type=int, default=0)
parser.add_argument('--broker', choices=('redis', 'amqp'), default='redis')
options = parser.parse_args()
venv, conc, app_dir, ntasks = options.venv, options.conc, options.app_dir, options.ntasks
q = f'mlp{os.getpid()}'
redis = os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/5'
broker = redis if options.broker == 'redis' else os.environ.get('CELERY_RS_AMQP_URL', 'amqp://guest:guest@127.0.0.1:5673//')
env = {k: v for k, v in os.environ.items() if not k.startswith('PYTHON')}; env['BROKER'] = broker
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
assert idle[3] == int(conc), idle
sys.path.insert(0, app_dir); os.environ['BROKER'] = broker
from memapp import app
import redis as R
r = R.Redis.from_url(redis)
def produce():
    for i in range(ntasks):
        args = ('x' * options.payload_bytes, '') if options.payload_bytes else (i, i)
        app.send_task('memapp.add', args, queue=q)
th = threading.Thread(target=produce); t0 = time.time(); th.start()
peak = [0, 0, 0, 0, 0]
while True:
    m, z, c, n = sample(); tot = m + z + c
    if tot > peak[0]: peak = [tot, m, z, c, n]
    if not th.is_alive():
        if options.broker == 'redis':
            empty = r.llen(q) == 0
        else:
            with app.connection_for_read() as conn:
                empty = conn.default_channel.queue_declare(queue=q, passive=True).message_count == 0
        if empty: break
    time.sleep(0.2)
el = time.time() - t0; time.sleep(3); after = sample()
f = lambda kb: f'{kb/1024:.1f}'
print(f'{venv.split("/")[-1]} idle tot={f(sum(idle[:3]))} ({idle[3]}) | PICCO tot={f(peak[0])} master={f(peak[1])} zygote={f(peak[2])} figli={f(peak[3])} ({peak[4]}) | dopo tot={f(sum(after[:3]))} master={f(after[0])} zygote={f(after[1])} figli={f(after[2])} | {ntasks/el:.0f} task/s')
os.killpg(p.pid, signal.SIGTERM); p.wait(30)
