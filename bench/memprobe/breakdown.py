"""Start a worker -c N and break down PSS by process and mapping."""
import os, subprocess, sys, time, collections, signal
venv, conc = sys.argv[1], sys.argv[2]
app_dir = sys.argv[3]
redis = os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390') + '/5'
env = {k: v for k, v in os.environ.items() if not k.startswith('PYTHON')}
env['BROKER'] = redis
p = subprocess.Popen([f'{venv}/bin/celery', '-A', 'memapp', 'worker', '-P', 'prefork', '-c', conc,
                      '-l', 'warning', '-Q', f'membd{os.getpid()}', '--without-mingle', '--without-gossip'],
                     cwd=app_dir, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
time.sleep(8)
if os.environ.get('NTASKS'):
    sys.path.insert(0, app_dir); os.environ['BROKER']=redis
    from memapp import app as capp
    capp.conf.task_default_queue = f'membd{os.getpid()}'
    rs=[capp.send_task('memapp.add', (i, i), queue=f'membd{os.getpid()}') for i in range(int(os.environ['NTASKS']))]
    t0=time.time()
    while time.time()-t0<60:
        import redis as _r
        n=_r.Redis.from_url(redis).llen(f'membd{os.getpid()}')
        if n==0: break
        time.sleep(0.5)
    time.sleep(4)
def kids(pid):
    out=[]
    for t in os.listdir(f'/proc/{pid}/task'):
        try: out += [int(x) for x in open(f'/proc/{pid}/task/{t}/children').read().split()]
        except OSError: pass
    return out
def walk(pid, depth=0):
    yield pid, depth
    for c in kids(pid): yield from walk(c, depth+1)
def roll(pid):
    d={}
    for l in open(f'/proc/{pid}/smaps_rollup'):
        parts=l.split()
        if len(parts)>=3 and parts[-1]=='kB': d[parts[0].rstrip(':')]=int(parts[1])
    return d
def bymap(pid):
    agg=collections.Counter(); name='?'
    for l in open(f'/proc/{pid}/smaps'):
        f=l.split()
        if '-' in f[0] and len(f)>=5 and not f[0].endswith(':'):
            name = f[5] if len(f)>5 else '[anon]'
            name = os.path.basename(name)
        elif f[0]=='Pss:': agg[name]+=int(f[1])
    return agg
tot=0; seen=[]
for pid, d in walk(p.pid):
    r=roll(pid); tot+=r['Pss']
    cmd=open(f'/proc/{pid}/cmdline').read().replace('\0',' ')[:60]
    thr=len(os.listdir(f'/proc/{pid}/task'))
    print(f"{'  '*d}{pid} pss={r['Pss']/1024:.1f} anon={r.get('Pss_Anon',0)/1024:.1f} file={r.get('Pss_File',0)/1024:.1f} privdirty={r['Private_Dirty']/1024:.1f} rss={r['Rss']/1024:.1f} thr={thr} {cmd}")
    seen.append((pid,d))
print(f"TOTAL PSS {tot/1024:.1f} MB")
for pid,d in seen[:3]:
    print('--- top mappings', pid)
    for n,v in bymap(pid).most_common(12): print(f'   {v/1024:6.2f} {n}')
os.killpg(p.pid, signal.SIGTERM); p.wait(30)
