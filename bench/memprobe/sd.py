import gc, os, sys, time, struct, collections, signal
sys.path.insert(0, os.path.dirname(__file__))
from memapp import app
import celery.concurrency.prefork
for m in ('celery.app.task','celery.canvas','celery.result','celery.backends','celery.backends.base','celery.app.trace','celery.app.amqp','billiard.util'):
    __import__(m)
app.loader.import_default_modules(); app.finalize()
gc.freeze()
def anon_ranges(pid):
    out=[]
    for l in open(f'/proc/{pid}/maps'):
        f=l.split()
        name=f[5] if len(f)>5 else ''
        if name in ('', '[heap]') and 'w' in f[1]:
            a,b=(int(x,16) for x in f[0].split('-')); out.append((a,b,name or 'anon'))
    return out
def softdirty(pid, ranges):
    pages=set()
    with open(f'/proc/{pid}/pagemap','rb') as fh:
        for a,b,_ in ranges:
            fh.seek((a>>12)*8); data=fh.read(((b-a)>>12)*8)
            for i,(e,) in enumerate(struct.iter_unpack('<Q', data)):
                if e>>63 & 1 and e>>55 & 1: pages.add((a>>12)+i)
    return pages
ranges = anon_ranges(os.getpid())
r, w = os.pipe()
open('/proc/self/clear_refs','w').write('4')
pid = os.fork()
if pid == 0:
    os.read(r, 1); os._exit(0)
os.waitpid(-1, os.WNOHANG) if False else None
time.sleep(1.0)
cpages = softdirty(pid, ranges)
ppages = softdirty(os.getpid(), ranges)
os.kill(pid, 9); os.waitpid(pid, 0)
print('child-written pages', len(cpages), 'parent-written pages', len(ppages), 'union', len(cpages|ppages))
gc.unfreeze()
objs = gc.get_objects()
seen=set(); bytype=collections.Counter(); pagetypes=collections.defaultdict(collections.Counter)
def visit(o):
    i=id(o)
    if i in seen: return
    seen.add(i); pg=i>>12
    if pg in cpages: pagetypes[pg][type(o).__name__]+=1
for o in objs:
    visit(o)
    for x in gc.get_referents(o): visit(x)
covered = set(pagetypes)
print('child pages with known objects', len(covered), 'unknown', len(cpages-covered))
agg=collections.Counter()
for pg,c in pagetypes.items():
    agg[c.most_common(1)[0][0]]+=1
print('pagine del figlio per tipo dominante:', agg.most_common(15))
# regioni delle pagine ignote
reg=collections.Counter()
for pg in cpages-covered:
    for a,b,n in ranges:
        if a>>12 <= pg < b>>12: reg[f'{n}:{(b-a)>>10}k']+=1; break
print('pagine ignote per regione:', reg.most_common(10))
