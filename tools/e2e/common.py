"""Shared helpers for the real-project checks."""
import glob, json, os, re, signal, subprocess, time

class Run:
    def __init__(self, name, cwd, env):
        self.name, self.cwd, self.env = name, cwd, env
        self.results, self.procs = [], []
        self.logdir = env['E2E_LOGS']

    def check(self, name, fn, expect=True):
        t = time.time()
        try:
            got = fn()
            ok = expect(got) if callable(expect) else got == expect
        except BaseException as e:
            got, ok = f'{type(e).__name__}: {e}'[:300], False
            if isinstance(e, KeyboardInterrupt):
                raise
        print(f"[{'OK ' if ok else 'BAD'}] {name} ({time.time()-t:.1f}s) -> {str(got)[:160]}", flush=True)
        self.results.append({'name': name, 'ok': bool(ok), 'got': repr(got)[:400]})
        return got

    def spawn(self, label, *cmd, cwd=None, env=None):
        log = open(os.path.join(self.logdir, f'{label}.log'), 'wb')
        p = subprocess.Popen(cmd, cwd=cwd or self.cwd, env={**self.env, **(env or {})}, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        p.label, p.logpath = label, log.name
        self.procs.append(p)
        return p

    def wait_log(self, p, pattern, timeout=60):
        end = time.time() + timeout
        while time.time() < end:
            if re.search(pattern, open(p.logpath, errors='replace').read()):
                return True
            if p.poll() is not None:
                raise RuntimeError(f'{p.label} exited {p.returncode}: {open(p.logpath, errors="replace").read()[-600:]}')
            time.sleep(0.1)
        raise TimeoutError(f'{p.label}: {pattern}: {open(p.logpath, errors="replace").read()[-600:]}')

    def stop(self, p, timeout=30):
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
        try:
            rc = p.wait(timeout)
        except subprocess.TimeoutExpired:
            rc = 'hung'
        self.killgroup(p)
        return rc

    def killgroup(self, p):
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass

    def cmd(self, *cmd, timeout=90, cwd=None):
        r = subprocess.run(cmd, cwd=cwd or self.cwd, env=self.env, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout + r.stderr

    def cleanup(self):
        for p in self.procs:
            if p.poll() is None:
                p.terminate()
            self.killgroup(p)

    def scan_logs(self, allowed):
        bad = []
        for path in glob.glob(os.path.join(self.logdir, '*.log')):
            for line in open(path, errors='replace'):
                if re.search(r'panicked|Segmentation|Fatal Python|core dumped|Unrecoverable|CRITICAL|double free|Bad file descriptor|Task handler raised|Exception ignored|consumer: Cannot', line) and not re.search(allowed, line):
                    bad.append(os.path.basename(path) + ': ' + line.strip()[:200])
        return bad

    def finish(self):
        path = os.path.join(os.environ['E2E_OUT'], f'{self.name}.json')
        json.dump(self.results, open(path, 'w'), indent=1)
        print(f'{self.name}: {sum(r["ok"] for r in self.results)}/{len(self.results)} ok -> {path}', flush=True)

def session_pss(sid):
    total = n = 0
    for d in os.listdir('/proc'):
        if not d.isdigit():
            continue
        try:
            stat = open(f'/proc/{d}/stat').read()
            if int(stat.rsplit(')', 1)[1].split()[3]) != sid:
                continue
            m = re.search(r'^Pss:\s+(\d+)', open(f'/proc/{d}/smaps_rollup').read(), re.M)
            total += int(m.group(1)); n += 1
        except (OSError, AttributeError, IndexError, ValueError):
            pass
    return round(total / 1024, 1), n

def wait_until(fn, timeout=30, every=0.2):
    end = time.time() + timeout
    while True:
        v = fn()
        if v or time.time() > end:
            return v
        time.sleep(every)
