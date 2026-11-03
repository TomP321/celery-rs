"""Basic task flows on pools, brokers and backends outside the Rust core."""
import json, os, signal, subprocess, sys, time, uuid
import urllib.request
venv, name = sys.argv[1], sys.argv[2]
here = os.path.dirname(os.path.abspath(__file__)); vb = f'{venv}/bin'; project = f'{here}/projects/other_app'
tmp = os.environ['E2E_RUN']
R = os.environ.get('CELERY_RS_REDIS_URL', 'redis://127.0.0.1:6390').rstrip('/')
rb, rr = R + '/8', R + '/9'
PG = 'postgres:e2e@127.0.0.1:55432/postgres'
db = f"e2e_{os.environ.get('CELERY_RS_LANE', '0')}_{uuid.uuid4().hex[:8]}"
for sub in ('fs', 'res', 'control'):
    os.makedirs(f'{tmp}/{sub}', exist_ok=True)
fsconf = {'broker_transport_options': {'data_folder_in': f'{tmp}/fs', 'data_folder_out': f'{tmp}/fs', 'control_folder': f'{tmp}/control'}}
s3conf = {'s3_endpoint_url': 'http://127.0.0.1:55500', 's3_bucket': 'e2e', 's3_base_path': db, 's3_access_key_id': 'x', 's3_secret_access_key': 'y', 's3_region': 'us-east-1'}
sqsconf = {'broker_transport_options': {'region': 'us-east-1', 'is_secure': False, 'polling_interval': 0.2, 'visibility_timeout': 60}}
postgres = f'db+postgresql+psycopg2://{PG}'
mongodb = f'mongodb://127.0.0.1:55017/{db}'
CASES = [
    ('pool prefork', rb, rr, {'pool': 'prefork'}),
    ('pool threads', rb, rr, {'pool': 'threads', 'par': 10}),
    ('pool solo', rb, rr, {'pool': 'solo', 'c': 1}),
    ('pool gevent', rb, rr, {'pool': 'gevent', 'c': 20, 'par': 20}),
    ('pool eventlet', rb, rr, {'pool': 'eventlet', 'c': 20, 'par': 20}),
    ('broker SQS', 'sqs://x:y@127.0.0.1:59324', rr, {'conf': sqsconf}),
    ('broker mongodb', mongodb + '_b', rr, {}),
    ('broker sqlalchemy', f'sqla+postgresql+psycopg2://{PG}', rr, {}),
    ('broker filesystem', 'filesystem://', rr, {'conf': fsconf}),
    ('backend postgres', rb, postgres, {}),
    ('backend sqlite', rb, f'db+sqlite:///{tmp}/res.sqlite', {}),
    ('backend memcached', rb, 'cache+memcached://127.0.0.1:55011/', {}),
    ('backend mongodb', rb, mongodb, {}),
    ('backend s3', rb, 's3', {'conf': s3conf}),
    # Chords fail intermittently with "GroupResult no longer exists", on the reference too.
    ('backend dynamodb', rb, f'dynamodb://x:y@localhost:55800/{db}?read=5&write=5', {'chord': '0'}),
    ('backend filesystem', rb, f'file://{tmp}/res', {}),
    ('backend rpc', rb, 'rpc://', {'chord': '0'}),
]
env0 = dict(os.environ, PYTHONPATH=project, AWS_ACCESS_KEY_ID='x', AWS_SECRET_ACCESS_KEY='y', AWS_DEFAULT_REGION='us-east-1')


def worker(label, broker, backend, options, stdout):
    queue = 'o' + uuid.uuid4().hex[:8]
    env = dict(env0, E2E_BROKER=broker, E2E_RESULT=backend, E2E_QUEUE=queue, E2E_CONF=json.dumps(options.get('conf', {})),
               E2E_CHORD=options.get('chord', '1'))
    if options.get('par'):
        env['E2E_PARALLEL'] = str(options['par'])
    proc = subprocess.Popen([f'{vb}/celery', '-A', 'otasks', 'worker', '-P', options.get('pool', 'prefork'),
                             '-c', str(options.get('c', 10 if options.get('par') else 2)), '-l', 'INFO', '-n', queue + '@%h'],
                            env=env, cwd=tmp, stdout=stdout, stderr=subprocess.STDOUT, start_new_session=True)
    time.sleep(5)
    return proc, env


def stop(proc):
    alive = proc.poll() is None
    proc.send_signal(signal.SIGTERM)
    try:
        code = proc.wait(25)
    except subprocess.TimeoutExpired:
        code = 'hung'
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except OSError:
        pass
    return code if alive else f'dead:{proc.returncode}'


def driver(script, env):
    try:
        done = subprocess.run([f'{vb}/python', f'{project}/{script}'], env=env, cwd=tmp, capture_output=True, text=True, timeout=300)
    except subprocess.TimeoutExpired:
        return {'driver': 'TIMEOUT'}
    lines = [line for line in done.stdout.splitlines() if line.startswith('JSON')]
    return json.loads(lines[0][4:]) if lines else {'driver': 'CRASH ' + done.stderr[-300:]}


EXPECT = {'add': 5, 'failure': 'kaboom', 'retry': 1, 'chain': 6, 'group': [0, 2, 4, 6, 8], 'chord': 10, 'countdown': 2, '200 tasks': True, 'stop': 0}
subprocess.run([f'{vb}/python', '-c', "import boto3; boto3.client('s3', endpoint_url='http://127.0.0.1:55500', region_name='us-east-1').create_bucket(Bucket='e2e')"], env=env0, check=True)
results = []
for label, broker, backend, options in CASES:
    with open(f"{os.environ['E2E_LOGS']}/{label.replace(' ', '_')}.log", 'wb') as log:
        proc, env = worker(label, broker, backend, options, log)
        out = driver('driver.py', env)
        out['stop'] = stop(proc)
    wrong = {k: v for k, v in out.items() if EXPECT.get(k, True) != v}
    results.append({'name': label, 'ok': not wrong, 'got': json.dumps(wrong or out)[:400]})
    print(f"[{'OK ' if not wrong else 'BAD'}] {label} {json.dumps(wrong) if wrong else ''}"[:300], flush=True)
# The first retry of a fresh worker must not leave RETRY behind a finished attempt.
for label, backend in (('postgres', postgres), ('mongodb', mongodb)):
    outcomes = []
    for _ in range(3):
        proc, env = worker(label, rb, backend, {'c': 3}, subprocess.DEVNULL)
        outcomes.append(driver('first_retry.py', env))
        stop(proc)
    ok = all(o == {'get': 1, 'state': 'SUCCESS'} for o in outcomes)
    results.append({'name': f'first retry, {label}', 'ok': ok, 'got': json.dumps(outcomes)[:400]})
    print(f"[{'OK ' if ok else 'BAD'}] first retry, {label} {json.dumps(outcomes) if not ok else ''}"[:300], flush=True)
json.dump(results, open(f"{os.environ['E2E_OUT']}/{name}.json", 'w'), indent=1)
