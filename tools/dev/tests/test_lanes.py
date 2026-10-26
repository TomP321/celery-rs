"""Verifica assegnazione, rientranza e pulizia dei processi con i broker reali."""
import concurrent.futures
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[3]
WRAPPER = ROOT / 'tools/dev/with-brokers'


class LanesTests(unittest.TestCase):
    def test_reentrant(self):
        result = subprocess.run([str(WRAPPER), str(WRAPPER), 'python3', '-c',
                                 'import os; print("LANE=" + os.environ["CELERY_RS_LANE"])'],
                                capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.count('corsia '), 1)
        self.assertIn('LANE=', result.stdout)

    def test_distinct_lanes(self):
        def run():
            return subprocess.run([str(WRAPPER), 'python3', '-c',
                                   'import os,time; print("LANE=" + os.environ["CELERY_RS_LANE"], flush=True); time.sleep(1)'],
                                  capture_output=True, text=True, timeout=60)
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            results = list(pool.map(lambda _: run(), range(2)))
        for result in results:
            self.assertEqual(result.returncode, 0, result.stderr)
        lanes = [next(line for line in r.stdout.splitlines() if line.startswith('LANE=')) for r in results]
        self.assertNotEqual(*lanes)

    def test_management_closes_real_exclusive_owner(self):
        code = f"""
import contextlib
import os
import runpy
import uuid
from kombu import Connection, Queue
helper = runpy.run_path({str(WRAPPER)!r})
connection = Connection(os.environ['CELERY_RS_AMQP_URL'])
Queue('lane.cleanup.' + uuid.uuid4().hex, exclusive=True, auto_delete=True)(connection).declare()
helper['clean'](os.environ)
with contextlib.suppress(Exception):
    connection.close()
"""
        result = subprocess.run([str(WRAPPER), str(ROOT / '.venv-ref/bin/python'), '-c', code],
                                capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('chiudo connessione residua', result.stdout)

    def test_reaps_daemon_before_command_exits(self):
        child_code = 'import subprocess; print(subprocess.Popen(["sleep", ".1"], start_new_session=True).pid)'
        code = f"""
import subprocess
import time
from pathlib import Path
pid = int(subprocess.check_output(['python3', '-c', {child_code!r}]))
deadline = time.monotonic() + 5
while Path(f'/proc/{{pid}}').exists() and time.monotonic() < deadline:
    time.sleep(.05)
assert not Path(f'/proc/{{pid}}').exists()
"""
        result = subprocess.run([str(WRAPPER), 'python3', '-c', code],
                                capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_interrupt_reaps_detached_child(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / 'pid'
            code = ('import subprocess,time; from pathlib import Path; '
                    'p=subprocess.Popen(["sleep","300"],start_new_session=True); '
                    f'Path({str(marker)!r}).write_text(str(p.pid)); time.sleep(300)')
            proc = subprocess.Popen([str(WRAPPER), 'python3', '-c', code],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            try:
                deadline = time.monotonic() + 60
                while not marker.exists() and time.monotonic() < deadline:
                    time.sleep(.05)
                self.assertTrue(marker.exists())
                child = int(marker.read_text())
                proc.send_signal(signal.SIGINT)
                _, stderr = proc.communicate(timeout=30)
                self.assertEqual(proc.returncode, 130, stderr.decode())
                self.assertFalse(Path(f'/proc/{child}').exists())
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=30)


if __name__ == '__main__':
    unittest.main()
