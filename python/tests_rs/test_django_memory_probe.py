"""Check inherited-page attribution used by the Django memory probe."""
import ctypes
import importlib.util
import mmap
import os
from pathlib import Path
import signal
import subprocess
import sys

import pytest


def _check_pages():
    path = Path(__file__).resolve().parents[2] / 'bench/memprobe/django_orm.py'
    spec = importlib.util.spec_from_file_location('django_orm_probe', path)
    probe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(probe)
    with mmap.mmap(-1, mmap.PAGESIZE, flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS) as mapping:
        mapping[0] = 1
        address = ctypes.addressof(ctypes.c_char.from_buffer(mapping))
        page = address >> 12
        ranges = [(address, address + mmap.PAGESIZE, 'anon')]
        commands_read, commands_write = os.pipe()
        replies_read, replies_write = os.pipe()
        pid = os.fork()
        if pid == 0:
            try:
                os.close(commands_write)
                os.close(replies_read)
                os.write(replies_write, b'r')
                os.read(commands_read, 1)
                mapping[0] = 2
                os.write(replies_write, b'w')
                os.read(commands_read, 1)
            finally:
                os._exit(0)
        os.close(commands_read)
        os.close(replies_write)
        try:
            assert os.read(replies_read, 1) == b'r'
            assert page in probe.shared_pages(pid, ranges)
            with open(f'/proc/{pid}/clear_refs', 'w') as target:
                target.write('4')
            assert page not in probe.softdirty(pid, ranges)
            os.write(commands_write, b'w')
            assert os.read(replies_read, 1) == b'w'
            assert page not in probe.shared_pages(pid, ranges)
            assert page in probe.softdirty(pid, ranges)
            assert mapping[0] == 1
        finally:
            os.close(commands_write)
            os.close(replies_read)
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)


@pytest.mark.skipif(sys.platform != 'linux' or not hasattr(ctypes.CDLL(None), 'mallinfo2'),
                    reason='Linux pagemap and glibc mallinfo2 are required')
def test_probe_distinguishes_shared_and_copied_pages():
    source = "import runpy; runpy.run_path(__import__('sys').argv[1])['_check_pages']()"
    process = subprocess.run([sys.executable, '-c', source, __file__],
                             capture_output=True, text=True, timeout=15)
    assert process.returncode == 0, process.stdout + process.stderr
