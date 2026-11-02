"""Select a prebuilt celery-rs wheel as a PEP 517 backend.

The sdist contains no compilable source. Download the Linux glibc wheel for
x86_64 or aarch64 from the wheels branch, verify SHA-256 and return it as
the build result. No Rust toolchain is needed."""
import hashlib
import os
import platform
import shutil
import sys
import urllib.request

BASE_URL = 'https://github.com/TomP321/celery-rs/raw/wheels/'
VERSION = '5.7.0rc1+rs.0.1.4'
WHEELS = {
    'x86_64': ('celery-5.7.0rc1+rs.0.1.4-cp310-abi3-manylinux_2_28_x86_64.whl',
               'a0ea644e6c659b4993a0bfe1b6e7eff2c69e3c5402ef18a5a0a1c8d3139d95bb'),
    'aarch64': ('celery-5.7.0rc1+rs.0.1.4-cp310-abi3-manylinux_2_28_aarch64.whl',
                '21c03c1579488774ffcac77f5e40e6ba98f7fed4d92b48a99aa46bb7251788e6'),
}
ALIASES = {'amd64': 'x86_64', 'arm64': 'aarch64'}
HERE = os.path.dirname(os.path.abspath(__file__))


def _wheel_for_this_machine():
    if not sys.platform.startswith('linux'):
        raise RuntimeError(f'celery-rs supports Linux only (platform: {sys.platform})')
    libc, _ = platform.libc_ver()
    if libc and libc != 'glibc':
        raise RuntimeError(f'celery-rs requires glibc ≥ 2.28 (found libc: {libc})')
    machine = platform.machine().lower()
    machine = ALIASES.get(machine, machine)
    if machine not in WHEELS:
        raise RuntimeError(f'no celery-rs wheel for architecture {machine!r} '
                           f'(available: {", ".join(WHEELS)})')
    return WHEELS[machine]


def _download(name, sha256, directory):
    url = BASE_URL + name.replace('+', '%2B')
    target = os.path.join(directory, name)
    digest = hashlib.sha256()
    with urllib.request.urlopen(url, timeout=120) as response, open(target, 'wb') as out:
        while chunk := response.read(1 << 20):
            digest.update(chunk)
            out.write(chunk)
    if digest.hexdigest() != sha256:
        os.unlink(target)
        raise RuntimeError(f'wheel {name} SHA-256 mismatch: corrupted download?')
    return name


def get_requires_for_build_wheel(config_settings=None):
    return []


def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):
    dist_info = f'celery-{VERSION}.dist-info'
    target = os.path.join(metadata_directory, dist_info)
    os.makedirs(target, exist_ok=True)
    shutil.copy(os.path.join(HERE, 'METADATA'), os.path.join(target, 'METADATA'))
    return dist_info


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    name, sha256 = _wheel_for_this_machine()
    return _download(name, sha256, wheel_directory)
