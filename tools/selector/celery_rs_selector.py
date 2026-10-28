"""Backend di build PEP 517 che installa la wheel precompilata di celery-rs adatta alla macchina.

Il pacchetto sorgente (sdist) di celery-rs non contiene codice da compilare: quando pip o uv lo
"costruiscono", questo backend sceglie la wheel per l'architettura corrente (x86_64 o aarch64,
Linux glibc), la scarica dal branch `wheels` del repository, ne verifica lo SHA-256 e la consegna
come wheel costruita. Nessuna toolchain Rust necessaria.
"""
import hashlib
import os
import platform
import shutil
import sys
import urllib.request

BASE_URL = 'https://github.com/TomP321/celery-rs/raw/wheels/'
VERSION = '5.7.0rc1+rs.0.1.1'
WHEELS = {
    'x86_64': ('celery-5.7.0rc1+rs.0.1.1-cp310-abi3-manylinux_2_28_x86_64.whl',
               '197de3a5e019c6b62e16c50a9db766a30a94bc646be90509972f12aa37cccf07'),
    'aarch64': ('celery-5.7.0rc1+rs.0.1.1-cp310-abi3-manylinux_2_28_aarch64.whl',
                '44c48c4277ac2b509bfbeecd23a06b9fdb1e101a7b911cd84152eab8b1590fb7'),
}
ALIASES = {'amd64': 'x86_64', 'arm64': 'aarch64'}
HERE = os.path.dirname(os.path.abspath(__file__))


def _wheel_for_this_machine():
    if not sys.platform.startswith('linux'):
        raise RuntimeError(f'celery-rs supporta solo Linux (piattaforma: {sys.platform})')
    libc, _ = platform.libc_ver()
    if libc and libc != 'glibc':
        raise RuntimeError(f'celery-rs richiede glibc ≥ 2.28 (libc trovata: {libc})')
    machine = platform.machine().lower()
    machine = ALIASES.get(machine, machine)
    if machine not in WHEELS:
        raise RuntimeError(f'nessuna wheel celery-rs per l\'architettura {machine!r} '
                           f'(disponibili: {", ".join(WHEELS)})')
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
        raise RuntimeError(f'SHA-256 della wheel {name} non corrisponde: download alterato?')
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
