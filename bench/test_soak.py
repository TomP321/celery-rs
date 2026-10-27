"""Il gate deve rilevare pendenza, picchi nei figli e crescita di fd/thread."""
from soak import analyze


def samples(delta=0, fd_delta=0, thread_delta=0):
    return [{'seconds': t, 'processes': [
        {'pid': 1, 'start': '1', 'role': 'worker-child', 'pss_kib': 10240 + i * delta,
         'fds': 5 + i * fd_delta, 'threads': 1 + i * thread_delta}]
    } for i, t in enumerate((0, 300, 1800, 3600))]


def test_analysis_ignores_warmup_and_accepts_stable_process():
    row, = analyze(samples())
    assert row['passed'] and row['samples'] == 3
    assert row['slope_mib_hour'] == 0


def test_analysis_detects_growth_and_resource_leaks():
    for values in (samples(delta=2048), samples(fd_delta=1), samples(thread_delta=1)):
        row, = analyze(values)
        assert not row['passed']


def test_analysis_rejects_short_run():
    assert analyze(samples()[:1]) == []
