"""Il confronto conserva gli errori e distingue gli esiti JUnit."""
import importlib.util
from pathlib import Path


MODULE = Path(__file__).resolve().parents[2] / 'tools/compat/celery_compat/integration.py'
loader = importlib.util.spec_from_file_location('integration_runner', MODULE)
runner = importlib.util.module_from_spec(loader)
loader.loader.exec_module(runner)


def test_junit_outcomes(tmp_path):
    source = tmp_path / 't/integration'
    source.mkdir(parents=True)
    (source / 'test_worker.py').touch()
    xml = tmp_path / 'results.xml'
    xml.write_text('''<testsuites><testsuite>
      <testcase classname="t.integration.test_worker.TestWorker" name="test_ok"/>
      <testcase classname="t.integration.test_worker.TestWorker" name="test_skip">
        <skipped message="service unavailable"/></testcase>
      <testcase classname="t.integration.test_worker.TestWorker" name="test_fail">
        <failure message="wrong result"/></testcase>
      <testcase classname="t.integration.test_worker.TestWorker" name="test_fail">
        <error message="teardown failed"/></testcase>
      <testcase classname="t.integration.test_worker.TestWorker" name="test_fail"/>
      <testcase classname="" name="t.integration.test_database_backend">
        <error message="collection failure">ModuleNotFoundError: sqlalchemy</error></testcase>
    </testsuite></testsuites>''')
    cases = runner.outcomes(xml, tmp_path)
    prefix = 't/integration/test_worker.py::TestWorker::'
    assert cases[prefix + 'test_ok']['status'] == 'passed'
    assert cases[prefix + 'test_skip'] == {'status': 'skipped', 'detail': 'service unavailable'}
    assert cases[prefix + 'test_fail'] == {'status': 'error', 'detail': 'teardown failed'}
    collection = cases['t/integration/test_database_backend.py']
    assert collection['status'] == 'error'
    assert 'ModuleNotFoundError: sqlalchemy' in collection['detail']


def test_only_failures_against_passing_reference_are_regressions():
    reference = {node: {'status': status} for node, status in
                 [('ok', 'passed'), ('broken', 'failed'), ('absent', 'skipped'),
                  ('new_error', 'passed'), ('flaky', 'passed')]}
    candidate = {node: {'status': status} for node, status in
                 [('ok', 'passed'), ('broken', 'failed'), ('absent', 'error'),
                  ('new_error', 'error'), ('flaky', 'failed')]}
    assert runner.regressions(reference, candidate) == ['flaky', 'new_error']
    candidate['flaky']['status'] = 'passed'
    assert runner.regressions(reference, candidate) == ['new_error']


def test_locked_invocation_preserves_selection(monkeypatch):
    import argparse
    parser = argparse.ArgumentParser()
    runner.register(parser.add_subparsers(), '/ref', '/rs')
    args = parser.parse_args(['integration', '--broker', 'redis', '-k', 'leak',
                              '--', 't/integration/test_worker.py'])
    captured = []
    monkeypatch.setattr(runner, 'cmd_integration', lambda selected: captured.extend(selected.forward))
    args.fn(args)
    assert captured == ['--ref', '/ref', '--rs', '/rs', '--broker', 'redis', '-k', 'leak',
                        '--', 't/integration/test_worker.py']
