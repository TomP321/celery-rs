"""Retry ordering with a slow, process-shared backend."""
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import Mock

import pytest

from celery import Celery
from celery.backends.base import Backend, BaseBackend
from celery.exceptions import Retry


class SlowBackend(BaseBackend):
    def _store_result(self, task_id, result, state, traceback=None, request=None):
        if state == 'RETRY':
            time.sleep(0.3)
        with open(self.app.conf.result_path, 'a') as stream:
            stream.write(json.dumps([state, str(result) if state == 'RETRY' else result]) + '\n')


def make_app(directory):
    app = Celery('retry_order', broker='filesystem://')
    app.conf.update(
        broker_transport_options={
            'data_folder_in': directory, 'data_folder_out': directory,
            'polling_interval': 0.01, 'control_folder': directory,
        },
        result_path=str(Path(directory) / 'results'),
        worker_hijack_root_logger=False,
    )
    app._backend = SlowBackend(app=app)

    @app.task(bind=True, shared=False)
    def retry_once(self):
        if not self.request.retries:
            self.retry(exc=ValueError('again'), countdown=0)
        return 42

    return app, retry_once


@pytest.mark.parametrize('pool', ['celery.concurrency.rs_prefork:RsTaskPool', 'solo'])
def test_retry_cannot_overwrite_success(tmp_path, pool):
    env = dict(os.environ, CELERY_RS_WORKER='0')
    env['PYTHONPATH'] = os.pathsep.join(filter(None, (
        str(Path(__file__).parent), env.get('PYTHONPATH'),
    )))
    script = '''
import sys
from test_retry_store_result import make_app
app, task = make_app(sys.argv[1])
app.Worker(pool=sys.argv[2], concurrency=2, without_heartbeat=True,
           without_mingle=True, without_gossip=True, loglevel='INFO').start()
'''
    with open(tmp_path / 'worker.log', 'w') as log:
        worker = subprocess.Popen([sys.executable, '-c', script, str(tmp_path), pool],
                                  env=env, stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 20
            while 'ready.' not in (tmp_path / 'worker.log').read_text():
                assert worker.poll() is None, (tmp_path / 'worker.log').read_text()
                assert time.monotonic() < deadline, (tmp_path / 'worker.log').read_text()
                time.sleep(0.02)
            app, task = make_app(str(tmp_path))
            task.delay()
            path = tmp_path / 'results'
            while not path.exists() or len(path.read_text().splitlines()) < 2:
                assert time.monotonic() < deadline, (tmp_path / 'worker.log').read_text()
                time.sleep(0.02)
            events = [json.loads(line) for line in path.read_text().splitlines()]
            assert [event[0] for event in events] == ['RETRY', 'SUCCESS'], events
            assert events[-1][1] == 42
            app.close()
        finally:
            worker.terminate()
            try:
                worker.wait(timeout=10)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait()


def test_status_parameter():
    class StatusBackend(Backend):
        def _store_result(self, task_id, result, status, traceback=None, request=None):
            self.saved = task_id, result, status

    app = Celery('status')
    backend = StatusBackend(app=app)
    assert backend.store_result('id', 42, 'SUCCESS') == 42
    assert backend.saved == ('id', 42, 'SUCCESS')


def test_store_error_context():
    from celery.exceptions import BackendStoreError

    app = Celery('store_error')
    app.conf.update(result_backend_always_retry=True, result_backend_max_retries=0)
    backend = Backend(app=app)
    backend.exception_safe_to_retry = lambda exc: True
    backend._store_result = Mock(side_effect=OSError('unavailable'))
    with pytest.raises(BackendStoreError) as caught:
        backend.store_result('task-id', 42, 'SUCCESS')
    assert caught.value.task_id == 'task-id'
    assert caught.value.state == 'SUCCESS'


@pytest.mark.parametrize('ignore,store_errors,expected', [
    (False, False, True), (True, False, False), (True, True, True),
])
@pytest.mark.parametrize('throw', [False, True])
def test_retry_stored_before_publish(ignore, store_errors, expected, throw):
    from celery.exceptions import Retry

    app = Celery('ordering', broker='memory://')

    @app.task(bind=True, shared=False, ignore_result=ignore,
              store_errors_even_if_ignored=store_errors)
    def task(self):
        pass

    backend = Mock()
    task.backend = backend
    signature = Mock()
    task.signature_from_request = Mock(return_value=signature)
    signature.apply_async.side_effect = lambda: (
        backend.mark_as_retry.call_count == int(expected)
        or pytest.fail('retry must be stored before publish'))
    task.push_request(id='id', called_directly=False, is_eager=False,
                      ignore_result=ignore, args=(), kwargs={})
    try:
        if throw:
            with pytest.raises(Retry) as caught:
                task.retry(exc=ValueError('again'), countdown=0)
            reason = caught.value
        else:
            reason = task.retry(exc=ValueError('again'), countdown=0, throw=False)
        assert backend.mark_as_retry.call_count == int(expected)
        assert getattr(reason, '_retry_stored', False) == expected
    finally:
        task.pop_request()


@pytest.mark.parametrize('eager,direct', [(True, False), (False, True)])
def test_local_retry_does_not_store(eager, direct):
    from celery.exceptions import Retry

    app = Celery('local')

    @app.task(bind=True, shared=False)
    def task(self):
        pass

    task.backend = Mock()
    task.push_request(id='id', called_directly=direct, is_eager=eager)
    try:
        with pytest.raises(Retry):
            task.retry(countdown=0)
        task.backend.mark_as_retry.assert_not_called()
    finally:
        task.pop_request()


def test_publish_failure_keeps_retry():
    from celery.exceptions import Reject

    app = Celery('publish_failure')

    @app.task(bind=True, shared=False)
    def task(self):
        pass

    task.backend = Mock()
    signature = Mock()
    signature.apply_async.side_effect = OSError('publish failed')
    task.signature_from_request = Mock(return_value=signature)
    task.push_request(id='id', called_directly=False, is_eager=False)
    try:
        with pytest.raises(Reject):
            try:
                raise ValueError('original')
            except ValueError as exc:
                task.retry(exc=exc, countdown=0)
        args = task.backend.mark_as_retry.call_args.args
        assert args[0] == 'id'
        assert isinstance(args[1], ValueError)
        assert 'ValueError: original' in args[2]
        assert 'celery.exceptions.Retry: Retry in 0s' in args[2]
    finally:
        task.pop_request()


def test_store_failure_still_publishes():
    app = Celery('store_failure')

    @app.task(bind=True, shared=False)
    def task(self):
        pass

    task.backend = Mock()
    task.backend.mark_as_retry.side_effect = OSError('backend down')
    signature = Mock()
    task.signature_from_request = Mock(return_value=signature)
    task.push_request(id='id', called_directly=False, is_eager=False)
    try:
        with pytest.raises(Retry) as info:
            task.retry(exc=ValueError('original'), countdown=0)
        signature.apply_async.assert_called_once_with()
        assert not getattr(info.value, '_retry_stored', False)
    finally:
        task.pop_request()


def test_save_group_parameter_name():
    saved = []

    class GroupBackend(Backend):
        def _save_group(self, group_id, group_result):
            saved.append((group_id, group_result))

    GroupBackend(app=Celery('save_group')).save_group('id', 'result')
    assert saved == [('id', 'result')]
