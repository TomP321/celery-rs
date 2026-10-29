"""Bounded resource reuse and cooperative waiting on latency paths."""
from types import SimpleNamespace

from celery import Celery
from celery.app import _rs_control, _rs_producer
from celery.backends.asynchronous import BaseResultConsumer


def test_control_reuses_pool_descriptor(monkeypatch):
    app = Celery('control-descriptor', broker='amqp://guest:guest@localhost//',
                 set_as_current=False)
    original = _rs_producer.fast_connection
    calls = []

    def connect(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(_rs_producer, 'fast_connection', connect)
    try:
        first = _rs_control.fast_connection(app)
        for _ in range(20):
            assert _rs_control.fast_connection(app) is first
        assert first is app.amqp.producer_pool.connections.connection
        assert len(calls) == 1
        first.transport_options['unsupported-option'] = True
        assert _rs_control.fast_connection(app) is None
        first.transport_options.clear()
        monkeypatch.setenv('CELERY_RS_PRODUCER', '0')
        assert _rs_control.fast_connection(app) is None
    finally:
        app.close()


def test_control_respects_custom_connection():
    app = Celery('custom-control', broker='amqp://guest:guest@localhost//',
                 set_as_current=False)
    try:
        app.connection_for_write = lambda: None
        assert _rs_control.fast_connection(app) is None
        assert app.amqp._producer_pool is None
    finally:
        app.close()


def test_wait_yields_cpu_only_while_pending(monkeypatch):
    from celery.backends import asynchronous

    consumer = object.__new__(BaseResultConsumer)
    consumer.on_message = None
    consumer.on_wait_for_pending = lambda *args, **kwargs: None
    result = SimpleNamespace(on_ready=SimpleNamespace(ready=False))
    calls = []
    callback = lambda *args: None

    def drain(promise, **kwargs):
        assert kwargs['on_interval'] is callback
        yield
        promise.ready = True
        yield

    consumer.drain_events_until = drain
    monkeypatch.setattr(asynchronous, 'sleep', calls.append)
    assert list(consumer._wait_for_pending(result, on_interval=callback)) == [None, None]
    assert calls == [0]
    assert consumer.on_message is None
