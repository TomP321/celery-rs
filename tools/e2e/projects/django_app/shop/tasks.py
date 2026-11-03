import os
import time
from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from django.conf import settings
from django.core import mail
from django.core.cache import cache
from django.db import connection, transaction, IntegrityError
from django.utils import timezone, translation
from django.utils.translation import gettext as _
from .models import Item


@shared_task
def add(x, y):
    return x + y

@shared_task
def create_item(name):
    return Item.objects.create(name=name).pk

@shared_task
def count_items(prefix):
    return Item.objects.filter(name__startswith=prefix).count()

@shared_task
def db_identity(delay=0.15):
    time.sleep(delay)
    with connection.cursor() as c:
        c.execute('select pg_backend_pid()')
        return [os.getpid(), c.fetchone()[0]]

@shared_task(bind=True, max_retries=3, default_retry_delay=0)
def flaky(self, key):
    if cache.incr(key) < 3:
        raise self.retry(exc=RuntimeError('again'))
    return cache.get(key)

@shared_task
def boom():
    raise ValueError('kaboom')

@shared_task
def broken_tx(key):
    Item.objects.create(name='dup', unique_key=key)
    try:
        with transaction.atomic():
            Item.objects.create(name='dup', unique_key=key)
    except IntegrityError:
        pass
    return Item.objects.filter(unique_key=key).count()

@shared_task
def broken_query():
    with connection.cursor() as c:
        c.execute('select * from does_not_exist')

@shared_task(soft_time_limit=1)
def soft():
    try:
        time.sleep(10)
    except SoftTimeLimitExceeded:
        return 'soft-limit-handled'

@shared_task(time_limit=1)
def hard():
    time.sleep(10)

@shared_task
def cached(key, value):
    cache.set(key, value, 60)
    return cache.get(key)

@shared_task
def django_env():
    with translation.override('it'):
        word = _('Yes')
    return {'marker': settings.E2E_MARKER, 'tz': timezone.localtime().tzname() in ('CET', 'CEST'), 'word': word}

@shared_task
def send_one():
    mail.send_mail('s', 'b', 'a@e2e.test', ['b@e2e.test'])
    return len(mail.outbox) >= 1

@shared_task
def after_commit(pk):
    item = Item.objects.get(pk=pk)
    Item.objects.create(name=f'done:{item.name}')
    return item.name

@shared_task
def tick():
    return Item.objects.create(name='tick').pk

@shared_task
def total(values):
    return sum(values)

@shared_task
def pid():
    return os.getpid()
