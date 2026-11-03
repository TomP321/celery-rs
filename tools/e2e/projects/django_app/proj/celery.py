import os
from celery import Celery
from celery.signals import worker_process_init

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proj.settings')
app = Celery('proj')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()


@worker_process_init.connect
def mark_child(**kwargs):
    with open(os.path.join(os.environ['E2E_RUN'], f'child-{os.getpid()}'), 'w'):
        pass
