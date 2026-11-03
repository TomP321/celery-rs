import os
from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proj.settings')
application = get_wsgi_application()
# Publish in the gunicorn master, before it forks: the producer must survive the fork.
from shop.tasks import add
add.delay(1, 1)
