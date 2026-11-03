import os
SECRET_KEY = 'e2e'
DEBUG = False
ALLOWED_HOSTS = ['*']
ROOT_URLCONF = 'proj.urls'
USE_TZ = True
TIME_ZONE = 'Europe/Rome'
USE_I18N = True
LANGUAGE_CODE = 'it'
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
INSTALLED_APPS = ['django.contrib.contenttypes', 'django.contrib.auth', 'django_celery_results', 'django_celery_beat', 'shop']
DATABASES = {'default': {'ENGINE': 'django.db.backends.postgresql', 'NAME': os.environ['E2E_DB'], 'USER': 'postgres',
                         'PASSWORD': 'e2e', 'HOST': '127.0.0.1', 'PORT': 55432, 'CONN_MAX_AGE': 60}}
CACHES = {'default': {'BACKEND': 'django_redis.cache.RedisCache', 'LOCATION': os.environ['E2E_CACHE']}}
EMAIL_BACKEND = 'django.core.mail.backends.locmem.EmailBackend'
CELERY_BROKER_URL = os.environ['E2E_BROKER']
CELERY_RESULT_BACKEND = os.environ['E2E_RESULT']
CELERY_RESULT_EXTENDED = True
CELERY_TIMEZONE = TIME_ZONE
CELERY_TASK_TRACK_STARTED = True
CELERY_TASK_DEFAULT_QUEUE = os.environ['E2E_QUEUE']
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True
CELERY_BEAT_SCHEDULER = 'django_celery_beat.schedulers:DatabaseScheduler'
E2E_MARKER = 'from-django-settings'
