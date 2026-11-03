from django.test import TestCase, override_settings
from celery import current_app
from .models import Item
from .tasks import add, create_item


class EagerTests(TestCase):
    def test_apply(self):
        self.assertEqual(add.apply(args=(2, 3)).get(), 5)

    def test_eager_orm(self):
        current_app.conf.task_always_eager = True
        try:
            pk = create_item.delay('eager').get()
        finally:
            current_app.conf.task_always_eager = False
        self.assertTrue(Item.objects.filter(pk=pk).exists())

    def test_on_commit_captured(self):
        with self.captureOnCommitCallbacks() as callbacks:
            Item.objects.create(name='sig:cap')
        self.assertEqual(len(callbacks), 1)
