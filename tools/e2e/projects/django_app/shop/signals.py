from django.db.models.signals import post_save
from django.dispatch import receiver
from .models import Item


@receiver(post_save, sender=Item)
def on_item(sender, instance, created, **kwargs):
    if created and instance.name.startswith('sig:'):
        from .tasks import after_commit
        after_commit.delay_on_commit(instance.pk)
