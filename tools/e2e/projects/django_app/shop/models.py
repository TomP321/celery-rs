from django.db import models


class Item(models.Model):
    name = models.CharField(max_length=100)
    unique_key = models.CharField(max_length=100, null=True, unique=True)
