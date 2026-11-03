import json
from celery.result import AsyncResult
from django.db import transaction
from django.http import JsonResponse
from .models import Item
from .tasks import add


def enqueue(request):
    res = add.delay(int(request.GET['x']), int(request.GET['y']))
    return JsonResponse({'id': res.id})


def result(request, tid):
    res = AsyncResult(tid)
    return JsonResponse({'state': res.state, 'result': res.result if res.successful() else None})


def item(request):
    with transaction.atomic():
        obj = Item.objects.create(name=request.GET['name'])
    return JsonResponse({'pk': obj.pk})
