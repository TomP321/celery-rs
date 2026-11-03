from django.urls import path
from shop import views

urlpatterns = [path('enqueue', views.enqueue), path('result/<str:tid>', views.result), path('item', views.item)]
