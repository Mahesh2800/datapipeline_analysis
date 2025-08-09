from django.urls import path
from . import views

urlpatterns = [
    path('', views.api_root, name='api_root'),  # GET /api/
    path('create-order/', views.create_order, name='create_order'),  # POST /api/create-order/
    path('orders/', views.list_orders, name='list_orders'),   #get all orders
]
