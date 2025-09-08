from django.urls import path
from . import views

urlpatterns = [
    path('', views.api_root, name="api_root"),
    path('orders/', views.list_orders, name='list_orders'),   # GET all orders
    path('orders/create/', views.create_order, name='create_order'),  # POST new order
]
