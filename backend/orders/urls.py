# orders/urls.py

from django.urls import path
from . import views

urlpatterns = [
    # ----------------------------------------------------
    # CORE API ENDPOINTS
    # ----------------------------------------------------
    # Serves: /api/
    path('', views.api_root, name="api_root"), 
    
    # Serves: /api/orders/create/
    path('orders/create/', views.create_order, name='create_order'), 
    
    # ----------------------------------------------------
    # DASHBOARD METRICS ENDPOINTS
    # ----------------------------------------------------
    # Serves: /api/metrics/summary/
    path('metrics/summary/', views.get_summary_metrics, name='summary_metrics'),
    
    # Serves: /api/metrics/top-products/
    path('metrics/top-products/', views.get_top_products, name='top_products'),
    
    # Serves: /api/metrics/fraud-alerts/
    path('metrics/fraud-alerts/', views.get_fraud_alerts, name='fraud_alerts'),
    
    # ----------------------------------------------------
    # GENERATIVE AI ENDPOINT (NEW)
    # ----------------------------------------------------
    # Serves: /api/metrics/recommend/{user_id}/ (e.g., /api/metrics/recommend/1001/)
    # The <int:user_id> captures the ID from the URL and passes it to the view function.
    path('metrics/recommend/<int:user_id>/', views.get_recommendations, name='get_recommendations'),
]