# """
# URL configuration for backend project.

# The `urlpatterns` list routes URLs to views. For more information please see:
#     https://docs.djangoproject.com/en/5.1/topics/http/urls/
# Examples:
# Function views
#     1. Add an import:  from my_app import views
#     2. Add a URL to urlpatterns:  path('', views.home, name='home')
# Class-based views
#     1. Add an import:  from other_app.views import Home
#     2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
# Including another URLconf
#     1. Import the include() function: from django.urls import include, path
#     2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
# """
# # from django.contrib import admin
# # from django.urls import path, include

# # urlpatterns = [
# #     path('admin/', admin.site.urls),      # Admin panel
# #     path('api/', include('orders.urls'))  # Include app's URLs
# # ]
# from django.http import HttpResponse
# from django.contrib import admin
# from django.urls import path, include

# def home(request):
#     return HttpResponse("Welcome to the Real-Time E-commerce Data Pipeline")

# urlpatterns = [
#     path('', home),  # Root URL
#     path('admin/', admin.site.urls),
#     path('api/', include('orders.urls')),  # Include orders app URLs
# ]




# backend/urls.py (The main project URL configuration)

from django.http import HttpResponse
from django.contrib import admin
from django.urls import path, include

def home(request):
    """Simple root response for the non-API entry point."""
    return HttpResponse("Welcome to the Real-Time E-commerce Data Pipeline")

urlpatterns = [
    # 1. ROOT PATH: Serves the simple welcome message
    path('', home),
    
    # 2. ADMIN PATH
    path('admin/', admin.site.urls),
    
    # 3. API INCLUSION: All dashboard/data API calls start at /api/
    # This includes the orders app's URL patterns.
    path('api/', include('orders.urls')), 
]