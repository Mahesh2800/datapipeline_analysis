from rest_framework.decorators import api_view
from rest_framework.response import Response
from .serializers import OrderSerializer
from .models import Order


@api_view(['GET'])
def api_root(request):
    return Response({"message": "Welcome to the E-commerce Data Pipeline API"})


@api_view(['POST'])
def create_order(request):
    serializer = OrderSerializer(data=request.data)
    if serializer.is_valid():
        serializer.save()
        return Response({"message": "Order saved successfully!"})
    return Response(serializer.errors, status=400)


@api_view(['GET'])
def list_orders(request):
    orders = Order.objects.all()
    serializer = OrderSerializer(orders, many=True)
    return Response(serializer.data)
