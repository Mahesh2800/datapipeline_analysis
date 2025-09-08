from rest_framework.decorators import api_view
from rest_framework.response import Response
from .serializers import OrderSerializer
from .models import Order
from kafka import KafkaProducer
import json


# Initialize Kafka Producer (do this only once)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Kafka broker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


@api_view(['GET'])
def api_root(request):
    return Response({"message": "Welcome to the E-commerce Data Pipeline API"})


@api_view(['POST'])
def create_order(request):
    data = request.data.copy()

    # Map incoming JSON keys to model fields
    mapped_data = {
        "order_id": data.get("order_id"),
        "user_id": data.get("user_id"),   # store user_id as customer_name for now
        "product_name": data.get("product_name"),
        #"quantity": 1,  # default since not provided
        "price": data.get("price"),
    }

    serializer = OrderSerializer(data=mapped_data)
    if serializer.is_valid():
        order = serializer.save()

        # Send order data to Kafka topic with requested keys
        producer.send('orders', {
            "order_id": order.order_id,
            "user_id": data.get("user_id"),
            "product_name": data.get("product_name"),
            "price": str(order.price),
           # "created_at": str(order.created_at),
        })
        producer.flush()
        return Response(serializer.data, status=201)
    return Response(serializer.errors, status=400)


@api_view(['GET'])
def list_orders(request):
    orders = Order.objects.all()
    serializer = OrderSerializer(orders, many=True)
    return Response(serializer.data)
