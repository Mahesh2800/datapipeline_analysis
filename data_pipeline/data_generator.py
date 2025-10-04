# import time
# import random
# from kafka import KafkaProducer
# import json
# from datetime import datetime

# producer = KafkaProducer(
#     bootstrap_servers=['localhost:9092'],
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# products = [
#     "Laptop", "Tablet", "Smart Watch", "Headphones", 
#     "Bag", "Shoes", "T-shirt", "Jacket", "Mouse", 
#     "Keyboard", "Monitor", "Webcam", "Speaker", 
#     "Backpack", "Sunglasses", "Water Bottle"
# ]

# order_id = 1

# while True:
#     order = {
#         "order_id": order_id,
#         "user_id": random.randint(1000, 2000),
#         "product_name": random.choice(products),
#         "price": round(random.uniform(100, 2000), 2),
#         "quantity": random.randint(1, 5), # <-- Add quantity
#         "created_at_dt": datetime.now().isoformat()
#     }

#     producer.send("orders", order)
#     print(f"Sent: {order}")

#     order_id += 1
#     time.sleep(random.uniform(0.5, 2))  # wait between 0.5–2 sec




import time
import random
from kafka import KafkaProducer
import json
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = [
    "Laptop", "Tablet", "Smart Watch", "Headphones", 
    "Bag", "Shoes", "T-shirt", "Jacket", "Mouse", 
    "Keyboard", "Monitor", "Webcam", "Speaker", 
    "Backpack", "Sunglasses", "Water Bottle"
]

warehouses = ["WH1", "WH2", "WH3"]
payment_statuses = ["SUCCESS", "FAILED", "PENDING"]

order_id = 1
payment_id = 1

while True:
    # 1️⃣ Generate Order Event
    order = {
        "order_id": order_id,
        "user_id": random.randint(1000, 2000),
        "product_name": random.choice(products),
        "price": round(random.uniform(100, 2000), 2),
        "quantity": random.randint(1, 5),
        "created_at_dt": datetime.now().isoformat()
    }
    producer.send("orders", order)
    print(f"[Order Sent] {order}")

    # 2️⃣ Generate Payment Event
    payment = {
        "payment_id": payment_id,
        "order_id": order_id,
        "status": random.choice(payment_statuses),
        "amount": order["price"] * order["quantity"],
        "created_at": datetime.now().isoformat()
    }
    producer.send("payments", payment)
    print(f"[Payment Sent] {payment}")

    # 3️⃣ Generate Inventory Event
    inventory = {
        "product_name": order["product_name"],
        "stock_change": -order["quantity"],  # selling reduces stock
        "warehouse": random.choice(warehouses),
        "created_at": datetime.now().isoformat()
    }
    producer.send("inventory", inventory)
    print(f"[Inventory Sent] {inventory}")

    order_id += 1
    payment_id += 1

    time.sleep(random.uniform(0.5, 2))
