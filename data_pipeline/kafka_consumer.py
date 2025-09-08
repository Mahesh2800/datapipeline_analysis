# from kafka import KafkaConsumer
# import json

# # Initialize consumer
# consumer = KafkaConsumer(
#     'orders',
#     bootstrap_servers=['localhost:9092'],
#     auto_offset_reset='earliest',  # read from beginning if no offset committed
#     enable_auto_commit=True,
#     group_id='order-group',
#     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
# )

# print("Listening to 'orders' topic...")

# for message in consumer:
#     order = message.value
#     print(f"Received Order: {order}")
# kafka_consumer.py
from kafka import KafkaConsumer
import json
from database_writer import insert_order  # import function

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening to 'orders' topic...")

for message in consumer:
    order = message.value
    print(f"📥 Received Order: {order}")
    insert_order(order)   # save to PostgreSQL
