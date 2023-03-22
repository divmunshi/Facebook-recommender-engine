from kafka import KafkaConsumer

# Replace 'localhost:9092' with the appropriate broker address
consumer = KafkaConsumer(
    'your_topic_name', # Replace with your topic name
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: x.decode('utf-8')
)

print("Listening for messages on topic 'your_topic_name':") # Replace with your topic name
for message in consumer:
    print(f"Message received: {message.value}")
from kafka import KafkaConsumer

# Replace 'localhost:9092' with the appropriate broker address
consumer = KafkaConsumer(
    'your_topic_name', # Replace with your topic name
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: x.decode('utf-8')
)

print("Listening for messages on topic 'your_topic_name':") # Replace with your topic name
for message in consumer:
    print(f"Message received: {message.value}")
