from kafka import KafkaConsumer

# Replace 'localhost:9092' with the appropriate broker address
consumer = KafkaConsumer(
    'test', # Replace with your topic name
    bootstrap_servers=['kafka:9093'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: x.decode('utf-8')
)

print("Listening for messages on topic 'test':") # Replace with your topic name
for message in consumer:
    print(f"Message received: {message.value}")
