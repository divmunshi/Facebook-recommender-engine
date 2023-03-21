import logging
from flask import Flask, request
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print('Hello')

time.sleep(20)

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',  # Replace with your Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/', methods=['GET', 'POST'])
def hello_world():
    try:
        message = {"text": "Hello, World!"}
        producer.send('test', message)
        return 'Hello, World!'
    except NoBrokersAvailable:
        return 'Error: No Brokers Available', 500
def log_request():
    # Log incoming request
    print(f"Incoming request: {request}")
    return 'OK'

@app.route('/evt', methods=['POST'])
def evt():
    data = request.get_json()
    logger.info(f'Received evt POST request with data: {data}')

    # Send data to Kafka
    try:
        producer.send('test', data)  # Replace 'evt_topic' with your desired topic name
        producer.flush()
    except NoBrokersAvailable:
        return 'Error: No Brokers Available', 500

    return 'Success'

@app.route('/item', methods=['POST'])
def item():
    data = request.get_json()
    logger.info(f'Received item POST request with data: {data}')

    # Send data to Kafka
    producer.send('item_topic', data)  # Replace 'item_topic' with your desired topic name
    producer.flush()

    return 'Success'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
