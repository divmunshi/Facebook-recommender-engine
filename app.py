import logging
import socket
from flask import Flask, request
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_connection_status(host, port):
    s = socket.socket()
    try:
        logger.info(f"Checking {host} connection status")
        s.connect((host, port))
    except Exception as e:
        s.close()
        logger.info(f"No, {host} connection is not yet open. Retry in 5sec.")
        time.sleep(5)
        return check_connection_status(host, port)
    logger.info(f"Ok, {host} connection is open!")
    s.close()

print('Hello')

# time.sleep(20)

# Check Kafka connection status
host = 'kafka'
port = 9093
producer = None
# check_connection_status(host, port)

# Set up Kafka producer
# producer = KafkaProducer(
#     bootstrap_servers='kafka:9092',  # Replace with your Kafka broker address
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

def create_kafka_producer(host, port):
    check_connection_status(host, port)
    return KafkaProducer(
        bootstrap_servers=f'{host}:{port}',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def get_producer():
    global producer
    while producer is None:
        try:
            producer = create_kafka_producer(host, port)
        except NoBrokersAvailable:
            logger.info("Retrying to create Kafka producer in 5 seconds...")
            time.sleep(5)
    return producer

@app.route('/', methods=['GET'])
def sendid():
    user_id = request.headers.get('User-Id')
    session_id = request.headers.get('Session-Id')
    logger.info(f"User-Id: {user_id}")
    logger.info(f"Session-Id: {session_id}")
    return '241'
# def hello_world():
#     prod = get_producer()
#     if prod is not None:
#         message = {"text": "Hello, World!"}
#         prod.send('test', message)
#         return 'Hello, World!'
#     else:
#         return 'Error: No Brokers Available', 500
# def log_request():
#     Log incoming request
#     print(f"h")

@app.route('/evt', methods=['POST'])
def evt():
    cur_usr = request.headers.get("user_id")
    session = request.headers.get("session_id")
    ev_type = request.headers.get("event_type")
    msg_log = f"User: {cur_usr} Session: {session} Type: {ev_type}"
    logger.info(msg_log)
    # logger.info(f'Received evt POST request with data: {data}')
    # Send data to Kafka
    prod = get_producer()
    if prod is not None:
        # prod.send('evt', data)  # Replace 'evt_topic' with your desired topic name
        prod.flush()
        return 'Success'
    else:
        return 'Error: No Brokers Available', 500
 

@app.route('/item', methods=['POST'])
def item():
    data = request.get_json()
    msg_log = f"Received item POST request with userid: {data['user_id']} item_id: {data['item_id']} content_type: {data['content_type']} bucket_key: {data['bucket_key']} item_key: {data['item_key']}"
    logger.info(msg_log)

    # Send data to Kafka
    prod = get_producer()
    if prod is not None:
        # prod.send('item', data)  # Replace 'evt_topic' with your desired topic name
        prod.flush()
        return 'Success'
    else:
        return 'Error: No Brokers Available', 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
