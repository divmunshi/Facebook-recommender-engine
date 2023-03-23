import logging
import socket
from flask import Flask, request
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json
from kafka_helpers import check_connection_status, get_producer

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#Kafka details
host = 'kafka'
port = 9093

@app.route('/', methods=['GET'])
def sendid():
    user_id = request.headers.get('User-Id')
    session_id = request.headers.get('Session-Id')
    logger.info(f"User-Id: {user_id}")
    logger.info(f"Session-Id: {session_id}")
    return '241'

@app.route('/evt', methods=['POST'])
def evt():
    data = request.get_json()
    logger.info(f'Received evt POST request with data: {data}')
    headers = dict(request.headers)
    logger.info(f'Received evt POST request with data: {data}, headers: {headers}')
    # Send data to Kafka
    prod = get_producer()
    if prod is not None:
        prod.send('evt', data)  # Replace 'evt_topic' with your desired topic name
        prod.flush()
        return 'Success'
    else:
        return 'Error: No Brokers Available', 500
 

@app.route('/item', methods=['POST'])
def item():
    data = request.get_json()
    logger.info(f'Received item POST request with data: {data}')

    # Send data to Kafka
    prod = get_producer()
    if prod is not None:
        prod.send('item', data)  # Replace 'evt_topic' with your desired topic name
        prod.flush()
        return 'Success'
    else:
        return 'Error: No Brokers Available', 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
