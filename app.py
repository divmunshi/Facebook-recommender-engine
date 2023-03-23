import logging
import socket
from flask import Flask, request
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json
from kafka_helpers import check_connection_status, get_producer, delivery_report, start_consumer_if_not_running

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/', methods=['GET'])
def sendid():
    user_id = request.headers.get('User-Id')
    session_id = request.headers.get('Session-Id')
    logger.info(f"User-Id: {user_id}")
    logger.info(f"Session-Id: {session_id}")
    return '241'

@app.route('/evt', methods=['POST'])
def evt():
    cur_usr = request.headers.get("user_id")
    session = request.headers.get("session_id")
    ev_type = request.headers.get("event_type")
    msg_log = f"User: {cur_usr} Session: {session} Type: {ev_type}"
    logger.info(msg_log)
    # logger.info(f'Received evt POST request with data: {data}')
    # Send data to Kafka
    start_consumer_if_not_running('evt', 'evt_group')
    prod = get_producer()
    if prod is not None:
        message = {
            "user_id": cur_usr,
            "session_id": session,
            "event_type": ev_type
        }
        serialized_data = json.dumps(message).encode('utf-8')
        prod.produce('evt', value=serialized_data, key=None, callback=delivery_report)
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
        message = data
        # Serialize the data
        serialized_data = json.dumps(message).encode('utf-8')
        prod.produce('item', value=serialized_data, key=None, callback=delivery_report)
        prod.flush()
        return 'Success'
    else:
        return 'Error: No Brokers Available', 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

