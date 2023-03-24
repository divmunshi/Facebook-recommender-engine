import logging
import socket
from flask import Flask, request
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json
from kafka_helpers import check_connection_status, delivery_report, create_kafka_producer, create_kafka_consumer, consume
from redis_helpers import cache_redis_data, get_random_redis_item
import threading

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Check Kafka connection status
event_consumer = None
item_consumer = None
producer = None

if check_connection_status('kafka', 9093):
    producer = create_kafka_producer('kafka', 9093)
    item_consumer = create_kafka_consumer('kafka', 9093, 'item_group', 'item')
    event_consumer = create_kafka_consumer('kafka', 9093, 'evt_group', 'evt')
    # start the consumer threads
    if item_consumer is not None:
        item_thread = threading.Thread(target=consume, args=(item_consumer,))
        item_thread.start()
    if event_consumer is not None:
        event_thread = threading.Thread(target=consume, args=(event_consumer,))
        event_thread.start()

@app.route('/', methods=['GET'])
def sendid():
    # user_id = request.headers.get('User-Id')
    # session_id = request.headers.get('Session-Id')
    # logger.info(f"User-Id: {user_id}")
    # logger.info(f"Session-Id: {session_id}")
    random_id = get_random_redis_item()
    if random_id is None:
        return '1211'
    logger.info(f"Random id being returned: {random_id}")
    return random_id

@app.route('/evt', methods=['POST'])
def evt():
    cur_usr = request.headers.get("user_id")
    session = request.headers.get("session_id")
    ev_type = request.headers.get("event_type")
    msg_log = f"User: {cur_usr} Session: {session} Type: {ev_type}"
    logger.info(msg_log)
    # logger.info(f'Received evt POST request with data: {data}')
    # Send data to Kafka
    logger.info(producer)
    if producer is not None:
        message = {
            "user_id": cur_usr,
            "session_id": session,
            "event_type": ev_type
        }
        serialized_data = json.dumps(message).encode('utf-8')
        producer.produce('evt', value=serialized_data, key=None, callback=delivery_report)
        producer.flush()
        if event_consumer is not None:
            consume(event_consumer)
        return 'Success'
    else:
        return 'Error: No Brokers Available', 500
 

@app.route('/item', methods=['POST'])
def item():
    data = request.get_json()
    msg_log = f"Received item POST request with userid: {data['user_id']} item_id: {data['item_id']} content_type: {data['content_type']} bucket_key: {data['bucket_key']} item_key: {data['item_key']}"
    logger.info(msg_log)

    redis_status = cache_redis_data(data['item_id'], json.dumps(data))

    # Send data to Kafka
    if producer is not None:
        message = data
        # Serialize the data
        serialized_data = json.dumps(message).encode('utf-8')
        producer.produce('item', value=serialized_data, key=None, callback=delivery_report)
        producer.flush()
        return "Success", 200
    else:
        return (f'Error: No Brokers Available and {redis_status}'), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)



