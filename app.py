import logging
import socket
from flask import Flask, request
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
from datetime import datetime
import json
import psycopg2
from kafka_helpers import check_connection_status, delivery_report, create_kafka_producer, create_kafka_consumer, consume
from redis_helpers import cache_redis_data, get_random_redis_item
import threading
import requests

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

# Postgres
conn = psycopg2.connect(
    database="backprop-bunch",
    user="root",
    password="backprop",
    host="postgres",
    port="5432", 
    application_name="app"
)

@app.route('/', methods=['GET'])
def sendid():
    logger.info("ITEM REQUESTED")
    user_id = request.headers.get('User-Id')
    session_id = request.headers.get('Session-Id')
    logger.info(f"User-Id: {user_id}")
    logger.info(f"Session-Id: {session_id}")
    time_stamp = time.time()

    cursor = conn.cursor()
    cursor.execute("INSERT INTO requests (user_id, session_id, time_stamp) VALUES (%s, %s,%s)",  (user_id, session_id, time_stamp))
    conn.commit()
    cursor.close()
    # random_id = get_random_redis_item()
    # if random_id is None:
    #     return '1211'
    # logger.info(f"Random id being returned: {random_id}")
    # return random_id # 
    return "123"

@app.route('/evt', methods=['POST'])
def evt():
    cur_usr = request.headers.get("user_id")
    session = request.headers.get("session_id")
    ev_type = request.headers.get("event_type")
    msg_log = f"User: {cur_usr} Session: {session} Type: {ev_type}"
    # logger.info(msg_log)
    time_stamp = time.time()

    ## send to postgres
    cursor = conn.cursor()
    cursor.execute("INSERT INTO events (user_id, session_id, time_stamp, event_type) VALUES (%s, %s,%s, %s)",  (cur_usr, session, time_stamp, ev_type))
    conn.commit()
    cursor.close()
    # logger.info(f'Received evt POST request with data: {data}')
    # Send data to Kafka
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
    logger.info("ITEM RECIEVED")
    data = request.get_json()
    msg_log = f"Received item POST request with userid: {data['user_id']} item_id: {data['item_id']} content_type: {data['content_type']} bucket_key: {data['bucket_key']} item_key: {data['item_key']}"
    logger.info(msg_log)
   
    cur_usr = data['user_id']
    itm_id = data['item_id']
    itm_key = data['item_key']
    cnt_type = data['content_type']
    bckt_key = data['bucket_key']
    time_stamp = time.time()

    cursor = conn.cursor()
    cursor.execute("INSERT INTO items (user_id, item_id, bucket_key, time_stamp, item_key, content_type) VALUES (%s, %s,%s, %s, %s, %s)",  (cur_usr, itm_id, bckt_key, time_stamp, itm_key,cnt_type))
    conn.commit()
    cursor.close()


    # redis_status = cache_redis_data(data['item_id'], json.dumps(data))

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


@app.route('/items2db')
def items2db(): 
    logger.info("SCRAPING ITEMS")
    url = "http://ec2-34-238-156-102.compute-1.amazonaws.com:7070/items"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        for item in data: 
            time_stamp =int(time.mktime(datetime.strptime(item['created_at'], '%a, %d %b %Y %H:%M:%S %Z').timetuple()))
            cursor = conn.cursor()
            cursor.execute("INSERT INTO items (user_id, bucket_key, time_stamp, item_key, content_type) VALUES (%s, %s, %s, %s, %s)",  (item['user_id'], item['bucket_key'], time_stamp, item['item_key'],item['type']))
            conn.commit()
            cursor.close()
        return "done"
    else:
        print(f"Error: {response.status_code} - {response.reason}")
        return "error"
    
@app.route('/users2db')
def users2db(): 
    logger.info("SCRAPING USERS")
    url = "http://ec2-34-238-156-102.compute-1.amazonaws.com:7070/users"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        for item in data: 
            cursor = conn.cursor()
            cursor.execute("INSERT INTO users (age, country, gender, user_id) VALUES (%s, %s, %s, %s)",  (item['age'], item['country'], item['gender'],item['id']))
            conn.commit()
            cursor.close()
        return "done"
    else:
        print(f"Error: {response.status_code} - {response.reason}")
        return "error"
    


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)



