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
producer = None

if check_connection_status('kafka', 9093):
    producer = create_kafka_producer('kafka', 9093)

# Postgres
# conn = psycopg2.connect(
#     database="backprop-bunch",
#     user="root",
#     password="backprop",
#     host="postgres",
#     port="5432", 
#     application_name="app"
# )

@app.route('/', methods=['GET'])
def sendid():
    logger.info("ITEM REQUESTED")
    user_id = request.headers.get('User-Id')
    session_id = request.headers.get('Session-Id')
    if producer is not None:
        msg = json.dumps({
        "user_id": user_id,
        "session_id": session_id,
        "evt_time": time.time(),
        "evt_type": "ask_reco"
        })
        producer.produce('log_fct_evt', value=serialized_data, key=None, callback=delivery_report)
        producer.flush()
    logger.info(f"User-Id: {user_id}")
    logger.info(f"Session-Id: {session_id}")
    # time_stamp = time.time()
    # sql_timestamp = datetime.fromtimestamp(time_stamp).strftime('%Y-%m-%d %H:%M:%S')
    # logger.info(sql_timestamp)
    # cursor = conn.cursor()
    # cursor.execute("INSERT INTO requests (user_id, session_id, recieved_at) VALUES (%s, %s,%s)",  (str(user_id), str(session_id), sql_timestamp))
    # conn.commit()
    # cursor.close()
    random_id = get_random_redis_item()
    if producer is not None:
        msg = json.dumps({
        "user_id": user_id,
        "session_id": session_id,
        "evt_time": time.time(),
        "evt_type": "return_reco"
        })
        producer.produce('log_fct_evt', value=serialized_data, key=None, callback=delivery_report)
        producer.flush()
    if random_id is None:
        return '1211'
    logger.info(f"Random id being returned: {random_id}")
    return random_id

@app.route('/evt', methods=['POST'])
def evt():
    logger.info("EVENT RECIEVED")
    cur_usr = request.headers.get("user_id")
    session = request.headers.get("session_id")
    ev_type = request.headers.get("event_type")
    msg_log = f"User: {cur_usr} Session: {session} Type: {ev_type}"
    logger.info(msg_log)
    time_stamp = time.time()

    ## send to postgres

    # cursor = conn.cursor()
    # cursor.execute("INSERT INTO events (user_id, session_id, created_at, event_type) VALUES (%s, %s,%s, %s)",  (cur_usr, session, time_stamp, ev_type))
    # conn.commit()
    # cursor.close()

    # logger.info(f'Received evt POST request with data: {data}')
    # Send data to Kafka
    if producer is not None:
            msg = json.dumps({
            "user_id": user_id,
            "session_id": session_id,
            "evt_time": time.time(),
            "evt_type": ev_type
            })
            producer.produce('log_fct_evt', value=serialized_data, key=None, callback=delivery_report)
            producer.flush()
            return "success", 200
    else:
        return (f'Error with evt producer'), 500
 

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

    # cursor = conn.cursor()
    
    # cursor.execute("INSERT INTO items (user_id, item_id, bucket_key, created_at, item_key, content_type) VALUES (%s, %s,%s, %s, %s, %s)",  (cur_usr, itm_id, bckt_key, time_stamp, itm_key,cnt_type))
    # conn.commit()
    # cursor.close()


    redis_status = cache_redis_data(data['item_key'], data)

    # Send data to Kafka
    if producer is not None:
            msg = json.dumps({
            "user_id": cur_usr,
            "session_id": "NA",
            "evt_time": time_stamp,
            "evt_type": 'item_created'
            })
            producer.produce('log_fct_evt', value=serialized_data, key=None, callback=delivery_report)
            producer.flush()
            return "success", 200
    else:
        return (f'Error with evt producer'), 500


@app.route('/items2db')
def items2db(): 
    logger.info("SCRAPING ITEMS")
    url = "http://ec2-34-238-156-102.compute-1.amazonaws.com:7070/items"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        sql_timestamp = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        for item in data: 
            cursor = conn.cursor()
            cursor.execute("INSERT INTO items (user_id, bucket_key, created_at, item_key, content_type) VALUES (COALESCE(%s, NULL), COALESCE(%s, NULL), COALESCE(CAST(%s AS TIMESTAMP), NULL), COALESCE(%s, NULL),COALESCE(%s, NULL))",  (str(item.get('user_id')), item.get('bucket_key'), sql_timestamp, item.get('item_key'), item.get('type')))
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
            cursor.execute("INSERT INTO users (age, country, gender, user_id) VALUES (COALESCE(CAST(%s AS INTEGER), NULL), COALESCE(%s, NULL), COALESCE(%s, NULL), COALESCE(%s, NULL))",  (item.get('age'), item.get('country'), item.get('gender'), item.get('id')))
            conn.commit()
            cursor.close()
        return "done"
    else:
        print(f"Error: {response.status_code} - {response.reason}")
        return "error"
    


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)



