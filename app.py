import logging
import socket
from flask import Flask, request
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json
from kafka_helpers import check_connection_status, get_producer
import psycopg2

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#Kafka details
host = 'kafka'
port = 9093

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
    cursor.execute("SELECT item_key FROM items  ORDER BY time_stamp DESC LIMIT 1")
    item_key_reccomended = cursor.fetchall()
    item_key_reccomended = item_key_reccomended[0][0]
    # logger.info(f"Reccomended ID: {item_key_reccomended} type {type(item_key_reccomended)}")
    item_key_reccomended = item_key_reccomended.strip()

    logger.info(item_key_reccomended)

    conn.commit()
    cursor.close()
    
    return  "9999" #item_key_reccomended

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
    
    # cursor.execute("INSERT INTO events (user_id, session_id, time_stamp, event_type) VALUES (%s, %s,%s, %s)",  (cur_usr, session, time_stamp, ev_type))
    conn.commit()
    cursor.close()
    return "100"
    # logger.info(f'Received evt POST request with data: {data}')
    # Send data to Kafka
    # prod = get_producer()
    # if prod is not None:
    #     # prod.send('evt', data)  # Replace 'evt_topic' with your desired topic name
    #     prod.flush()
    #     return 'Success'
    # else:
    #     return 'Error: No Brokers Available', 500
 

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
    
    # cursor.execute("INSERT INTO items (user_id, item_id, bucket_key, time_stamp, item_key, content_type) VALUES (%s, %s,%s, %s, %s, %s)",  (cur_usr, itm_id, bckt_key, time_stamp, itm_key,cnt_type))
    conn.commit()
    cursor.close()


    # Send data to Kafka
    # prod = get_producer()
    # if prod is not None:
    #     # prod.send('item', data)  # Replace 'evt_topic' with your desired topic name
    #     prod.flush()
    #     return 'Success'
    # else:
    #     return 'Error: No Brokers Available', 500
    return "240"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
