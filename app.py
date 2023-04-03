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
from redis_helpers import cache_redis_data, get_random_redis_item, postgres_to_redis_if_empty, add_user_history_to_redis, get_user_history_from_redis, user_has_seen_item, update_user_history_in_redis, add_item_to_redis, update_item_duration, get_item_from_redis, get_random_popular_item, get_user_recommendation_keys
import requests

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Check Kafka connection status
producer = None

if check_connection_status('kafka', 9093):
    producer = create_kafka_producer('kafka', 9093)


@app.route('/testtest')
def redistests():
    update_item_duration("19051991-a748-429c-9007-92ef5af16531", 500)
    item = get_item_from_redis("19051991-a748-429c-9007-92ef5af16531")
    popular_item = get_random_popular_item(set(
        ["19051991-a748-429c-9007-92ef5af16531", "eb69db11-ed11-4689-af4e-95c5dcc58eb9"]))
    logger.info(f"popular_item is {popular_item}")
    # logger.info(f"pop item is {popular_item}")
    # random_item = get_random_redis_item()
    # logger.info(f"random item is {random_item}")

    # postgres_to_redis_if_empty()
    # user_id = 'brvhjberjh'
    # session_id = 'hwevui2448738'
    # update_user_history_in_redis(user_id, session_id, "gjhddevwjw", 200)
    # user_history = get_user_history_from_redis(36388)
    # logger.info(user_history)
    # # logger.info(user_history)
    # item_seen = user_has_seen_item(user_id, "gewhvfejw", max_sessions=10)
    # logger.info(f"item seen is {item_seen}")
    # new_req_data = {
    #     "evt_time": time.time(),
    #     "recommendation_time": time.time(),
    #     "recommendation_key": "gjhddevwjw"
    #     }
    # add_user_history_to_redis(user_id, session_id, new_req_data)
    return 'ok', 200


@app.route('/')
def sendid():
    time_requested = time.time()
    # postgres_to_redis_if_empty()
    # logger.info("ITEM REQUESTED")
    user_id = request.headers.get('User-Id')
    session_id = request.headers.get('Session-Id')
    # logger.info(f"User-Id: {user_id}")
    # logger.info(f"Session-Id: {session_id}")
    user_has_seen = get_user_recommendation_keys(user_id, max_sessions=30)
    # logger.info(f"User has seen these: {user_has_seen}")

    if producer is not None:
        msg = json.dumps({
            "user_id": user_id,
            "session_id": session_id,
            "evt_time": time_requested,
            "evt_type": "ask_reco"
        })
        producer.produce('log_fct_evt', value=msg,
                         key=None)
        producer.flush()
        # popular item
    # item_to_return = get_random_popular_item(
    #     exclude_item_ids=set(user_has_seen))
    # logger.info(f"popular item: {item_to_return}")
    # if item_to_return is None:
    logger.info('getting random instead')
    item_to_return = get_random_redis_item(excluded_keys=user_has_seen)

    recommendation_time = time.time()
    if producer is not None:
        msg = json.dumps({
            "user_id": user_id,
            "session_id": session_id,
            "evt_time": recommendation_time,
            "evt_type": "return_reco",
            "recommendation": item_to_return
        })
        producer.produce('log_fct_evt', value=msg,
                         key=None)
        producer.flush()
    if item_to_return is None:
        return '0f7b1746-068c-4111-9237-4473519d1135'

    logger.info(f"Id being returned: {item_to_return}")
    new_req_data = {
        "time_requested": time_requested,
        "recommendation_time": recommendation_time,
        "recommendation_key": item_to_return
    }
    add_user_history_to_redis(user_id, session_id, new_req_data)
    return item_to_return


@app.route('/evt', methods=['POST'])
def evt():
    logger.info("EVENT RECIEVED")
    cur_usr = request.headers.get("user_id")
    session = request.headers.get("session_id")
    ev_type = request.headers.get("event_type")
    msg_log = f"User: {cur_usr} Session: {session} Type: {ev_type}"
    logger.info(msg_log)
    time_stamp = time.time()
    # Send data to Kafka
    if producer is not None:
        msg = json.dumps({
            "user_id": request.headers.get("user_id"),
            "session_id": session,
            "evt_time": time.time(),
            "evt_type": ev_type
        })
        producer.produce('log_fct_evt', value=msg,
                         key=None, callback=delivery_report)
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

    # redis_status = cache_redis_data(data['item_key'], data)

    data = {
        "bucket_key": bckt_key,
        "created_at": time_stamp,
        "item_key": data['item_key'],
        "content_type": data['content_type'],
        "user_id": data['user_id']
    }

    add_item_to_redis(data)

    # Send data to Kafka
    if producer is not None:
        msg = json.dumps({
            "user_id":  data['user_id'],
            "session_id": "NA",
            "evt_time": time_stamp,
            "evt_type": 'item_created'
        })
        producer.produce('log_fct_evt', value=msg,
                         key=None, callback=delivery_report)
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
        sql_timestamp = datetime.fromtimestamp(
            time.time()).strftime('%Y-%m-%d %H:%M:%S')
        for item in data:
            add_item_to_redis(item)
            logger.info(item)
            # cursor = conn.cursor()
            # cursor.execute("INSERT INTO items (user_id, bucket_key, created_at, item_key, content_type) VALUES (COALESCE(%s, NULL), COALESCE(%s, NULL), COALESCE(CAST(%s AS TIMESTAMP), NULL), COALESCE(%s, NULL),COALESCE(%s, NULL))",  (str(
            #     item.get('user_id')), item.get('bucket_key'), sql_timestamp, item.get('item_key'), item.get('type')))
            # conn.commit()
            # cursor.close()
        return "done"
    else:
        #     print(f"Error: {response.status_code} - {response.reason}")
        return "error"


@app.route('/users2db')
def users2db():
    logger.info("SCRAPING USERS")
    url = "http://ec2-34-238-156-102.compute-1.amazonaws.com:7070/users"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        for item in data:
            # add_item_to_redis(item)
            cursor = conn.cursor()
            cursor.execute("INSERT INTO users (age, country, gender, user_id) VALUES (COALESCE(CAST(%s AS INTEGER), NULL), COALESCE(%s, NULL), COALESCE(%s, NULL), COALESCE(%s, NULL))",  (item.get(
                'age'), item.get('country'), item.get('gender'), item.get('id')))
            conn.commit()
            cursor.close()
        return "done"
    else:
        print(f"Error: {response.status_code} - {response.reason}")
        return "error"


if __name__ == '__main__':

    app.run(host='0.0.0.0', port=8080)
