# redis_helpers.py

import redis
import logging
import random
import json
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

import os

redis_password = os.getenv('REDIS_PASSWORD')
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



redis_client = redis.Redis(
    host='backprop-bunch-redis-container',
    port=6379,
    password=redis_password
)

def add_user_history_to_redis(user_id, session_id, new_req_data):
    # check if session exists and append if it does, otherwise push
    logger.info(type(new_req_data))
    if redis_client.hexists(user_id, session_id):
        logger.info('session exists')
        # retrieve the existing session data from Redis
        existing_session_data = redis_client.hget(user_id, session_id)
        existing_session_data_list = json.loads(existing_session_data.decode('utf-8'))
        
        # append the new dictionary to the session data list
        existing_session_data_list.append(new_req_data)
        
        # update the session data in Redis
        redis_client.hmset(user_id, {session_id: json.dumps(existing_session_data_list)})
    else:
        redis_client.hmset(user_id, {session_id: json.dumps([new_req_data])})

def update_user_history_in_redis(user_id, session_id, item_id, duration_item_viewed):
    # check if session exists and update if it does, otherwise do nothing
    if redis_client.hexists(user_id, session_id):
        # retrieve the existing session data from Redis
        existing_session_data = redis_client.hget(user_id, session_id)
        existing_session_data_list = json.loads(existing_session_data.decode('utf-8'))
        # find the dictionary in the session data list where the item_id matches
        for i, req_data in enumerate(existing_session_data_list):
            logger.info(req_data.get('recommendation_key'))
            if req_data.get('recommendation_key') == item_id:
                logger.info(f"Adding duration to user {user_id} for item {item_id}")
                # update the dictionary with the new information
                existing_session_data_list[i]['duration_item_viewed'] = duration_item_viewed
                break

        # update the session data in Redis
        redis_client.hmset(user_id, {session_id: json.dumps(existing_session_data_list)})


def get_user_history_from_redis(user_id):
    session_data = {}
    for session_id in redis_client.hkeys(user_id):
        session_data[session_id.decode('utf-8')] = json.loads(redis_client.hget(user_id, session_id).decode('utf-8'))
    return session_data

def user_has_seen_item(user_id, item_id, max_sessions=10):
    session_ids = redis_client.hkeys(user_id)
    session_ids.reverse()
    for session_id in session_ids[:max_sessions]:
        session_data = json.loads(redis_client.hget(user_id, session_id).decode('utf-8'))
        for req_data in session_data:
            if req_data.get('recommendation_key') == item_id:
                logger.info('user has seen this item')
                return True
    return False


def get_random_redis_item():
    # Get all values in Redis for the "item_key" field
    all_values = redis_client.hvals('items')

    # If there are no values, return None
    if not all_values:
        return None

    # Select a random value
    random_value = random.choice(all_values)

    # Decode the value from bytes to a regular string
    str_value = random_value.decode('utf-8')
    logger.info(str_value)

    # Return the selected value
    return str_value

def postgres_to_redis_if_empty():
    logger.info(redis_client.dbsize())
    # redis_client.flushall()
    if redis_client.dbsize() == 0:
        logger.info('in here 2')
        # Redis is empty, so we need to fetch data from Postgres and save to Redis
        conn = psycopg2.connect(
            database="backprop-bunch",
            user="root",
            password="backprop",
            host="postgres",
            port="5432",
            application_name="app"
        )
        cur = conn.cursor()
        cur.execute("SELECT * FROM items ORDER BY created_at DESC LIMIT 1000")
        rows = cur.fetchall()
        # logger.info(rows)
        for row in rows:
            item_key = row[0] # assuming the first column is the key
            data = row[1:] # assuming the rest of the columns are the data
            redis_client.hset('items', 'item_key', item_key)
            logger.info('Added everything to redis')
    else:
        logger.info('Redis has data so adding nothing')


def cache_redis_data(id, data):
    # Check if data is already in Redis cache
    if isinstance(id, bytes):
            # If so, decode it to a regular string
            id = id.decode('utf-8')
    cached_response = redis_client.hgetall(id)
    logger.info(data)
    logger.info(id)

    if cached_response:
        # If data is already in Redis cache, return cached response
        return 'Exists on redis'
    else:
        # If data is not in Redis cache, process the request and cache the response
        # .... Insert Code to process the request and generate response here ...

        # Cache the response in Redis for future requests
        # Serialize the data to a JSON-formatted string
        json_data = json.dumps(data)
        redis_client.hset(id, mapping=json_data)
        # redis_client.expire(id, 3600) # set expiry time for cache to 1 hour

        # Return the response to the client
        return 'Success saving to redis'

