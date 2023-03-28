# redis_helpers.py

import redis
import logging
import random
import json
import psycopg2
from datetime import datetime
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

redis_client = redis.Redis(
    host='backprop-bunch-redis-container',
    port=6379
    # password='cookies'
)


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

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

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
        redis_client.expire(id, 3600) # set expiry time for cache to 1 hour

        # Return the response to the client
        return 'Success saving to redis'

