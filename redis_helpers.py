# redis_helpers.py

import redis
import logging
import random
import json
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

redis_client = redis.Redis(
    host='backprop-bunch-redis-container',
    port=6379
    # password='cookies'
)


def get_random_redis_item():
    # redis_client.flushall()
    # Get all keys in Redis
    all_keys = redis_client.keys()
    logger.info(all_keys)

    # Filter out non-hash keys
    hash_keys = [key for key in all_keys if redis_client.type(key) == b'hash']
    logger.info(hash_keys)
    # If there are no hash keys, return None
    if not hash_keys:
        return None

    # Select a random hash key
    random_hash_key = random.choice(hash_keys)

    # Decode the hash key from bytes to a regular string
    str_hash_key = random_hash_key.decode('utf-8')
    logger.info(str_hash_key)

    # Get the hash data for the selected key
    # hash_data = redis_client.hgetall(str_hash_key)

    # Extract the item_key field from the hash and return it
    # item_key = hash_data.get(b'item_key', None)
    # string_key = item_key.decode('utf-8')
    # logger.info(string_key)
    return str_hash_key


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

