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
    keys = redis_client.keys()
    logger.info(keys)

    # Check if keys is empty
    if not keys:
        return None

    random_key = random.choice(keys)
    random_key_text = random_key.decode('utf-8')
    random_key_str = str(random_key_text)
    logger.info(random_key_str)

    item = redis_client.get(random_key_str)
    redis_str = item.decode('utf-8')
    redis_dict = json.loads(redis_str)
    logger.info(redis_str)
    item_id = redis_dict['item_id']
    item_id_str = str(item_id)

    return item_id_str


def cache_redis_data(id, data):
        #redis
    #Check if data is already in Redis cache
    cached_response = redis_client.get(id)
    logger.info(cached_response)
    # if you want to do it with json then 
    #cached_response = redis_client.get(json.dumps(data))

    if cached_response: 
        #If data is already in Redis cache, return cached response
        return 'Exists on redis'
    elif not cached_response:
        #If data is not in Redis cache, process the request and cache the response 
        #.... Insert Code to process the request and generate response here ...
        


        #Cache the response in Redis for future requests
        redis_client.set(id, data)
        #redis_client.set(json.dumps(data),json.dumps(response))
        redis_client.expire(id, 3600) #set expiry time for cache to 1 hour
        #redis_client.expire(json.dumps(data), 3600)

        # Return the response to the client
        return 'Success saving to redis'
    else:
        return 'Error saving to redis'

