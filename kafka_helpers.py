# kafka_helpers.py

import logging
import socket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import threading
from confluent_kafka import Producer, KafkaError, Consumer
from threading import Thread

producer = None
# Kafka details
host = 'kafka'
port = 9093
running_consumers = {}

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_connection_status(host, port):
    s = socket.socket()
    try:
        logger.info(f"Checking {host} connection status")
        s.connect((host, port))
    except Exception as e:
        s.close()
        logger.info(f"No, {host} connection is not yet open. Retry in 5sec.")
        time.sleep(5)
        return check_connection_status(host, port)
    logger.info(f"Ok, {host} connection is open!")
    return True
    s.close()


def create_kafka_producer(host, port):
    conf = {
        'bootstrap.servers': f'{host}:{port}',
    }
    return Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(
            f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')


def create_kafka_consumer(host, port, group_id, topic):
    conf = {
        'bootstrap.servers': f'{host}:{port}',
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    return consumer


def consume(consumer):
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error(f"Consumer error: {msg.error()}")
        else:
            logging.info(f"Received message: {msg.value().decode('utf-8')}")
            # if redis_type == 'item'
            # cache_redis_data(data['user_id'], json.dumps(data))
