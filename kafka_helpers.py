# kafka_helpers.py

import logging
import socket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
from confluent_kafka import Producer, KafkaError, Consumer
from threading import Thread

producer = None
#Kafka details
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
    s.close()


def create_kafka_producer(host, port):
    conf = {
        'bootstrap.servers': f'{host}:{port}',
    }
    return Producer(conf)

def get_producer():
    global producer
    while producer is None:
        try:
            producer = create_kafka_producer(host, port)
        except KafkaError as e:
            logger.info("Retrying to create Confluent Kafka producer in 5 seconds...")
            time.sleep(5)

    return producer

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def create_kafka_consumer(host, port, group_id, topic, retries=5, delay=5):
    conf = {
        'bootstrap.servers': f'{host}:{port}',
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    attempt = 0
    while attempt < retries:
        try:
            consumer = Consumer(conf)
            consumer.subscribe([topic])
            return consumer
        except KafkaError as e:
            attempt += 1
            logger.warning(f"Failed to create Kafka consumer, attempt {attempt}/{retries}. Retrying in {delay} seconds...")
            time.sleep(delay)

    logger.error(f"Failed to create Kafka consumer after {retries} attempts.")
    return None



def start_consumer_if_not_running(topic, group_id):
    global running_consumers
    if topic not in running_consumers:
        consumer = create_kafka_consumer(host, port, group_id, topic)
        if consumer is not None:
            running_consumers[topic] = consumer

            def consume():
                while True:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        logger.error(f"Consumer error: {msg.error()}")
                    else:
                        # Process the message
                        logger.info(f"Received message: {msg.value().decode('utf-8')}")

            consumer_thread = Thread(target=consume)
            consumer_thread.daemon = True
            consumer_thread.start()
        else:
            logger.warning(f"Consumer not started for topic {topic} as create_kafka_consumer returned None.")