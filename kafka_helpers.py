# kafka_helpers.py

import logging
import socket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

producer = None

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
    check_connection_status(host, port)
    return KafkaProducer(
        bootstrap_servers=f'{host}:{port}',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def get_producer():
    global producer
    while producer is None:
        try:
            producer = create_kafka_producer(host, port)
        except NoBrokersAvailable:
            logger.info("Retrying to create Kafka producer in 5 seconds...")
            time.sleep(5)
    return producer