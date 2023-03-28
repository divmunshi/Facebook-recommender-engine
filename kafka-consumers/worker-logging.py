import json
import time
import psycopg2
from confluent_kafka import Consumer
import logging
import socket
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

conn = psycopg2.connect(
    database="backprop-bunch",
    user="root",
    password="backprop",
    host="postgres",
    port="5432", 
    application_name="app"
)

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

if check_connection_status('kafka', 9093):

    kafka_params = {
    'bootstrap.servers': 'kafka:9093',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
    }

    c = Consumer(kafka_params)
    c.subscribe(['log_fct_evt'])

    def main():
      while True:
          msg = c.poll(2.0)
          if msg is None: continue
          if msg.error(): continue
          msg_raw = msg.value().decode('utf-8')
          msg = json.loads(msg_raw)
          print("MSG:", msg)
        
        # {'user_id': msg[""], 'session_id': '717ddb8a-4f2a-480e-b19f-efd465b7cda2'}

if __name__ == "__main__":
  main()