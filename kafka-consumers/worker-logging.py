import json
import time
import psycopg2
from psycopg2 import sql
from confluent_kafka import Consumer
import logging
import socket
from datetime import datetime
from datetime import timedelta

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

def insert_logs(item_dict, table):
    
    cursor = conn.cursor()
    insert_sql = psycopg2.sql.SQL("INSERT INTO {} (user_id, session_id, evt_time, event_type, recommendation) VALUES ({}, {}, CAST({} AS TIMESTAMP), {}, COALESCE({}, NULL))").format(
    sql.Identifier(table),
    sql.Literal(item_dict["user_id"]),
    sql.Literal(item_dict["session_id"]),
    sql.Literal(psycopg2.TimestampFromTicks(item_dict["evt_time"])),
    sql.Literal(item_dict["evt_type"] ),
    sql.Literal(item_dict["recommendation"] if item_dict.get("recommendation") is not None else None)
    )
    cursor.execute(insert_sql)
    conn.commit()   
    cursor.close()
    
def create_partition_subtable(msg, table):
    logger.info(msg)
    msg_dict = msg
    evt_datetime = datetime.fromtimestamp(msg_dict['evt_time'])
    end_datetime = evt_datetime + timedelta(hours=1)
    cursor = conn.cursor()
    table_name = f"logs_{evt_datetime.strftime('%Y_%m_%d_%H')}"
    create_table_sql = psycopg2.sql.SQL("""
    CREATE TABLE IF NOT EXISTS {table_name} PARTITION OF {parent_table}
        FOR VALUES FROM ({start_time}) TO ({end_time});
    """).format(
    table_name=sql.Identifier(table_name),
    start_time= sql.Literal(evt_datetime.strftime('%Y-%m-%d %H:00:00')), #sql.Literal(msg_dict["evt_time"]),
    end_time=  sql.Literal(end_datetime.strftime('%Y-%m-%d %H:00:00')), #sql.Literal(evt_datetime + timedelta(hours=1)),
    parent_table = sql.Identifier(table))

    cursor.execute(create_table_sql)
    conn.commit()   
    cursor.close()

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
          create_partition_subtable(msg,"logs")
          insert_logs(msg, "logs")
          print("MSG:", msg)
        
        # {'user_id': msg[""], 'session_id': '717ddb8a-4f2a-480e-b19f-efd465b7cda2'}

if __name__ == "__main__":
  main()