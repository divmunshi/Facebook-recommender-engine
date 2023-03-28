import time
from datetime import datetime
from datetime import timedelta
import json
import psycopg2
from psycopg2 import sql
import logging

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

def insert_logs(item_dict, table):
    
    cursor = conn.cursor()
    insert_sql = psycopg2.sql.SQL("INSERT INTO {} (user_id, session_id, evt_time, event_type) VALUES ({}, {}, CAST({} AS TIMESTAMP), {})").format(
    sql.Identifier(table),
    sql.Literal(item_dict["user_id"]),
    sql.Literal(item_dict["session_id"]),
    sql.Literal(psycopg2.TimestampFromTicks(item_dict["evt_time"])),
    sql.Literal(item_dict["evt_type"] ))### may have to add event type to request dict

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

    
json_test = {'user_id': '6023', 'session_id': '9f6a7ca1-92b2-426c-9761-74d0803f82f6', 'evt_time': 1680002210.182431, 'evt_type': 'return_reco'}
create_partition_subtable(json_test, "logs")
insert_logs(json_test, "logs")