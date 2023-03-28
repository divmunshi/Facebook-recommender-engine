import time
from datetime import datetime
import json
import psycopg2

conn = psycopg2.connect(
    database="backprop-bunch",
    user="root",
    password="backprop",
    host="postgres",
    port="5432", 
    application_name="app"
)

def create_insert_sql(item_dict, table):
    insert_sql = psycopg2.sql.SQL("INSERT INTO logs (user_id, session_id, recieved_at, event_type) VALUES ({}, {}, {}, {})").format(
    sql.Literal(item_dict["user_id"]),
    sql.Literal(item_dict["session_id"]),
    sql.Literal(item_dict["recieved_at"]),
    sql.Literal(item_dict["event_type"] )### may have to add event type to request dict
)
    
def create_subtable_sql(item_dict, table):
    cursor = conn.cursor()
    table_name = f"requests_{item_dict['recieved_at'].strftime('%Y%m%d%H')}"
    create_table_sql = psycopg2.sql.SQL("""
    CREATE TABLE IF NOT EXISTS {table_name} (
        CHECK (recieved_at >= {start_time} AND recieved_at < {end_time})
    ) INHERITS (requests);
    """).format(
    table_name=sql.Identifier(table_name),
    start_time=sql.Literal(item_dict["recieved_at"]),
    end_time=sql.Literal(item_dict["recieved_at"] + timedelta(hours=1))
    cursor.execute(insert_sql)
    conn.commit()   
    cursor.close()
)
    

create_subtable_sql()