import time
from datetime import datetime
from datetime import timedelta
import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
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

cursor = conn.cursor()
unique_sesh_sql = psycopg2.sql.SQL("SELECT DISTINCT session_id FROM logs;")
cursor.execute(unique_sesh_sql)


cursor2 = conn.cursor() #cursor_factory=DictCursor
count = 1
for sesh in cursor.fetchall():
    if count < 2:
        print(count)
        print(sesh[0])
        sesh_logs_sql =  psycopg2.sql.SQL("SELECT * FROM logs WHERE session_id = '54b683ee-5943-438b-a04a-7ff9de14d05d' ORDER BY evt_time;").format(sesh_id=sql.Literal(sesh[0]))
        cursor2.execute(sesh_logs_sql)
        rows = cursor2.fetchall()
        columns = [desc[0] for desc in cursor2.description]
        result = []
        for row in rows:
                result.append(dict(zip(columns, row)))
        logger.info(result)
        for i in reversed(range(1,len(result))):
            logger.info(i)
            logger.info(result[i])
            iterval = "hello" # result[i]['evt_time']-result[i-1]['evt_time']
            logger.info(iterval)
    count += count

conn.commit()   
cursor.close()


