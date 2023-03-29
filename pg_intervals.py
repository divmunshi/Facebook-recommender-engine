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
unique_sesh_sql = psycopg2.sql.SQL("SELECT DISTINCT session_id FROM logs;") ### May need to select the latest partition automatically
cursor.execute(unique_sesh_sql)


cursor2 = conn.cursor() #cursor_factory=DictCursor
count = 1
for sesh in cursor.fetchall():
    if count < 2:
        print(count)
        print(sesh[0])
        sesh_logs_sql =  psycopg2.sql.SQL("SELECT * FROM logs WHERE session_id = '35a598b8-5631-4bbb-ba93-5e77e8cf85e6' ORDER BY evt_time;").format(sesh_id=sql.Literal(sesh[0]))
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
            interval = result[i]['evt_time']-result[i-1]['evt_time']
            logger.info(interval)
            # result[i]['engagement_duration'] = interval
            ### alter contents of reccomendation table where? 
            cursor3 = conn.cursor()
            sesh_logs_sql =  psycopg2.sql.SQL("INSERT INTO recommendations (session_id, user_id, engagement_duration, sent_at, item_key) VALUES({},{},{},{},{})").format(
                sql.Literal(result[i]["session_id"]),
                sql.Literal(result[i]["user_id"]),
                sql.Literal(psycopg2.TimestampFromTicks(datetime.timestamp(interval))),
                sql.Literal(result[i]["evt_time"]),
                sql.Literal(result[i]["recommendation"])
            )
            cursor3.execute()
            
            

    count += count

conn.commit()   
cursor.close()
cursor2.close()
cursor3.close()


