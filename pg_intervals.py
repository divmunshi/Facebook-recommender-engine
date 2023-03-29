import time
from datetime import datetime
from datetime import timedelta
import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
import logging
from redis_helpers import update_user_history_in_redis
### RUN THIS FILE EVERY HOUR FROM APACHE AIRFLOW ###
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

### Get the Session_Ids from postgres ### 
cursor = conn.cursor()
unique_sesh_sql = psycopg2.sql.SQL("SELECT DISTINCT session_id FROM logs WHERE session_id IS NOT NULL AND evt_time > NOW() - interval '1 hour';") ### May need to select the latest partition automatically
cursor.execute(unique_sesh_sql)

cursor2 = conn.cursor() 
cursor3 = conn.cursor()
count = 1

### Iterate through session_ids to get all logs, calculate engagement time and upload to reccomendations ### 
for sesh in cursor.fetchall():
    print(count)
    print(sesh[0])
    sesh_logs_sql =  psycopg2.sql.SQL("SELECT * FROM logs WHERE session_id = {sesh_id} ORDER BY evt_time;").format(sesh_id=sql.Literal(sesh[0]))
    cursor2.execute(sesh_logs_sql)
    rows = cursor2.fetchall()
    columns = [desc[0] for desc in cursor2.description]
    result = []
    ### Iterates through logs and creates a list of dicts ###
    for row in rows:
            logger.info("Iterating rows of session")
            result.append(dict(zip(columns, row)))
    if result[-1]['event_type'] == "end_session":
        for i in range(1,len(result)-1):
            logger.info(result[i])
            if result[i]["event_type"] == "return_reco":
                interval_timestamp = datetime.timestamp(result[i+1]['evt_time'])-datetime.timestamp(result[i]['evt_time'])
                update_user_history_in_redis(result[i]["user_id"], result[i]["session_id"], result[i]["recommendation"], interval_timestamp)
                received_at = result[i-1]['evt_time']
                sent_at = result[i]['evt_time']
                latency_timestamp =  datetime.timestamp(sent_at)-datetime.timestamp(received_at)
                ### Generate SQL  to upload to reccomendations ###
                sesh_logs_sql =  psycopg2.sql.SQL("INSERT INTO recommendations (session_id, user_id, engagement_duration, sent_at, item_key, latency, recieved_at) VALUES({},{},{},{},COALESCE({}, NULL),{},{})").format(
                    sql.Literal(result[i]["session_id"]),
                    sql.Literal(result[i]["user_id"]),
                    sql.Literal(psycopg2.TimestampFromTicks(interval_timestamp)),
                    sql.Literal(result[i]["evt_time"]),
                    sql.Literal(result[i]["recommendation"]),
                    sql.Literal(psycopg2.TimestampFromTicks(latency_timestamp)),
                    sql.Literal(psycopg2.TimestampFromTicks(datetime.timestamp(received_at)))
                )
                cursor3.execute(sesh_logs_sql)
    count = count+1

conn.commit()   
cursor.close()
cursor2.close()
cursor3.close()


