
## app route / 
cursor.execute("INSERT INTO recommendations (user_id, session_id, recieved_at) VALUES (%s, %s,%s)",  (user_id, session_id, time_stamp))


## /evt
cursor.execute("INSERT INTO events (user_id, session_id, created_at, event_type) VALUES (%s, %s,%s, %s)",  (cur_usr, session, time_stamp, ev_type))

## /item
cursor.execute("INSERT INTO items (user_id, item_id, bucket_key, created_at, item_key, content_type) VALUES (%s, %s,%s, %s, %s, %s)",  (cur_usr, itm_id, bckt_key, time_stamp, itm_key,cnt_type))

#item2db
cursor.execute("INSERT INTO items (user_id, bucket_key, created_at, item_key, content_type) VALUES (%s, %s, %s, %s, %s)",  (item['user_id'], item['bucket_key'], time_stamp, item['item_key'],item['type']))
