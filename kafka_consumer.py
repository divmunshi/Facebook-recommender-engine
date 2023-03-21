# from kafka import KafkaConsumer
# from json import loads
# from time import sleep
# consumer = KafkaConsumer(
#     'topic_test',
#     bootstrap_servers=['kafka:9092'],
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     group_id='my-group-id',
#     value_deserializer=lambda x: loads(x.decode('utf-8'))
# )
# for event in consumer:
#     event_data = event.value
#     # Do whatever you want
#     print(event_data)
#     sleep(0.5)



from confluent_kafka import Consumer
from time import sleep
sleep(30)

c = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['mytopic'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()