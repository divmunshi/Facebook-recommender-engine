# from time import sleep
# from json import dumps
# from kafka import KafkaProducer


# print("Producer Running")
# sleep(10)
# producer = KafkaProducer(
#     bootstrap_servers=['kafka:9093'],
#     value_serializer=lambda x: dumps(x).encode('utf-8')
# )
# for j in range(9999):
#     print("Iteration", j)
#     data = {'counter': j}
#     producer.send('topic_test', value=data)
#     sleep(0.5)

from time import sleep
from confluent_kafka import Producer
from json import dumps

sleep(30)
p = Producer({'bootstrap.servers': 'kafka:9092'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

for j in range(9999):
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    print("Iteration", j)
    data = {'counter': j}

    # Asynchronously produce a message. The delivery report callback will
    # be triggered from the call to poll() above, or flush() below, when the
    # message has been successfully delivered or failed permanently.
    p.produce('mytopic', dumps(data).encode('utf-8'), callback=delivery_report)
    sleep(0.5)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()