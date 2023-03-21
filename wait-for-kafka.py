import sys
import time
from kafka import KafkaProducer

kafka_bootstrap_servers = sys.argv[1]
timeout = int(sys.argv[2])

start_time = time.time()

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
        break
    except Exception as e:
        if time.time() > start_time + timeout:
            print(f"Kafka is still unavailable after {timeout} seconds - exiting")
            sys.exit(1)

        print(f"Kafka is unavailable - sleeping: {e}")
        time.sleep(1)
