import os
import configparser
import sys
from confluent_kafka import Consumer
from confluent_kafka.cimpl import KafkaError

# Initialize the configuration parser
config = configparser.ConfigParser()

# Read configuration file
config.read("./config.ini")

print("Read configuration ...")

try:
    kafka_topic = config.get('PROJ_CONF', 'KAFKA_TOPIC')
    kafka_consumer_groupid = config.get('PROJ_CONF', 'KAFKA_CONSUMER_GROUP_ID')
    kafka_broker = config.get('PROJ_CONF', 'KAFKA_BROKER')
    my_channel = config.get('PROJ_CONF', 'MY_CHANNEL')
except Exception as e:
    print(f"Error cannot get required parameters: {e}")
    sys.exit(1)

print("Initialize Kafka Consumer ...")
# Initialize the Kafka consumer
consumer = Consumer({
    'bootstrap.servers': kafka_broker,
    'group.id': kafka_consumer_groupid,
    'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
})

print(f"Kafka topic: {kafka_topic}")
print(f"Kafka consumer: {consumer}")
# Subscribe to the Kafka topic
consumer.subscribe([kafka_topic])

try:
    while True:
        print("Start consume ...")
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error while consuming message: {msg.error()}")
                break
        print(f"Consumed message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    pass

finally:
    consumer.close()

