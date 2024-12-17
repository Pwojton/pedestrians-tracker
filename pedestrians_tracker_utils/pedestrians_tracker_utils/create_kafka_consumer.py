import os
from confluent_kafka import Consumer


def create_kafka_consumer(topic, group_id):
    kafka_config = {
        'bootstrap.servers': os.getenv("KAFKA_SERVER"),
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])
    return consumer
