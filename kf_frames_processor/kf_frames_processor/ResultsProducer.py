import os
from confluent_kafka import Producer
from loguru import logger
from dotenv import load_dotenv


class ResultsProducer:
    def __init__(self, topic):
        load_dotenv()
        self.topic = topic
        # self.producer = Producer({'bootstrap.servers': os.getenv("KAFKA_SERVER")})
        self.producer = Producer({'bootstrap.servers': 'localhost:9094'})

    def delivery_report(self, err, msg):
        """Reports the status of frame delivery to Kafka."""
        if err:
            logger.error("Results from topic {} delivery failed: {}", self.topic, err)
        else:
            logger.success(
                f'Results successfully produced to topic "{msg.topic()}" [partition {msg.partition()}] at offset {msg.offset()}')

    def send_results(self, results):
        self.producer.produce(topic=self.topic, value=results, on_delivery=self.delivery_report)
        self.producer.poll(0.1)
