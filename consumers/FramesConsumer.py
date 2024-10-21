import sys
import cv2 as cv
import numpy as np
import torch
from confluent_kafka import Consumer, KafkaException
from loguru import logger


def configure_logger():
    """Configures the logger with a specified format."""
    logger.remove(0)
    logger.add(sys.stderr, format="{level} : {time} : {message}")


class FramesConsumer:
    def __init__(self, model):
        self.kafka_config = {
            'bootstrap.servers': 'localhost:9094',
            'group.id': 'my-group',
            'auto.offset.reset': 'earliest'
        }
        self.topic = 'frames'
        self.model = model
        self.consumer = self.create_kafka_consumer()
        configure_logger()

    def create_kafka_consumer(self):
        """Creates and returns a Kafka Consumer subscribed to a given topic."""
        consumer = Consumer(self.kafka_config)
        consumer.subscribe([self.topic])
        return consumer

    def decode_frame(self, encoded_frame):
        """Decodes the received frame from byte buffer to an image array."""
        np_frame = np.frombuffer(encoded_frame, dtype=np.uint8)
        return cv.imdecode(np_frame, cv.IMREAD_COLOR)

    def process_frame(self, frame):
        """Processes the frame using the YOLO model and returns the annotated frame."""
        results = self.model.track(frame, persist=True, conf=0.3)
        return results[0].plot()

    def run(self):
        """Main loop to consume and process frames from Kafka."""
        print(torch.cuda.is_available())  # Check if CUDA is available for GPU usage

        try:
            while True:
                msg = self.consumer.poll(0)

                if msg is None:  # No message received
                    continue

                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                logger.info("Frame received", msg.timestamp())
                frame = self.decode_frame(msg.value())

                if frame is not None:
                    annotated_frame = self.process_frame(frame)
                    cv.imshow('Received Frame', annotated_frame)

                    if cv.waitKey(1) & 0xFF == ord('q'):
                        break
                else:
                    logger.error("Failed to decode the frame.")
        except KafkaException as e:
            logger.error(f"Kafka error occurred: {e}")
        finally:
            self.consumer.close()
            cv.destroyAllWindows()
            logger.info("Consumer closed and resources released.")