import sys
import cv2 as cv
import numpy as np
from confluent_kafka import Consumer, KafkaException
from loguru import logger
from ultralytics import YOLO

# Kafka Consumer configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

TOPIC = 'frames'


def configure_logger():
    """Configures the logger with a specified format."""
    logger.remove(0)
    logger.add(sys.stderr, format="{level} : {time} : {message}")


def create_kafka_consumer():
    """Creates and returns a Kafka Consumer subscribed to a given topic."""
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])
    return consumer


def decode_frame(encoded_frame):
    """Decodes the received frame from byte buffer to an image array."""
    np_frame = np.frombuffer(encoded_frame, dtype=np.uint8)
    return cv.imdecode(np_frame, cv.IMREAD_COLOR)


def process_frame(model, frame):
    """Processes the frame using YOLO model and returns the annotated frame."""
    results = model.track(frame, persist=True, conf=0.3)
    return results[0].plot()


def main():
    configure_logger()
    consumer = create_kafka_consumer()
    model = YOLO("model/yolov8-nano-7300-150e.pt")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:  # No message received
                continue

            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            logger.info("Frame received", msg.timestamp())
            frame = decode_frame(msg.value())

            if frame is not None:
                annotated_frame = process_frame(model, frame)
                cv.imshow('Received Frame', annotated_frame)

                if cv.waitKey(1) & 0xFF == ord('q'):
                    break
            else:
                logger.error("Failed to decode the frame.")
    except KafkaException as e:
        logger.error(f"Kafka error occurred: {e}")
    finally:
        consumer.close()
        cv.destroyAllWindows()
        logger.info("Consumer closed and resources released.")


if __name__ == '__main__':
    main()
