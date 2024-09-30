import sys
import cv2 as cv
import numpy as np
from confluent_kafka import Consumer
from loguru import logger

conf = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['frames'])

logger.remove(0)
logger.add(sys.stderr, format="{level} : {time} : {message}")


def decode_frame(decoded_frame):
    encoded_frame = np.frombuffer(decoded_frame, dtype=np.uint8)
    return cv.imdecode(encoded_frame, cv.IMREAD_COLOR)


def main():
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue

        logger.info("Frame received", msg.timestamp())

        frame = decode_frame(msg.value())

        if frame is not None:
            cv.imshow('Received Frame', frame)

            if cv.waitKey(1) & 0xFF == ord('q'):
                break
        else:
            logger.error("Failed to decode the frame.")

    consumer.close()
    cv.destroyAllWindows()


if __name__ == '__main__':
    main()
