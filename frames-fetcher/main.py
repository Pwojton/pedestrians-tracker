import os
import cv2 as cv
import sys
from loguru import logger
from dotenv import load_dotenv
from confluent_kafka import Producer
from camera_capture import CameraCapture

logger.remove(0)
logger.add(sys.stderr, format="{level} : {time} : {message}")


def delivery_report(err, msg):
    if err is not None:
        logger.error("Frame delivery failed")
        return
    logger.success('Frame successfully produced to topic "{}" [message partition {}] at offset {}'.format(
         msg.topic(), msg.partition(), msg.offset()))


def main():
    load_dotenv()
    camera_url = os.getenv("CAMERA_URL")
    kafka_server = os.getenv("KAFKA_SERVER")
    topic = os.getenv("TOPIC")

    conf = {
        'bootstrap.servers': kafka_server,
    }
    producer = Producer(conf)
    cap = CameraCapture(url=camera_url)

    while True:
        frame = cap.get_frame()

        if frame is None:
            logger.error("Empty frame!")
            cv.waitKey(1000)
            continue

        frame = cv.convertScaleAbs(frame, alpha=1, beta=60)
        success_encoded, encoded_frame = cv.imencode('.jpg', frame)
        encoded_frame = encoded_frame.tobytes()

        if success_encoded:
            producer.produce(topic=topic, value=encoded_frame, on_delivery=delivery_report)
            producer.poll(0.1)
        else:
            logger.error("Frame encoding failed")

    return


if __name__ == '__main__':
    main()
