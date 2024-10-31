import json
import os
import cv2 as cv
import numpy as np
from ResultsProducer import ResultsProducer
from confluent_kafka import Consumer, KafkaException
from loguru import logger
from dotenv import load_dotenv


class FramesProcessor:
    def __init__(self, model, producer_topic, group_id):
        load_dotenv()
        self.kafka_config = {
            'bootstrap.servers': os.getenv("KAFKA_SERVER"),
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        }
        self.topic = 'frames'
        self.model = model
        self.consumer = self.create_kafka_consumer()
        self.producer = ResultsProducer(producer_topic)

    def create_kafka_consumer(self):
        """Creates and returns a Kafka Consumer subscribed to a given topic."""
        consumer = Consumer(self.kafka_config)
        consumer.subscribe([self.topic])
        return consumer

    @staticmethod
    def decode_frame(encoded_frame):
        """Decodes the received frame from byte buffer to an image array."""
        np_frame = np.frombuffer(encoded_frame, dtype=np.uint8)
        return cv.imdecode(np_frame, cv.IMREAD_COLOR)

    def process_frame(self, frame, frame_timestamp):
        """Processes the frame using the YOLO model and returns the annotated frame."""
        pred_results = self.model(frame, conf=0.3)[0].boxes.cpu()  # predict by model

        pred_scores = pred_results.conf.unsqueeze(dim=1).numpy()
        pred_boxes = pred_results.xyxy.numpy()
        pred_classes = pred_results.cls.int().unsqueeze(dim=1).numpy()

        result = {
            "frame_timestamp": frame_timestamp,
            "score": pred_scores.tolist(),
            "boxes": pred_boxes.tolist(),
            "classes": pred_classes.tolist()
        }

        return result

    def run(self):
        """Main loop to consume and process frames from Kafka."""
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
                    annotated_frame = self.process_frame(frame, msg.timestamp())

                    # serialize to send to kafka
                    serialized_frame = json.dumps(annotated_frame).encode("utf-8")
                    self.producer.send_results(serialized_frame)

                    # cv.imshow('Received Frame', annotated_frame)

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