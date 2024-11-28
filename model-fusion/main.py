import os
import json
import logging
import numpy as np
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv
from ensemble_boxes import weighted_boxes_fusion

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_kafka_consumer(topic, group_id):
    kafka_config = {
        'bootstrap.servers': os.getenv("KAFKA_SERVER"),
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])
    return consumer


def flatten_data(data):
    return [np.squeeze(sublist).tolist() for sublist in data]


def normalize_boxes(boxes_list):
    frame_width = 1920
    frame_height = 1080
    normalized_boxes = [
        [
            [
                x_min / frame_width,
                y_min / frame_height,
                x_max / frame_width,
                y_max / frame_height
            ]
            for (x_min, y_min, x_max, y_max) in box_group
        ]
        for box_group in boxes_list
    ]

    return normalized_boxes


def revert_normalization(normalized_boxes):
    frame_width = 1920
    frame_height = 1080
    original_boxes = [
        [
            x_min * frame_width,
            y_min * frame_height,
            x_max * frame_width,
            y_max * frame_height
        ]
        for (x_min, y_min, x_max, y_max) in normalized_boxes
    ]
    return np.array(original_boxes, dtype=float)


def model_fusion(result):
    frame_width = 1920
    frame_height = 1080
    boxes_list = [result["YOLO_BOXES"], result["RTDETR_BOXES"]]
    scores_list = [flatten_data(result["YOLO_SCORES"]), flatten_data(result["RTDETR_SCORES"])]
    labels_list = [flatten_data(result["YOLO_CLASSES"]), flatten_data(result["RTDETR_CLASSES"])]

    boxes, scores, labels = weighted_boxes_fusion(normalize_boxes(boxes_list), scores_list, labels_list, iou_thr=0.5)
    return boxes, scores, labels


def main():
    combined_results_consumer = create_kafka_consumer(os.getenv("COMBINED_RESULTS_TOPIC"),
                                                      os.getenv("COMBINED_RESULTS_GROUP"))
    try:
        while True:
            msg = combined_results_consumer.poll(0)

            if msg is None:
                continue

            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            result = json.loads(msg.value().decode("utf-8"))
            result_key = json.loads(msg.key().decode("utf-8"))

            boxes, scores, labels = model_fusion(result)

            print(result_key, boxes, scores, labels)

    except KafkaException as e:
        logger.error(f"Kafka error occurred: {e}")


if __name__ == '__main__':
    main()
