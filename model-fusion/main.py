import os
import json
import logging
import cv2 as cv
import torch
from data_utils import flatten_data, normalize_boxes, revert_normalization
from frames_utils import decode_frame, draw_predictions
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


def model_fusion(result):
    boxes_list = [result["YOLO_BOXES"], result["RTDETR_BOXES"]]
    scores_list = [flatten_data(result["YOLO_SCORES"]), flatten_data(result["RTDETR_SCORES"])]
    labels_list = [flatten_data(result["YOLO_CLASSES"]), flatten_data(result["RTDETR_CLASSES"])]

    boxes, scores, labels = weighted_boxes_fusion(normalize_boxes(boxes_list), scores_list, labels_list, iou_thr=0.5,
                                                  skip_box_thr=0.5)

    return boxes, scores, labels


def process_results(frame, combined_value):
    frame = decode_frame(frame)
    boxes, scores, labels = model_fusion(combined_value)

    draw_predictions(frame=frame, boxes=revert_normalization(boxes), scores=scores, labels=labels)

    cv.imshow("Decoded Frame", frame)


def main():
    torch.device(0)
    combined_results_consumer = create_kafka_consumer(os.getenv("COMBINED_RESULTS_TOPIC"),
                                                      os.getenv("COMBINED_RESULTS_GROUP"))
    frames_consumer = create_kafka_consumer(topic=os.getenv("FRAMES_TOPIC"), group_id=os.getenv("FRAMES_GROUP"))

    frames_buffer = {}

    try:
        while True:
            frames_msg = frames_consumer.poll(0.1)
            combined_results_msg = combined_results_consumer.poll(0)

            if frames_msg and not frames_msg.error():
                frame_timestamp = frames_msg.timestamp()[1]
                frame_value = frames_msg.value()

                if frame_timestamp not in frames_buffer:
                    frames_buffer[frame_timestamp] = frame_value

            if combined_results_msg and not combined_results_msg.error():
                combined_key = int(combined_results_msg.key().decode("utf-8"))
                combined_value = json.loads(combined_results_msg.value().decode("utf-8"))

                if combined_key in frames_buffer:
                    frame = frames_buffer.pop(combined_key)
                    process_results(frame=frame, combined_value=combined_value)

                    if cv.waitKey(1) & 0xFF == ord('q'):  # Exit on 'q' key press
                        break
                else:
                    logger.info("Waiting for match...")

    except KafkaException as e:
        logger.error(f"Kafka error occurred: {e}")
    except KeyboardInterrupt:
        logger.info("Synchronization process interrupted")


if __name__ == '__main__':
    main()
