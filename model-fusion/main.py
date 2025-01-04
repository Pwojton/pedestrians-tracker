import os
import json
import logging
import torch
from data_utils import flatten_data, normalize_boxes, revert_normalization
from confluent_kafka import KafkaException
from dotenv import load_dotenv
from ensemble_boxes import weighted_boxes_fusion
from pedestrians_tracker_utils import ResultsProducer, create_kafka_consumer

load_dotenv('../.env')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def model_fusion(result):
    boxes_list = [result["YOLO_BOXES"], result["RTDETR_BOXES"]]
    scores_list = [flatten_data(result["YOLO_SCORES"]), flatten_data(result["RTDETR_SCORES"])]
    labels_list = [flatten_data(result["YOLO_CLASSES"]), flatten_data(result["RTDETR_CLASSES"])]

    boxes, scores, labels = weighted_boxes_fusion(normalize_boxes(boxes_list), scores_list, labels_list, iou_thr=0.5,
                                                  skip_box_thr=0.5)

    return boxes, scores, labels


def main():
    torch.device(0)
    combined_results_consumer = create_kafka_consumer(os.getenv("COMBINED_RESULTS_TOPIC"),
                                                      os.getenv("COMBINED_RESULTS_GROUP"))
    producer = ResultsProducer(os.getenv("FUSION_RESULTS_TOPIC"))

    try:
        while True:
            combined_results_msg = combined_results_consumer.poll(0)

            if combined_results_msg and not combined_results_msg.error():
                combined_key = int(combined_results_msg.key().decode("utf-8"))
                combined_value = json.loads(combined_results_msg.value().decode("utf-8"))

                boxes, scores, labels = model_fusion(combined_value)

                fusion_result = {
                    "key": combined_key,
                    "boxes": revert_normalization(boxes).tolist(),
                    "scores": scores.tolist(),
                    "labels": labels.tolist()
                }
                serialized_result = json.dumps(fusion_result).encode("utf-8")

                producer.send_results(serialized_result)

    except KafkaException as e:
        logger.error(f"Kafka error occurred: {e}")
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt")


if __name__ == '__main__':
    main()
