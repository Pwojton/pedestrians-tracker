import json
import logging
import numpy as np
import torch
import cv2 as cv
from supervision import Detections, BoxAnnotator, LabelAnnotator, ColorPalette, ByteTrack
from pedestrians_tracker_utils import create_kafka_consumer
from frames_utils import decode_frame
from confluent_kafka import KafkaException
from dotenv import load_dotenv

load_dotenv('../.env')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    torch.device(0)
    frames_buffer = {}
    fusion_consumer = create_kafka_consumer("fusion-results", "tracker")
    frames_consumer = create_kafka_consumer("frames", "tracker")

    tracker = ByteTrack()

    box_annotator = BoxAnnotator(color=ColorPalette.DEFAULT, thickness=2)
    label_annotator = LabelAnnotator(color=ColorPalette.DEFAULT)

    try:
        while True:
            '''
            Synchronization of topics contains frames and assembled results
            '''
            fusion_result_msg = fusion_consumer.poll(0)
            frames_msg = frames_consumer.poll(0.1)
            if frames_msg and not frames_msg.error():
                frame_timestamp = frames_msg.timestamp()[1]
                frame_value = frames_msg.value()

                if frame_timestamp not in frames_buffer:
                    frames_buffer[frame_timestamp] = frame_value

            if fusion_result_msg and not fusion_result_msg.error():
                fusion_result = json.loads(fusion_result_msg.value().decode("utf-8"))
                if fusion_result['key'] in frames_buffer:
                    frame = frames_buffer.pop(fusion_result['key'])
                    frame = decode_frame(frame)
                    detections = Detections(xyxy=np.asarray(fusion_result['boxes']),
                                            confidence=np.asarray(fusion_result['scores']),
                                            class_id=np.asarray(fusion_result["labels"], dtype=np.int32))

                    detections = tracker.update_with_detections(detections)

                    labels = [f"#{tracker_id}" for tracker_id in detections.tracker_id]

                    frame = box_annotator.annotate(detections=detections, scene=frame.copy())
                    frame = label_annotator.annotate(detections=detections, scene=frame.copy(), labels=labels)

                    cv.imshow("Decoded Frame", frame)
                    cv.waitKey(150)
                    if cv.waitKey(1) & 0xFF == ord('q'):
                        break
                else:
                    logger.info("Waiting for match...")

    except KafkaException as e:
        logger.error(f"Kafka error occurred: {e}")
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt")


if __name__ == '__main__':
    main()
