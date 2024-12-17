import json
import logging
import torch
import cv2 as cv
from pedestrians_tracker_utils import create_kafka_consumer
from frames_utils import decode_frame, draw_predictions
from confluent_kafka import KafkaException
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    torch.device(0)
    frames_buffer = {}
    fusion_consumer = create_kafka_consumer("fusion-results", "tracker")
    frames_consumer = create_kafka_consumer("frames", "tracker")

    try:
        while True:
            fusion_result_msg = fusion_consumer.poll(0)
            frames_msg = frames_consumer.poll(0.1)
            print(frames_msg)
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
                    draw_predictions(frame=frame, boxes=fusion_result['boxes'], scores=fusion_result['scores'],
                                     labels=fusion_result['labels'])
                    cv.imshow("Decoded Frame", frame)

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
