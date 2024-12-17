import os
import cv2 as cv
import sys
from loguru import logger
from dotenv import load_dotenv
from confluent_kafka import KafkaException
from camera_capture import CameraCapture
from pedestrians_tracker_utils import ResultsProducer


def configure_logger():
    """Configures the logger with a specific format."""
    logger.remove(0)
    logger.add(sys.stderr, format="{level} : {time} : {message}")


def load_env_variables():
    """Loads environment variables required for Kafka and camera configuration."""
    load_dotenv()
    # camera_url = os.getenv("CAMERA_URL")
    camera_url = "test-video/test1.mp4"  # Default to a test video
    kafka_server = os.getenv("KAFKA_SERVER")
    topic = os.getenv("TOPIC")
    return camera_url, kafka_server, topic


def process_frame(frame):
    """Processes the frame before sending it to Kafka."""
    return cv.convertScaleAbs(frame, alpha=1, beta=60)


def encode_frame(frame):
    """Encodes the frame to a JPEG format for transmission."""
    success, encoded_frame = cv.imencode('.jpg', frame)
    if success:
        return encoded_frame.tobytes()
    else:
        logger.error("Frame encoding failed")
        return None


def main():
    configure_logger()
    camera_url, kafka_server, topic = load_env_variables()
    producer = ResultsProducer(topic)

    # Initialize camera capture (use the default camera URL if none is provided)
    cap = CameraCapture(url=camera_url)
    try:
        while True:
            frame = cap.get_frame()

            if frame is None:
                logger.error("Empty frame received from camera!")
                cv.waitKey(1000)  # Add a delay to avoid spamming
                continue

            # Process and encode the frame
            processed_frame = process_frame(frame)
            encoded_frame = encode_frame(processed_frame)

            if encoded_frame:
                producer.send_results(encoded_frame)
    except KafkaException as e:
        logger.error(f"Kafka error occurred: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        logger.info("Producer closed and resources released.")


if __name__ == '__main__':
    main()
