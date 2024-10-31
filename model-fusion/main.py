import os
import json
import logging
from ResultsProducer import ResultsProducer
from confluent_kafka import Consumer, Producer, KafkaException
from dotenv import load_dotenv
from collections import deque, defaultdict

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


results_store = defaultdict(lambda: None)


def create_kafka_consumer(topic, group_id):
    kafka_config = {
        'bootstrap.servers': os.getenv("KAFKA_SERVER"),
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])
    return consumer


def create_kafka_producer():
    return Producer({'bootstrap.servers': os.getenv("KAFKA_SERVER")})


def process_message(message):
    if message is None or message.error():
        if message is not None:
            raise KafkaException(message.error())
        return None

    # Decode the message
    decoded_message = message.value().decode("utf-8")
    return json.loads(decoded_message)


def non_max_suppression(result1, result2):
    if result1['frame_timestamp'] == result2['frame_timestamp']:
        return "Jest w pyte B)"
    return "Gówno :("


def process_queue(queue):
    if not queue:
        return None, None

    result = queue.popleft()
    result_id = result["frame_timestamp"][1]
    matching_result = None

    if result_id in results_store:
        matching_result = results_store.pop(result_id)
    else:
        results_store[result_id] = result

    if result and matching_result:
        return result, matching_result


def integrate_queues(yolo_queue, rtdetr_queue):
    process_queue(yolo_queue)  # faster queue
    rtdetr_result, yolo_result = process_queue(rtdetr_queue)  # slower queue

    if yolo_result and rtdetr_result:
        return non_max_suppression(rtdetr_result, yolo_result)


def poll_consumer(consumer, queue):
    messages = consumer.consume(num_messages=10, timeout=0)  # Batch processing
    for message in messages:
        try:
            result = process_message(message)
            if result:
                queue.append(result)
        except KafkaException as e:
            logger.error(f"Kafka error: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding message: {e}")


def main():
    yolo_results_consumer = create_kafka_consumer(topic=os.getenv("YOLO_TOPIC"),
                                                  group_id=os.getenv("YOLO_GROUP"))
    rtdetr_results_consumer = create_kafka_consumer(topic=os.getenv("RT-DETR_TOPIC"),
                                                    group_id=os.getenv("RT-DETR_GROUP"))
    results_producer = ResultsProducer(os.getenv("RESULTS_PRODUCER_TOPIC"))

    yolo_queue = deque()
    rtdetr_queue = deque()

    while True:
        poll_consumer(yolo_results_consumer, yolo_queue)
        poll_consumer(rtdetr_results_consumer, rtdetr_queue)

        fusion_result = integrate_queues(yolo_queue, rtdetr_queue)

        if fusion_result:
            fusion_result = json.dumps(fusion_result).encode("utf-8")
            results_producer.send_results(fusion_result)


if __name__ == '__main__':
    main()
