import os
import threading
from FramesProcessor import FramesProcessor
from ultralytics import YOLO, RTDETR
from dotenv import load_dotenv


load_dotenv()


def rtdetr_consumer():
    model = RTDETR("models/rtdetr-l.pt")
    rtdetr_frames_consumer = FramesProcessor(model=model, producer_topic=os.getenv("RT-DETR_TOPIC"),
                                             group_id=os.getenv("RT-DETR_GROUP"))
    rtdetr_frames_consumer.run()


def yolo_consumer():
    model = YOLO("models/yolov8-nano-7300-200e.pt")
    yolo_frames_consumer = FramesProcessor(model=model, producer_topic=os.getenv("YOLO_TOPIC"),
                                           group_id=os.getenv("YOLO_GROUP"))
    yolo_frames_consumer.run()


def main():
    rtdetr_thread = threading.Thread(target=rtdetr_consumer)
    yolo_thread = threading.Thread(target=yolo_consumer)

    rtdetr_thread.start()
    yolo_thread.start()

    rtdetr_thread.join()
    yolo_thread.join()


if __name__ == "__main__":
    main()
