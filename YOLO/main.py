import os
from ultralytics import YOLO
from dotenv import load_dotenv
from pedestrians_tracker_utils import FramesProcessor

load_dotenv('../.env')


def main():
    model = YOLO('model/yolov8-nano-7300-200e.pt')
    print(os.getenv("YOLO_TOPIC"))
    yolo_frames_processor = FramesProcessor(model=model, producer_topic=os.getenv("YOLO_TOPIC"),
                                            group_id=os.getenv("YOLO_GROUP"))
    yolo_frames_processor.run()


if __name__ == "__main__":
    main()
