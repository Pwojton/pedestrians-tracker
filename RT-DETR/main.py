import os
from ultralytics import RTDETR
from dotenv import load_dotenv
from pedestrians_tracker_utils import FramesProcessor

load_dotenv('../.env')


def main():
    model = RTDETR('model/rtdetr-l-7300.pt')
    yolo_frames_processor = FramesProcessor(model=model, producer_topic=os.getenv("RT-DETR_TOPIC"),
                                            group_id=os.getenv("RT-DETR_GROUP"))
    yolo_frames_processor.run()


if __name__ == "__main__":
    main()
