from FramesConsumer import FramesConsumer
from ultralytics import RTDETR


def main():
    model = RTDETR("models/rtdetr-l.pt")
    yolo_frames_consumer = FramesConsumer(model)
    yolo_frames_consumer.run()


if __name__ == '__main__':
    main()
