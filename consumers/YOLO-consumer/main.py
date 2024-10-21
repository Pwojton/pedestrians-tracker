from FramesConsumer import FramesConsumer
from ultralytics import YOLO
import torch


def main():
    torch.device(0)
    model = YOLO("models/yolov8-nano-7300-200e.pt")
    yolo_frames_consumer = FramesConsumer(model)
    yolo_frames_consumer.run()


if __name__ == '__main__':
    main()
