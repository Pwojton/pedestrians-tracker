from threading import Thread
import cv2 as cv
import time


class CameraCapture:
    def __init__(self, url):
        self.capture = cv.VideoCapture(url)
        self.frame = None
        self.thread = Thread(target=self.update, args=())
        self.thread.daemon = True
        self.thread.start()

    def update(self):
        # Read the next frame from the stream in a different thread
        while True:
            if self.capture.isOpened():
                (_, self.frame) = self.capture.read()
            # time.sleep(.035)

    def get_frame(self):
        return self.frame
