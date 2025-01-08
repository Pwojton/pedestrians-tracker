import numpy as np
import cv2 as cv


def decode_frame(encoded_frame):
    np_frame = np.frombuffer(encoded_frame, dtype=np.uint8)
    return cv.imdecode(np_frame, cv.IMREAD_COLOR)
