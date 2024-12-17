import numpy as np
import cv2 as cv


def decode_frame(encoded_frame):
    np_frame = np.frombuffer(encoded_frame, dtype=np.uint8)
    return cv.imdecode(np_frame, cv.IMREAD_COLOR)


def draw_predictions(frame, boxes, scores, labels, color=(0, 255, 0)):
    for box, score, label in zip(boxes, scores, labels):
        x_min, y_min, x_max, y_max = map(int, box)
        cv.rectangle(frame, (x_min, y_min), (x_max, y_max), color, 2)
        label_text = f"{label}: {score:.2f}"
        cv.putText(frame, label_text, (x_min, y_min - 10), cv.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)