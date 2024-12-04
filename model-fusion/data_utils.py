import numpy as np

FRAME_WIDTH = 1920
FRAME_HEIGHT = 1080


def flatten_data(data):
    return [np.squeeze(sublist).tolist() for sublist in data]


def normalize_boxes(boxes_list):
    normalized_boxes = [
        [
            [
                x_min / FRAME_WIDTH,
                y_min / FRAME_HEIGHT,
                x_max / FRAME_WIDTH,
                y_max / FRAME_HEIGHT
            ]
            for (x_min, y_min, x_max, y_max) in box_group
        ]
        for box_group in boxes_list
    ]

    return normalized_boxes


def revert_normalization(normalized_boxes):
    original_boxes = [
        [
            x_min * FRAME_WIDTH,
            y_min * FRAME_HEIGHT,
            x_max * FRAME_WIDTH,
            y_max * FRAME_HEIGHT
        ]
        for (x_min, y_min, x_max, y_max) in normalized_boxes
    ]
    return np.array(original_boxes, dtype=float)
