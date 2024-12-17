# Pedestrians tracker

## About project
This is my Master's thesis project where I created human 
tracking system using multi-model fusion and distributed 
processing.

I ensembled results from YOLO and RT-DETR models utilizing 
Weighted Box Fusion 


## Installation 
### frames-fetcher
1. ``cd frames-fetcher && pip install requirements.txt``
2. Create ``.env`` file like this
```
CAMERA_URL=...
KAFKA_SERVER=...
TOPIC=...
```

### YOLO-consumer
1. ``cd YOLO-consumer && pip install requirements.txt``