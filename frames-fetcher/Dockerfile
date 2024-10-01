FROM python:latest
WORKDIR /app
RUN apt-get update && apt-get install -y \
    libgl1-mesa-glx \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
COPY main.py .
COPY camera_capture.py .
COPY .env .
RUN pip install -r requirements.txt
EXPOSE 9092
ENTRYPOINT ["python", "main.py"]
