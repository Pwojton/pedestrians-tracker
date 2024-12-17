from setuptools import setup, find_packages

setup(
    name="pedestrians_tracker_utils",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "confluent-kafka",
        "python-dotenv",
        "loguru",
    ],
)
