from setuptools import setup, find_packages

setup(
    name="kf_frames_processor",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "opencv-python",
        "confluent-kafka",
        "numpy",
        "loguru",
        "loguru",
        "python-dotenv"
    ],


)
