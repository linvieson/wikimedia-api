FROM python:3.9-slim

RUN apt-get update

RUN pip install --upgrade pip
RUN pip install kafka-python
RUN pip install requests

COPY kafka-related/to_kafka.py .

CMD ["python3", "to_kafka.py"]
