FROM python:3.9-slim

RUN pip install kafka-python
RUN pip install requests

COPY kafka-related/to_kafka.py /opt/app/

CMD ["python3", "/opt/app/to_kafka.py"]
