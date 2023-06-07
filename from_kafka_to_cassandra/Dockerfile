FROM python:3.9

RUN pip install kafka-python
RUN pip install cassandra-driver

COPY from_kafka_to_cassandra/to_cassandra.py /opt/app/

CMD [ "python3", "/opt/app/to_cassandra.py"]
