FROM python:3.9

RUN pip install kafka-python
RUN pip install cassandra-driver

COPY from_kafka_to_cassandra/to_cassandra.py .
# COPY from_kafka_to_cassandra/ddl.cql .

CMD [ "python3", "to_cassandra.py"]
