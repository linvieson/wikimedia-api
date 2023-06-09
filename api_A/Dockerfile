FROM python:3.9-slim

RUN pip install pyspark
RUN pip install flask
RUN pip install confluent_kafka

COPY api_A/api_A.py /opt/app/
CMD ["python3", "/opt/app/api_A.py"]
