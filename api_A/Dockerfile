FROM python:3.9-slim
# FROM gcr.io/datamechanics/spark:platform-3.1-dm14

# ENV PYSPARK_MAJOR_PYTHON_VERSION=3
# WORKDIR /opt/app/

# RUN wget  https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
# RUN mv postgresql-42.2.5.jar /opt/spark/jars

RUN pip install pyspark
RUN pip install flask
RUN pip install confluent_kafka


COPY api_A/api_A.py /opt/app/
CMD ["python3", "/opt/app/api_A.py"]
