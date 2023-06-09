FROM python:3.9-slim

RUN pip install flask
RUN pip install cassandra-driver

COPY api_B/api_B.py /opt/app/
CMD ["python3", "/opt/app/api_B.py"]
