FROM python:3.9-slim

RUN apt-get update

# RUN pip install --upgrade pip
RUN pip install pyspark
RUN pip install flask

COPY api_A/api_A.py .

CMD ["python3", "api_A.py"]
