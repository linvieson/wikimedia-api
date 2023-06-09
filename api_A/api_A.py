import json

from flask import Flask
from confluent_kafka import TopicPartition, DeserializingConsumer, Producer
from pyspark.sql.streaming import *
from pyspark.sql.functions import *


app = Flask(__name__)


kafka_bootstrap_servers = "kafka-server:9092"
input_topic_name = "request-topic"
output_topic_name = "response-topic"


global REQID
REQID = 0

@app.route('/statistics/pages_by_domain', methods=['GET'])
def get_pages_by_domain():
    producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

    global REQID
    message = {"function": 'get_pages_by_domain', "request_id": REQID}
    REQID += 1

    producer.produce(input_topic_name, value=json.dumps(message))
    producer.flush()

    offset = 0
    while True:
        # configure    
        configuration = {'bootstrap.servers': kafka_bootstrap_servers,
                        'group.id': "kafka",
                        'auto.offset.reset': 'earliest',
                        'isolation.level':'read_committed'}
        consumer = DeserializingConsumer(configuration)
        
        partition = 0
        partition = TopicPartition(output_topic_name, partition, offset)
        consumer.assign([partition])
        consumer.seek(partition)

    # read the message
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue
            if message.error():
                print('Error: {}'.format(message.error()))
                continue
            else:
                data = json.loads(message.value().decode('utf-8'))

                if data['request_id'] == REQID - 1:
                    return data['response'], data['code']



@app.route('/statistics/pages_by_bots', methods=['GET'])
def get_pages_by_bots():
    producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

    global REQID
    message = {"function": 'get_pages_by_bots', "request_id": REQID}
    REQID += 1

    producer.produce(input_topic_name, value=json.dumps(message))
    producer.flush()

    offset = 0
    while True:
        # configure    
        configuration = {'bootstrap.servers': kafka_bootstrap_servers,
                        'group.id': "kafka",
                        'auto.offset.reset': 'earliest',
                        'isolation.level':'read_committed'}
        consumer = DeserializingConsumer(configuration)
        
        partition = 0
        partition = TopicPartition(output_topic_name, partition, offset)
        consumer.assign([partition])
        consumer.seek(partition)

    # read the message
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue
            if message.error():
                print('Error: {}'.format(message.error()))
                continue
            else:
                data = json.loads(message.value().decode('utf-8'))

                if data['request_id'] == REQID - 1:
                    return data['response'], data['code']


@app.route('/statistics/top_20_users', methods=['GET'])
def get_top_20_users():
    producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

    global REQID
    message = {"function": 'get_top_20_users', "request_id": REQID}
    REQID += 1

    producer.produce(input_topic_name, value=json.dumps(message))
    producer.flush()

    offset = 0
    while True:
        # configure    
        configuration = {'bootstrap.servers': kafka_bootstrap_servers,
                        'group.id': "kafka",
                        'auto.offset.reset': 'earliest',
                        'isolation.level':'read_committed'}
        consumer = DeserializingConsumer(configuration)
        
        partition = 0
        partition = TopicPartition(output_topic_name, partition, offset)
        consumer.assign([partition])
        consumer.seek(partition)

    # read the message
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue
            if message.error():
                print('Error: {}'.format(message.error()))
                continue
            else:
                data = json.loads(message.value().decode('utf-8'))

                if data['request_id'] == REQID - 1:
                    return data['response'], data['code']


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)

