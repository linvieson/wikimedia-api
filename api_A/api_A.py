from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StringType, BooleanType
from datetime import datetime, timedelta
from flask import Flask, jsonify
from confluent_kafka import TopicPartition, DeserializingConsumer, Producer

import json
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






# @app.route('/statistics/pages_by_bots', methods=['GET'])
# def get_pages_by_bots():
#     end_time = datetime.now().replace(minute=0, second=0, microsecond=0)
#     start_time = end_time - timedelta(hours=6)
#     statistics = []

#     while start_time < end_time:
#         time_start_str = start_time.strftime('%H:%M')
#         time_end_str = (start_time + timedelta(hours=1)).strftime('%H:%M')

#         hour_statistics = messages_stream.filter(lambda msg: is_within_hour(msg, start_time)).filter(lambda msg: is_bot_created_page(msg)).countByValue().collect()

#         formatted_statistics = [{domain: count} for domain, count in hour_statistics]
#         statistics.append({
#             'time_start': time_start_str,
#             'time_end': time_end_str,
#             'statistics': formatted_statistics
#         })

#         start_time += timedelta(hours=1)

#     return jsonify(statistics)




if __name__ == '__main__':

    app.run(host='0.0.0.0')





