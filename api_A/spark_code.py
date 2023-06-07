from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StringType, BooleanType
from datetime import datetime, timedelta

from pyspark.sql.streaming import *
from pyspark.sql.functions import *
import json
from confluent_kafka import Consumer, Producer



# spark = SparkSession \
#         .builder \
#         .appName("StatisticsWikimediaA") \
#         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
#         .config("spark.sql.streaming.checkpointLocation", "/opt/app/spark-checkpoint") \
#         .getOrCreate()


kafka_bootstrap_servers = "kafka-server:9092"
input_topic_name = "request-topic"
output_topic_name = "response-topic"
data_topic_name = "wiki-topic"





def get_pages_by_domain():
    # return {'message': 'okk'}
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", data_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

    end_time = datetime.now().replace(minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(hours=6)
    statistics, time_range = [], []

    current_hour = start_time
    
    while current_hour < end_time:
        next_hour = (current_hour + timedelta(hours=1)) if (current_hour + timedelta(hours=1)) <= end_time else end_time
        time_range.append({"time_start": current_hour, "time_end": next_hour})
        statistics.append(df.filter((df.rev_timestamp >= current_hour) & (df.rev_timestamp < next_hour))
                          .groupBy("domain")
                          .count()
                          .collect())

        current_hour = next_hour

        # curr_time += timedelta(hours=1)

        response = []
        for i in range(len(time_range)):
            response.append({"time_start": time_range[i]["time_start"], "time_end": time_range[i]["time_end"],
                            "statistics": [{row["domain"]: row["count"]} for row in statistics[i]]})

        return json.dumps(response), 200






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



# def is_within_hour(message, hour):
#     timestamp = message['timestamp'] // 1000  # Convert milliseconds to seconds
#     message_time = datetime.fromtimestamp(timestamp)
#     return hour <= message_time < (hour + timedelta(hours=1))


# def is_bot_created_page(message):
#     performer = message['performer']
#     return performer.get('user_is_bot', False)


def process_messages(producer, consumer):
    while True:
        consumer.subscribe(topics=[input_topic_name])

        msg = consumer.poll(1.0)

        if msg is None:
            continue
        elif msg.error():
            print('Error: {}'.format(msg.error()))
            continue
    
        else:
            msg_value = json.loads(msg.value().decode('utf-8'))

            if msg_value['function'] == 'get_pages_by_domain':
                response = get_pages_by_domain(msg_value)


            final_response = {'request_id': msg_value['request_id'], 'response': response[0], 'code': response[1]}
            producer.produce(output_topic_name, json.dumps(final_response))
            producer.flush()


if __name__ == '__main__':

    # build spark session

    spark = SparkSession \
            .builder \
            .appName("StatisticsWikimediaA") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
            .config("spark.sql.streaming.checkpointLocation", "/opt/app/spark-checkpoint") \
            .getOrCreate()
    

    
    # Enable hadoop s3a settings
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", \
    "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")


    producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

    configuration = {'bootstrap.servers': kafka_bootstrap_servers,
                    'group.id': "kafka",
                    'auto.offset.reset': 'earliest',
                    'isolation.level':'read_committed'}
    consumer = Consumer(configuration)

    process_messages(producer, consumer)

    # schema = StructType() \
    #         .add("timestamp", StringType()) \
    #         .add("domain", StringType()) \
    #         .add("performer", StructType()
    #             .add("user_is_bot", BooleanType()))

    # df = spark.read.format("org.apache.spark.sql.cassandra").options(table="pages_by_domain", keyspace="wiki").load()



    # df.select(upper(col("value")).alias("value"))\
    #     .writeStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    #     .option("topic", output_topic_name) \
    #     .option("checkpointLocation", "/opt/app/kafka_checkpoint").start().awaitTermination()




    # messages_stream = spark \
    #     .readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    #     .option("subscribe", input_topic_name) \
    #     .option("startingOffsets", "earliest") \
    #     .load()


    # Start the query to consume messages
    # query = messages_stream.writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .start()

    # app.run(host='0.0.0.0')





