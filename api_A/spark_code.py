import json

from confluent_kafka import Consumer, Producer
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.streaming import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

kafka_bootstrap_servers = "kafka-server:9092"
input_topic_name = "request-topic"
output_topic_name = "response-topic"
data_topic_name = "wiki-topic"


def get_pages_by_domain():

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

        response = []
        for i in range(len(time_range)):
            response.append({"time_start": time_range[i]["time_start"], "time_end": time_range[i]["time_end"],
                            "statistics": [{row["domain"]: row["count"]} for row in statistics[i]]})

        return json.dumps(response), 200


def get_pages_by_bots():
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
                          .filter(lambda msg: df.created_by_bot is True)
                          .groupBy('domain')
                          .count()
                          .collect())

        current_hour = next_hour

        response = []
        for i in range(len(time_range)):
            response.append({"time_start": time_range[i]["time_start"], "time_end": time_range[i]["time_end"],
                            "statistics": [{"domain": row["domain"], "created_by_bots": row["count"]} for row in statistics[i]]})

        return json.dumps(response), 200


def get_top_20_users():
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", data_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

    end_time = datetime.now().replace(minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(hours=6)
    
    new_df = df.filter((df.rev_timestamp >= start_time) & (df.rev_timestamp < end_time)).groupBy("user_id").count()

    windowSpec = Window.orderBy(desc("count"))
    top_20_users = new_df.withColumn("rank", dense_rank().over(windowSpec)).filter(col("rank") <= 20)
    result = top_20_users.join(df, "user_id").select("user_name", "user_id", "start_time", "end_time", "page_title").collect()

    response = []
    for row in result:
        response.append({
            "user_name": row.user_name,
            "user_id": row.user_id,
            "time_start": row.start_time,
            "time_end": row.end_time,
            "page_title": row.page_title
        })

    return json.dumps(response), 200



def process_messages(producer, consumer):
    consumer.subscribe(topics=[input_topic_name])
    print('cpnsumers')

    for message in consumer:
        msg_value = message.value

        if msg_value['function'] == 'get_pages_by_domain':
            response = get_pages_by_domain()
        elif msg_value['function'] == 'get_pages_by_bots':
            response = get_pages_by_bots()
        elif msg_value['function'] == 'get_top_20_users':
            response = get_top_20_users()

        final_response = {'request_id': msg_value['request_id'], 'response': response[0], 'code': response[1]}
        producer.produce(output_topic_name, json.dumps(final_response))
        producer.flush()


if __name__ == '__main__':
    spark = SparkSession \
            .builder \
            .appName("StatisticsWikimediaA") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
            .config("spark.sql.streaming.checkpointLocation", "/opt/app/spark-checkpoint") \
            .getOrCreate()

    producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

    configuration = {'bootstrap.servers': kafka_bootstrap_servers,
                    'group.id': "kafka",
                    'auto.offset.reset': 'earliest',
                    'isolation.level':'read_committed'}
    consumer = Consumer(configuration)

    process_messages(producer, consumer)

