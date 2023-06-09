import re
import requests
import json
from kafka import KafkaProducer

url = "https://stream.wikimedia.org/v2/stream/page-create"


def extract_str(string, field_to_extract):
    match = re.search(rf'"{field_to_extract}":"([^"]+)"', string)
    if match:
        return match.group(1)


def extract_data(string, field_to_extract):
    match = re.search(rf'"{field_to_extract}":([^"]+),', string)
    if match:
        return match.group(1)


if __name__ == '__main__':
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='kafka-server:9092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    while True:
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            iterator = response.iter_lines()
        try:
            for line in iterator:
                data = line.decode('utf-8')

                if 'data:' in data:
                    data_dict = {}

                    for field in ['domain', 'rev_timestamp', 'page_title']:
                        data_dict[field] = extract_str(data, field)

                    for field in ['page_id', 'user_id']:
                        data_dict[field] = extract_data(data, field)

                    data_dict['user_is_bot'] = bool(extract_data(data, 'user_is_bot'))

                    producer.send('wiki-topic', value=data_dict)

                    print(f"Row has been sent {data_dict}\n")
        except:
            continue

    # Flush the producer to make sure all messages are sent
    producer.flush()

    # Close the producer
    producer.close()
