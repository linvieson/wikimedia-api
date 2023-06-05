from json import loads
from kafka import KafkaConsumer


class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        from cassandra.cluster import Cluster
        from cassandra.query import dict_factory
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)
        self.session.row_factory = dict_factory

    def close(self):
        self.session.shutdown()

    def execute(self, query):
        return self.session.execute(query)

    def insert_into_table(self, table, values):
        query = f"INSERT INTO {table} (uid, domain, rev_timestamp, user_is_bot, user_id) " "VALUES (?, ?, ?, ?, ?)"

        # self.execute(query)
        self.session.execute(self.session.prepare(query), values)



# Initialize Kafka consumer
consumer = KafkaConsumer(
    'wiki-topic',
    bootstrap_servers=['kafka-server:9092'],
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

if __name__ == '__main__':

    # Initialize Cassandra client
    client = CassandraClient(host='cassandra-node', port=9042, keyspace='wiki')
    client.connect()


    # Process messages from Kafka consumer
    for message in consumer:
        value = message.value
        # Extract data from the message
        domain = value['domain']
        rev_timestamp = value['rev_timestamp']
        page_id = value['page_id']
        user_id = value['user_id']
        user_is_bot = bool(value['user_is_bot'])

        # Insert data into "pages_by_domain" table
        print(page_id, domain, rev_timestamp, user_is_bot, user_id)
        client.insert_into_table("wiki.pages_by_domain", [page_id, domain, rev_timestamp, user_is_bot, user_id])

    # Close Cassandra connection
    client.close()
