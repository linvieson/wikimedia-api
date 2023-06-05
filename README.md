**Startup order:**
* Kafka installation and cluster creation: `kafka-related/run_kafka_cluster.sh`
* Reading data from wiki link, transforming it and sending to Kafka: `kafka-related/to_kafka.sh` \
  **!** Allow the container to run for at least 5 min before going to the next step
* Creating keyspace and table in Cassandra: `cassandra-related/run_cassandra-cluster.sh`\
  **!** Wait for 70 seconds before going to the next step
* Writing data from Kafka to Cassandra: `from_kafka_to_cassandra/to_cassandra.sh`