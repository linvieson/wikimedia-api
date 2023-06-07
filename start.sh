docker compose up -d

docker cp ddl.cql cassandra-node:/ddl.cql
echo 'COPIED LOCAL CQL FILE'

docker exec cassandra-node cqlsh -f ddl.cql
echo 'FINISHED EXECUTING DDL CQL FILE'

# docker run -it --rm --network big-data-project-data_manipulations_bd-project-network -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 bitnami/kafka:latest kafka-console-consumer.sh --bootstrap-server kafka-server:9092 --topic request-topic -from-beginning
