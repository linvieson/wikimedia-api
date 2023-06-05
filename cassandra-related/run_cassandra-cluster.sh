docker run --name cassandra-node --network bd-project-network -p 9042:9042 -d cassandra:latest
sleep 70
docker cp ddl.cql cassandra-node:/
docker exec cassandra-node cqlsh -f ddl.cql
