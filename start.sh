docker compose up -d

docker cp ddl.cql cassandra-node:/ddl.cql
docker exec cassandra-node cqlsh -f ddl.cql
