docker build -t write_cassandra:1.0 .
docker run --rm --name write_cassandra --network bd-project-network --rm write_cassandra:1.0