docker build -t write_kafka:1.0 .
docker run --rm --name write_kafka --network bd-project-network --rm write_kafka:1.0