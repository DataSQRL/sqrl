# Avro for SQRL: Setup and Execution Guide

## Compile SQRL Script
Compile your .sqrl and schema.graphqls using Docker:
```shell
docker run --rm -v $PWD:/build datasqrl/cmd compile c360.sqrl schema.graphqls --mnt $PWD
```

## Add Kafka Topics
Add the 'orders' topic to the init file for kafka. Either modify the create-topics.sh file directly to add the file or copy-paste the command below:

```shell
echo '
#!/bin/bash
/opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka:9092 --topic orders --partitions 1 --replication-factor 1
/opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka:9092 --topic ordercount-1 --partitions 1 --replication-factor 1
exit 0;' > build/deploy/create-topics.sh
```

## Start Sqrl
Start SQRL with Docker Compose:
```shell
(cd build/deploy; docker compose up)
```

## Add Data
Install required Python packages and send Avro messages:

Invoke this a few times.
```
pip3 install confluent-kafka avro-python3

python3 SendAvroMessage.py
```

## Observe New Order Counts

GraphQL Query:
```shell
curl -X POST http://localhost:8888/graphql \
-H "Content-Type: application/json" \
-d '{"query": "{ OrderCount { volume number timeSec } }"}'
```
