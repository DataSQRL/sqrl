# Avro for SQRL: Setup and Execution Guide

## Compile SQRL Script
Compile your .sqrl and schema.graphqls using Docker:
```shell
docker run --rm -v $PWD:/build datasqrl/cmd compile c360.sqrl schema.graphqls --mnt $PWD
```

## Add Kafka Topics
Create a script to add orders and ordercount-1 topics in Kafka:
```shell
echo '
#!/bin/bash
/opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka:9092 --topic orders --partitions 1 --replication-factor 1
/opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka:9092 --topic ordercount-1 --partitions 1 --replication-factor 1
exit 0;' > build/deploy/create-topics.sh
```
## Modify Docker Compose
Update Docker Compose for Kafka with external connection capabilities:

```yaml
  kafka:
    image: docker.io/bitnami/kafka:3.4.0-debian-11-r38
    ports:
      - "9092:9092"
      - "9094:9094"
    volumes:
      - "kafka_data:/bitnami"
    environment:
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT
  kafka-setup:
    image: docker.io/bitnami/kafka:3.4.0-debian-11-r38
    volumes:
      - './create-topics.sh:/create-topics.sh'
    command: ['/bin/bash', '/create-topics.sh']
    depends_on:
      - kafka

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
