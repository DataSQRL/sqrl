version: "3.8"
services:
<#if database?? && database>
  database:
    build:
      context: database
      dockerfile: Dockerfile
    ports:
      - '5432:5432'
</#if>
  flink-jobmanager:
    build:
      context: stream
      dockerfile: Dockerfile
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jobmanager

  flink-taskmanager:
    build:
      context: stream
      dockerfile: Dockerfile
    depends_on:
      - flink-jobmanager
    command: taskmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 1

  sql-client:
    build:
      context: stream
      dockerfile: Dockerfile
    command: bin/sql-client.sh embedded -f /scripts/flink-plan.sql
    depends_on:
      - flink-jobmanager
      - kafka
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jobmanager
        rest.address: flink-jobmanager

  kafka:
    image: docker.io/bitnami/kafka:3.4.0-debian-11-r38
    ports:
      - "9092:9092"
      - "9094:9094"
    environment:
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT
  kafka-setup:
    image: docker.io/bitnami/kafka:3.4.0-debian-11-r38
    volumes:
      - './log/create-topics.sh:/create-topics.sh'
    command: ['/bin/bash', '/create-topics.sh']
    depends_on:
      - kafka

  server:
    build:
      context: server
      dockerfile: Dockerfile
    depends_on:
      - database
      - kafka-setup
    ports:
      - "8888:8888"
