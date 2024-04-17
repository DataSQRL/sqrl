version: "3.8"
services:
<#if config["pipeline"]?seq_contains("database")>
  database:
    build:
      context: database
      dockerfile: Dockerfile
    ports:
      - '5432:5432'
</#if>
<#if config["pipeline"]?seq_contains("streams")>
  flink-jobmanager:
    build:
      context: streams
      dockerfile: Dockerfile
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jobmanager
<#if mountDir??>
    volumes:
      - ${mountDir}:${mountDir}
</#if>
  flink-taskmanager:
    build:
      context: streams
      dockerfile: Dockerfile
    depends_on:
      - flink-jobmanager
    command: taskmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 1
<#if mountDir??>
    volumes:
      - ${mountDir}:${mountDir}
</#if>
  flink-job-submitter:
    build:
      context: streams
      dockerfile: Dockerfile
    command: flink run /scripts/FlinkJob.jar
    depends_on:
      - flink-jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jobmanager
        rest.address: flink-jobmanager
</#if>
<#if config["pipeline"]?seq_contains("log")>
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
</#if>
<#if config["pipeline"]?seq_contains("server")>
  server:
    build:
      context: server
      dockerfile: Dockerfile
    depends_on:
      - database
      - kafka-setup
    ports:
      - "8888:8888"
</#if>
<#if config["pipeline"]?seq_contains("test")>
  test:
    build: test
    volumes:
      - ./test:/test
<#if config["compiler"]["snapshotPath"]??>
      - ${config["compiler"]["snapshotPath"]}:/test/snapshots
</#if>
    command: ["jmeter", "-n", "-t", "/test/test-plan.jmx", "-l", "/test/results.jtl"]
    depends_on:
      - server
      - flink-job-submitter
</#if>