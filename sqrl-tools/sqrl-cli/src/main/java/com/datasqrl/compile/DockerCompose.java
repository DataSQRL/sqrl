package com.datasqrl.compile;

import java.net.URL;

public class DockerCompose {

  public static String getYml() {
    return "# This is a docker-compose template for starting a DataSQRL compiled data pipeline\n"
        + "# This template uses the Apache Flink as the stream engine, Postgres as the database engine, and Vertx as the server engine.\n"
        + "# It assumes that:\n"
        + "# 1. You ran the `compile` command to compile your SQRL script (and API specification)\n"
        + "# 2. Are in the `build/deploy` directory which contains the deployment artifacts generated by the compiler\n"
        + "# 3. Have built the deployment artifacts for each pipeline engine\n"
        + "# Refer to the deployment documentation for more information:\n"
        + "# https://www.datasqrl.com/docs/reference/operations/deploy/overview/\n"
        + "version: \"3.8\"\n"
        + "services:\n"
        + "  database:\n"
        + "    image: postgres:14.6-alpine\n"
        + "    restart: always\n"
        + "    environment:\n"
        + "      - POSTGRES_USER=postgres\n"
        + "      - POSTGRES_PASSWORD=postgres\n"
        + "      - POSTGRES_DB=datasqrl\n"
        + "    ports:\n"
        + "      - '5432:5432'\n"
        + "    volumes:\n"
        + "      - ./database-schema.sql:/docker-entrypoint-initdb.d/init-schema.sql\n"
        + "\n"
        + "  flink-jobmanager:\n"
        + "    image: flink:1.16.1-scala_2.12-java11\n"
        + "    ports:\n"
        + "      - \"8081:8081\"\n"
        + "    command: jobmanager\n"
        + "    environment:\n"
        + "      - |\n"
        + "        FLINK_PROPERTIES=\n"
        + "        jobmanager.rpc.address: flink-jobmanager\n"
        + "\n"
        + "  flink-taskmanager:\n"
        + "    image: flink:1.16.1-scala_2.12-java11\n"
        + "    depends_on:\n"
        + "      - flink-jobmanager\n"
        + "    command: taskmanager\n"
        + "    environment:\n"
        + "      - |\n"
        + "        FLINK_PROPERTIES=\n"
        + "        jobmanager.rpc.address: flink-jobmanager\n"
        + "        taskmanager.numberOfTaskSlots: 1\n"
        + "\n"
        + "  kafka:\n"
        + "    image: docker.io/bitnami/kafka:3.4\n"
        + "    ports:\n"
        + "      - \"9092:9092\"\n"
        + "    volumes:\n"
        + "      - \"kafka_data:/bitnami\"\n"
        + "    environment:\n"
        + "      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true\n"
        + "      - ALLOW_PLAINTEXT_LISTENER=yes\n"
        + "  kafka-setup:\n"
        + "    image: docker.io/bitnami/kafka:3.4\n"
        + "    volumes:\n"
        + "      - './create-topics.sh:/create-topics.sh'\n"
        + "    command: ['/bin/bash', '/create-topics.sh']\n"
        + "    depends_on:\n"
        + "      - kafka\n"
        + "\n"
        + "  servlet:\n"
        + "    image: eclipse-temurin:11\n"
        + "    command: java -jar vertx-server.jar\n"
        + "    depends_on:\n"
        + "      - database\n"
        + "    ports:\n"
        + "      - \"8888:8888\"\n"
        + "    volumes:\n"
        + "      - ./server-model.json:/model.json\n"
        + "      - ./server-config.json:/config.json\n"
        + "      - ./vertx-server.jar:/vertx-server.jar\n"
        + "\n"
        + "  flink-job-submitter:\n"
        + "    image: curlimages/curl:7.80.0\n"
        + "    depends_on:\n"
        + "      - flink-jobmanager\n"
        + "      - database\n"
        + "      - kafka-setup\n"
        + "    volumes:\n"
        + "      - ./flink-job.jar:/flink-job.jar\n"
        + "    entrypoint: /bin/sh -c\n"
        + "    command: >\n"
        + "      \"while ! curl -s http://flink-jobmanager:8081/overview | grep -q '\\\"taskmanagers\\\":1'; do\n"
        + "        echo 'Waiting for Flink JobManager REST API...';\n"
        + "        sleep 5;\n"
        + "      done;\n"
        + "      echo 'Submitting Flink job...';\n"
        + "      curl -X POST -H 'Content-Type: application/x-java-archive' --data-binary '@/flink-job.jar' http://flink-jobmanager:8081/jars/upload;\n"
        + "      echo 'Job submitted.'\"\n"
        + "\n"
        + "volumes:\n"
        + "  database:\n"
        + "    driver: local\n"
        + "  kafka_data:\n"
        + "    driver: local";
  }
}