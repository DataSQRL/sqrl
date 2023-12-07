package com.datasqrl.compile;

import java.net.URL;
import java.nio.file.Path;
import java.util.Optional;

public class DockerCompose {

  public static String getYml(Optional<Path> mountDir) {
    String volumneMnt = mountDir.map(dir -> dir.toAbsolutePath().normalize())
        .map(dir -> ""
            + "    volumes:\n"
            + "      - " + dir.toAbsolutePath() + ":/build\n"
            + "      - ./init-flink.sh:/exec/init-flink.sh\n")
        .orElse(""
            + "    volumes:\n"
            + "      - ./init-flink.sh:/exec/init-flink.sh\n");

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
        + "    image: ankane/pgvector:v0.5.0\n"
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
        + "    command: /bin/bash /exec/init-flink.sh jobmanager\n"
        + "    environment:\n"
        + "      - |\n"
        + "        FLINK_PROPERTIES=\n"
        + "        jobmanager.rpc.address: flink-jobmanager\n"
        + volumneMnt
        + "\n"
        + "  flink-taskmanager:\n"
        + "    image: flink:1.16.1-scala_2.12-java11\n"
        + "    depends_on:\n"
        + "      - flink-jobmanager\n"
        + "    command: /bin/bash /exec/init-flink.sh taskmanager\n"
        + "    environment:\n"
        + "      - |\n"
        + "        FLINK_PROPERTIES=\n"
        + "        jobmanager.rpc.address: flink-jobmanager\n"
        + "        taskmanager.numberOfTaskSlots: 1\n"
        + volumneMnt
        + "\n"
        + "  kafka:\n"
        + "    image: docker.io/bitnami/kafka:3.4.0-debian-11-r38\n"
        + "    ports:\n"
        + "      - \"9092:9092\"\n"
        + "    volumes:\n"
        + "      - \"kafka_data:/bitnami\"\n"
        + "    environment:\n"
        + "      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true\n"
        + "      - ALLOW_PLAINTEXT_LISTENER=yes\n"
        + "  kafka-setup:\n"
        + "    image: docker.io/bitnami/kafka:3.4.0-debian-11-r38\n"
        + "    volumes:\n"
        + "      - './create-topics.sh:/create-topics.sh'\n"
        + "    command: ['/bin/bash', '/create-topics.sh']\n"
        + "    depends_on:\n"
        + "      - kafka\n"
        + "\n"
        + "  server:\n"
        + "    image: eclipse-temurin:11\n"
        + "    command: java -jar vertx-server.jar\n"
        + "    depends_on:\n"
        + "      - database\n"
        + "      - kafka-setup\n"
        + "    ports:\n"
        + "      - \"8888:8888\"\n"
        + "    volumes:\n"
        + "      - ./server-model.json:/server-model.json\n"
        + "      - ./server-config.json:/server-config.json\n"
        + "      - ./vertx-server.jar:/vertx-server.jar\n"
        + "\n"
        + "  flink-job-submitter:\n"
        + "    image: badouralix/curl-jq:alpine\n"
        + "    depends_on:\n"
        + "      - flink-jobmanager\n"
        + "      - database\n"
        + "      - kafka-setup\n"
        + "    volumes:\n"
        + "      - ./flink-job.jar:/flink-job.jar\n"
        + "      - ./submit-flink-job.sh:/submit-flink-job.sh\n"
        + "    entrypoint: /submit-flink-job.sh\n"
        + "\n"
        + "volumes:\n"
        + "  database:\n"
        + "    driver: local\n"
        + "  kafka_data:\n"
        + "    driver: local";
  }

  public static String getInitFlink() {
    return "#!/bin/bash\n"
        + "\n"
        + "# Copy the JAR file to the plugins directory\n"
        + "mkdir -p /opt/flink/plugins/s3-fs-presto\n"
        + "cp /opt/flink/opt/flink-s3-fs-presto-1.16.1.jar /opt/flink/plugins/s3-fs-presto/\n"
        + "# Execute the passed command\n"
        + "exec /docker-entrypoint.sh \"$@\"\n";
  }
  public static String getFlinkExecute() {
    return "#!/bin/sh\n"
        + "\n"
        + "while ! curl -s http://flink-jobmanager:8081/overview | grep -q '\"taskmanagers\":1'; do\n"
        + "  echo 'Waiting for Flink JobManager REST API...';\n"
        + "  sleep 5;\n"
        + "done;\n"
        + "echo 'Submitting Flink job...';\n"
        + "upload_response=$(curl -X POST -H \"Expect:\" -F \"jarfile=@flink-job.jar\" http://flink-jobmanager:8081/jars/upload);\n"
        + "echo \"$upload_response\";\n"
        + "jar_id=$(echo \"$upload_response\" | jq -r '.filename');\n"
        + "echo \"$jar_id\";\n"
        + "filename=$(echo \"$jar_id\" | awk -F'/' '{print $NF}');\n"
        + "sleep 3;\n"
        + "echo \"$filename\"\n"
        + "echo \"curl -X POST \"http://flink-jobmanager:8081/jars/${filename}/run\";\";\n"
        + "post_response=$(curl -X POST \"http://flink-jobmanager:8081/jars/${filename}/run\");\n"
        + "echo \"$post_response\";\n"
        + "echo 'Job submitted.'";
  }
}
