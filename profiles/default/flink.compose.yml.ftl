version: "3.8"
services:
  flink-jobmanager:
    build:
      context: flink
      dockerfile: Dockerfile
    ports:
      - "8081:8081"
    command: jobmanager
    env_file:
      - ".env"
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jobmanager
<#if config["values"]?? && config["values"]["mountDir"]??>
    volumes:
      - ${config["values"]["mountDir"]}
</#if>
  flink-taskmanager:
    build:
      context: flink
      dockerfile: Dockerfile
    depends_on:
      - flink-jobmanager
    command: taskmanager
    env_file:
      - ".env"
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 1
<#if config["values"]?? && config["values"]["mountDir"]??>
    volumes:
      - ${config["values"]["mountDir"]}
</#if>
  flink-job-submitter:
    build:
      context: flink
      dockerfile: Dockerfile
    command: flink run /scripts/FlinkJob.jar
    env_file:
      - ".env"
    depends_on:
      flink-jobmanager:
        condition: service_started
<#if config["enabled-engines"]?seq_contains("kafka")>
      kafka-setup:
        condition: service_completed_successfully
 </#if>
<#if config["enabled-engines"]?seq_contains("postgres") || config["enabled-engines"]?seq_contains("postgres_log")>
      database:
        condition: service_healthy
</#if>
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jobmanager
        rest.address: flink-jobmanager
      - PROPERTIES_BOOTSTRAP_SERVERS=kafka:9092
      - PROPERTIES_GROUP_ID=mygroupid
      - PGHOST=database
      - JDBC_URL=jdbc:postgresql://database:5432/datasqrl
      - JDBC_USERNAME=postgres
      - JDBC_PASSWORD=postgres
      - DATA_PATH=/data