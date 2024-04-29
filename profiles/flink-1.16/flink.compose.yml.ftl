version: "3.8"
services:
  flink-jobmanager:
    build:
      context: flink
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
      context: flink
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
      context: flink
      dockerfile: Dockerfile
    command: flink run /scripts/FlinkJob.jar
    depends_on:
      - flink-jobmanager
<#if config["enabled-engines"]?seq_contains("kafka")>
      - kafka-setup
 </#if>
<#if config["enabled-engines"]?seq_contains("postgres")>
      - database
</#if>
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jobmanager
        rest.address: flink-jobmanager
      - PROPERTIES_BOOTSTRAP_SERVERS=kafka:9092
      - JDBC_URL=jdbc:postgresql://database:5432/datasqrl
      - JDBC_USERNAME=postgres
      - JDBC_PASSWORD=postgres