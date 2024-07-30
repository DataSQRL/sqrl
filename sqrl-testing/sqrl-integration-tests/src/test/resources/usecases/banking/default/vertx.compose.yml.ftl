version: "3.8"
services:
  server:
    build:
      context: vertx
      dockerfile: Dockerfile
    depends_on:
      database:
        condition: service_started
<#if config["enabled-engines"]?seq_contains("kafka")>
      kafka-setup:
        condition: service_completed_successfully
</#if>
    ports:
      - "8888:8888"
    environment:
      - PROPERTIES_BOOTSTRAP_SERVERS=kafka:9092
      - PGHOST=database
      - PGPORT=5432
      - PGDATABASE=datasqrl
      - PGUSER=postgres
      - PGPASSWORD=postgres