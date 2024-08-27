version: "3.8"
services:
  server:
    build:
      context: vertx
      dockerfile: Dockerfile
<#if config["enabled-engines"]?seq_contains("kafka") || config["enabled-engines"]?seq_contains("postgres")>
    depends_on:
</#if>
<#if config["enabled-engines"]?seq_contains("postgres")>
      database:
        condition: service_started
</#if>
<#if config["enabled-engines"]?seq_contains("kafka")>
      kafka-setup:
        condition: service_completed_successfully
</#if>
    ports:
      - "8888:8888"
    env_file:
      - ".env"
    environment:
<#if config["enabled-engines"]?seq_contains("kafka")>
      - PROPERTIES_BOOTSTRAP_SERVERS=kafka:9092
</#if>
      - PGHOST=database
      - PGPORT=5432
      - PGDATABASE=datasqrl
      - PGUSER=postgres
      - PGPASSWORD=postgres
