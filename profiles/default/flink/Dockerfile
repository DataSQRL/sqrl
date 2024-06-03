# Stage 1: Building the dependencies and the fat JAR
FROM gradle:8.6-jdk11 AS build

COPY build.gradle /app/build.gradle
WORKDIR /app

# Fetch all necessary dependencies
RUN gradle --no-daemon --console=plain build

COPY . /app/
# Create fat jar
RUN gradle --no-daemon --console=plain shadowJar

FROM flink:1.18.1-scala_2.12-java11

RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/com/datasqrl/sqrl-lib-common/0.5.0/sqrl-lib-common-0.5.0.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/com/datasqrl/sqrl-flexible-csv/0.5.0/sqrl-flexible-csv-0.5.0.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/com/datasqrl/sqrl-errors/0.5.0/sqrl-errors-0.5.0.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/com/datasqrl/sqrl-secure/0.5.0/sqrl-secure-0.5.0.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/com/datasqrl/sqrl-jdbc-1.18/0.5.0/sqrl-jdbc-1.18-0.5.0.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/com/datasqrl/sqrl-time/0.5.0/sqrl-time-0.5.0.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/com/datasqrl/sqrl-text/0.5.0/sqrl-text-0.5.0.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/com/datasqrl/sqrl-json/0.5.0/sqrl-json-0.5.0.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/com/datasqrl/sqrl-name/0.5.0/sqrl-name-0.5.0.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/com/datasqrl/sqrl-flexible-json/0.5.0/sqrl-flexible-json-0.5.0.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/com/datasqrl/sqrl-vector/0.5.0/sqrl-vector-0.5.0.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/flink/flink-avro-confluent-registry/1.18.1/flink-avro-confluent-registry-1.18.1.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/flink/flink-avro/1.18.1/flink-avro-1.18.1.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.1.0-1.18/flink-connector-kafka-3.1.0-1.18.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop
RUN ln -fs /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/flink-s3-fs-hadoop/.

# Exclude this step for fat jar
COPY --from=build /app/build/libs/FlinkJob.jar /scripts/FlinkJob.jar
# UDFs
COPY data/ /data
