# Stage 1: Building the dependencies or the fat JAR
FROM gradle:8.6-jdk11 AS build

# Copy the Gradle configuration files
COPY build.gradle /app/
WORKDIR /app

# Fetch all necessary dependencies
RUN gradle copyDependencies

FROM flink:1.16.1-scala_2.12-java11

RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop
RUN ln -fs /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/flink-s3-fs-hadoop/.

# Exclude this step for fat jar
COPY --from=build /app/build/dependencies /opt/flink/lib/
# UDFs
COPY lib/ /opt/flink/lib/

# Copy plan to docker for easier inspection
COPY flink-plan.sql /scripts/flink-plan.sql
COPY flink-conf.yaml /scripts/flink-conf.yaml