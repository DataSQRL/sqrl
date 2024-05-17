FROM datasqrl/sqrl-dependencies:v0.5.0-RC5-SNAPSHOT as m2-dependencies

# Stage 1: Building the dependencies or the fat JAR
FROM gradle:8.6-jdk11 AS build
COPY --from=m2-dependencies /root/.m2 /root/.m2

# Copy the Gradle configuration files
COPY . /app/
WORKDIR /app

# Fetch all necessary dependencies
RUN gradle shadowJar

FROM flink:1.16.1-scala_2.12-java11

RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop
RUN ln -fs /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/flink-s3-fs-hadoop/.

# Exclude this step for fat jar
COPY --from=build /app/build/libs/FlinkJob.jar /scripts/FlinkJob.jar
# UDFs
COPY data/ /data
