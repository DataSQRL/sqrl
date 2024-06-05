# Stage 1: Building the dependencies and the fat JAR
FROM gradle:8.6-jdk11 AS build

WORKDIR /app

COPY build.gradle /app/build.gradle

# Fetch all necessary dependencies
RUN gradle --no-daemon --console=plain build

COPY . /app/
# Create fat jar
RUN gradle --no-daemon --console=plain shadowJar

FROM flink:1.18.1-scala_2.12-java11

RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop
RUN ln -fs /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/flink-s3-fs-hadoop/.

# Exclude this step for fat jar
COPY --from=build /app/build/libs/FlinkJob.jar /scripts/FlinkJob.jar
# UDFs
COPY data/ /data
