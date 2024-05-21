# Stage 1: Building the dependencies and the fat JAR
FROM gradle:8.6-jdk11 AS build

# Set the working directory in the Docker image
WORKDIR /app

# First, copy only the build.gradle file(s) into the Docker image
COPY build.gradle /app/

# Download and cache the project dependencies
# We are using `--no-daemon` and `--console=plain` for better CI logs
RUN gradle --no-daemon --console=plain dependencies

# Copy the Gradle configuration files
COPY . /app/
WORKDIR /app

# Fetch all necessary dependencies
RUN gradle --no-daemon --console=plain shadowJar

FROM flink:1.18.1-scala_2.12-java11

RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop
RUN ln -fs /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/flink-s3-fs-hadoop/.

# Exclude this step for fat jar
COPY --from=build /app/build/libs/FlinkJob.jar /scripts/FlinkJob.jar
# UDFs
COPY data/ /data
