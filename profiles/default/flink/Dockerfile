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

WORKDIR /opt/flink/lib


RUN echo "-> Install JARs: Flink's Hive connector" && \
    curl https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/1.18.1/flink-sql-connector-hive-3.1.3_2.12-1.18.1.jar -O

RUN echo "-> Install JARs: Dependencies for Iceberg" && \
    curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.5.2/iceberg-flink-runtime-1.18-1.5.2.jar -O

RUN echo "-> Install JARs: AWS / Hadoop S3" && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -O && \
    curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.763/aws-java-sdk-bundle-1.12.763.jar -O && \
    curl https://repo1.maven.org/maven2/software/amazon/awssdk/glue/2.24.5/glue-2.24.5.jar -O

RUN echo "-> Install JARs: Hadoop" && \
    curl https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.1.1/commons-configuration2-2.1.1.jar -O && \
    curl https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.4/hadoop-auth-3.3.4.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar -O && \
    curl https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar -O && \
    curl https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/5.3.0/woodstox-core-5.3.0.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.4/hadoop-hdfs-client-3.3.4.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.4/hadoop-mapreduce-client-core-3.3.4.jar -O

# Exclude this step for fat jar
COPY --from=build /app/build/libs/FlinkJob.jar /scripts/FlinkJob.jar
# UDFs
COPY data/ /data
