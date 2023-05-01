FROM gradle:8.1.0-jdk11

# copy built files
COPY target/sqrl-execute-flink-jar-0.1-SNAPSHOT-shaded.jar /sqrl.jar
COPY build.gradle /build.gradle
COPY entrypoint.sh /entrypoint.sh

# Create a workspace directory called /build
WORKDIR /build

RUN chmod +x /entrypoint.sh
# Copy build.gradle into /build directory and invoke the gradle build
ENTRYPOINT ["/entrypoint.sh"]
