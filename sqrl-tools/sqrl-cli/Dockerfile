FROM eclipse-temurin:11
WORKDIR /usr/src/app
COPY target/sqrl-cli.jar /usr/src/app/sqrl-cli.jar
COPY target/flink-job.jar /usr/src/app/flink-job.jar
COPY target/vertx-server.jar /usr/src/app/vertx-server.jar
COPY dockerrun.sh /usr/src/app/dockerrun.sh
ENTRYPOINT ["/usr/src/app/dockerrun.sh"]