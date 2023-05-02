FROM gradle:8.1.0-jdk11

COPY target/vertx-server.jar /vertx-server.jar
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]