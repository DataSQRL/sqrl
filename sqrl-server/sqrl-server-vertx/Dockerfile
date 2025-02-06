FROM eclipse-temurin:21

COPY target/vertx-server.jar /opt/sqrl/vertx-server.jar
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]