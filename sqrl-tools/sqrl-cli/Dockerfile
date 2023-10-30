FROM eclipse-temurin:11
WORKDIR /usr/src/app

# Install PostgreSQL
RUN apt-get update && apt-get install -y postgresql postgresql-contrib && rm -rf /var/lib/apt/lists/*

RUN service postgresql start && \
    su - postgres -c "psql -U postgres -c \"ALTER USER postgres WITH PASSWORD 'postgres';\"" && \
    su - postgres -c "psql -U postgres -c \"CREATE DATABASE datasqrl;\""

COPY target/sqrl-cli.jar /usr/src/app/sqrl-cli.jar
COPY target/flink-job.jar /usr/src/app/flink-job.jar
COPY target/vertx-server.jar /usr/src/app/vertx-server.jar
COPY dockerrun.sh /usr/src/app/dockerrun.sh
ENTRYPOINT ["/usr/src/app/dockerrun.sh"]