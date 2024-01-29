FROM eclipse-temurin:11
WORKDIR /usr/src/app
ARG POSTGRES_VERSION=14

# Install PostgreSQL
RUN apt-get update && apt-get install -y postgresql postgresql-contrib lsb-release gnupg && rm -rf /var/lib/apt/lists/*

# Install pgvector extension package
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'  && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -  && \
    apt-get update && \
    apt-get install -y postgresql-${POSTGRES_VERSION}-pgvector && \
    rm -rf /var/lib/apt/lists/*

#postgresql-16-pgvector
RUN service postgresql start && \
    su - postgres -c "psql -U postgres -c \"ALTER USER postgres WITH PASSWORD 'postgres';\"" && \
    su - postgres -c "psql -U postgres -c \"CREATE DATABASE datasqrl;\"" && \
    su - postgres -c "psql -U postgres -c \"CREATE EXTENSION vector;\""

COPY target/sqrl-cli.jar /usr/src/app/sqrl-cli.jar
COPY target/flink-job.jar /usr/src/app/flink-job.jar
COPY target/vertx-server.jar /usr/src/app/vertx-server.jar
COPY dockerrun.sh /usr/src/app/dockerrun.sh
