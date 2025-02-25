FROM eclipse-temurin:11-jdk-noble
WORKDIR /usr/src/app
ARG POSTGRES_VERSION=15

RUN apt-get update && \
    apt-get install -y lsb-release gnupg && \
    rm -rf /var/lib/apt/lists/*

# Add postgresql repo
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'  && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

# Install PostgreSQL and its extensions
RUN apt-get update && \
    apt-get install -y postgresql-${POSTGRES_VERSION} \
                       postgresql-contrib-${POSTGRES_VERSION} \
                       postgresql-${POSTGRES_VERSION}-pgvector && \
                       rm -rf /var/lib/apt/lists/*

# Setup postgres
RUN service postgresql start && \
    su - postgres -c "psql -U postgres -c \"ALTER USER postgres WITH PASSWORD 'postgres';\"" && \
    su - postgres -c "psql -U postgres -c \"CREATE DATABASE datasqrl;\"" && \
    su - postgres -c "psql -U postgres -c \"CREATE EXTENSION vector;\""


# Add the Redpanda repository
RUN curl -1sLf 'https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | bash

# Install Redpanda
RUN apt-get update && apt-get install -y redpanda

COPY sqrl-cli/target/sqrl-cli.jar /opt/sqrl/sqrl-cli.jar
COPY sqrl-run/target/sqrl-run.jar /opt/sqrl/sqrl-run.jar
COPY sqrl-test/target/sqrl-test.jar /opt/sqrl/sqrl-test.jar

COPY dockerrun.sh /opt/sqrl/dockerrun.sh

ENV TZ=America/Los_Angeles

# Expose default Redpanda ports
EXPOSE 8080 8081 8888 9092 9644

WORKDIR /build

ENTRYPOINT ["/opt/sqrl/dockerrun.sh"]
