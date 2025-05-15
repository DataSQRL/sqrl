#
# Copyright © 2021 DataSQRL (contact@datasqrl.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM eclipse-temurin:11.0.22_7-jdk
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

COPY target/sqrl-cli.jar /usr/src/app/sqrl-cli.jar
COPY dockerrun.sh /usr/src/app/dockerrun.sh
ENTRYPOINT ["/usr/src/app/dockerrun.sh"]
