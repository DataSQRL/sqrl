#!/bin/bash
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

set -ex
cd /build

# Todo: there is a target flag we need to parse and set
export DATA_PATH=/build/build/deploy/flink/data
export UDF_PATH=/build/build/deploy/flink/lib

REDPANDA_DATA_PATH=/data/redpanda # same as redpanda.yaml
POSTGRES_DATA_PATH=/data/postgres # same as postgres.conf
FLINK_CP_DATA_PATH=/data/flink/checkpoints
FLINK_SP_DATA_PATH=/data/flink/savepoints

if [[ "$1" == "run" || "$1" == "test" || "$1" == "execute" ]]; then
    # Start Redpanda if KAFKA_HOST is not set
    if [[ -z "$KAFKA_HOST" ]]; then
        echo "Starting Redpanda..."
        mkdir -p $REDPANDA_DATA_PATH
        rpk redpanda start --schema-registry-addr 0.0.0.0:8086 --overprovisioned --config /etc/redpanda/redpanda.yaml --smp 1 --memory 1G --reserve-memory 0M --node-id 0 --check=false &

        export KAFKA_HOST=localhost
        export KAFKA_PORT=9092
        export PROPERTIES_BOOTSTRAP_SERVERS=localhost:9092
    fi

    # Create Postgres dir if necessary
    if [[ ! -d "$POSTGRES_DATA_PATH" ]]; then
      mkdir -p $POSTGRES_DATA_PATH
      chown -R postgres:postgres $POSTGRES_DATA_PATH
    fi

    # Init Postgres if necessary
    if [[ -z "$(ls -A "$POSTGRES_DATA_PATH")" ]]; then
      su - postgres -c "/usr/lib/postgresql/${POSTGRES_VERSION}/bin/initdb -D ${POSTGRES_DATA_PATH}"

      service postgresql start
      su - postgres -c "psql -U postgres -c \"ALTER USER postgres WITH PASSWORD 'postgres';\""
      su - postgres -c "psql -U postgres -c \"CREATE DATABASE datasqrl;\""
      su - postgres -c "psql -U postgres -c \"CREATE EXTENSION vector;\""
    fi

    # Start Postgres if POSTGRES_HOST is not set
    if [[ -z "$POSTGRES_HOST" ]]; then
        service postgresql start
        export POSTGRES_HOST=localhost
        export POSTGRES_PORT=5432
        export JDBC_URL="jdbc:postgresql://localhost:5432/datasqrl"
        export JDBC_AUTHORITY="localhost:5432/datasqrl"
        export PGHOST="localhost"
        export PGUSER="postgres"
        export JDBC_USERNAME="postgres"
        export JDBC_PASSWORD="postgres"
        export PGPORT=5432
        export PGPASSWORD="postgres"
        export PGDATABASE="datasqrl"
    fi

    # Wait for Postgres to start
    echo "Waiting for Postgres to start..."
    until pg_isready -h $POSTGRES_HOST -p $POSTGRES_PORT >/dev/null 2>&1; do
        echo "Waiting for Postgres to be ready at $POSTGRES_HOST:$POSTGRES_PORT..."
        sleep 2
    done

    echo "All necessary services are up."

    mkdir -p $FLINK_CP_DATA_PATH
    mkdir -p $FLINK_SP_DATA_PATH
fi

# Get the UID/GID of the /build directory to ensure files are created with correct ownership
BUILD_UID=$(stat -c '%u' /build)
BUILD_GID=$(stat -c '%g' /build)

# Pre-create the build directory with proper ownership to avoid permission errors
mkdir -p /build/build 2>/dev/null || true
chown -R "$BUILD_UID:$BUILD_GID" /build/ 2>/dev/null || true

# Run Java as root
echo "Executing SQRL command: \"$1\" ..."
set +e
java -jar /opt/sqrl/sqrl-cli.jar "${@}"
EXIT_CODE=$?

# After Java execution, fix ownership of any created files
chown -R "$BUILD_UID:$BUILD_GID" /build/ 2>/dev/null || true

set -e
exit $EXIT_CODE
