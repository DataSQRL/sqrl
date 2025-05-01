#!/bin/bash
set -e
cd /build

# Todo: there is a target flag we need to parse and set
export DATA_PATH=/build/build/deploy/flink/data
export UDF_PATH=/build/build/deploy/flink/lib

echo 'Compiling...this takes about 10 seconds'
java -jar /opt/sqrl/sqrl-cli.jar ${@}

if [ "$1" == "run" ] || [ "$1" == "test" ]; then
   # Determine the jar to run
    if [ "$1" == "run" ]; then
        JAR_NAME="sqrl-run.jar"
    else
        JAR_NAME="sqrl-test.jar"
    fi

    # Start Redpanda if KAFKA_HOST is not set
    if [ -z "$KAFKA_HOST" ]; then
        echo "Starting Redpanda..."
        # corresponds to value in /etc/redpanda/redpanda.yaml redpanda.data_directory
        mkdir -p /data/redpanda/
        rpk redpanda start --schema-registry-addr 0.0.0.0:8086 --overprovisioned --config /etc/redpanda/redpanda.yaml --smp 1 --memory 1G --reserve-memory 0M --node-id 0 --check=false &

        export KAFKA_HOST=localhost
        export KAFKA_PORT=9092
        export PROPERTIES_BOOTSTRAP_SERVERS=localhost:9092
    fi

    # Start Postgres if POSTGRES_HOST is not set
    if [ -z "$POSTGRES_HOST" ]; then
        echo "Starting Postgres..."
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

    echo "All services are up. Starting the main application..."
    echo "$@ $JAR_NAME"

    exec java -jar "/opt/sqrl/$JAR_NAME" "$@"
fi
