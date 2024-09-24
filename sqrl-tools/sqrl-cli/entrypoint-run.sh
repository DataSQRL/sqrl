#!/bin/bash

if [ "$1" == "run" ]; then
    # Start Flink if FLINK_HOST is not set
    if [ -z "$FLINK_HOST" ]; then
        echo "Starting Flink..."
        ${FLINK_HOME}/bin/start-cluster.sh
        export FLINK_HOST=localhost
        export FLINK_PORT=8081
    fi

    # Start Redpanda if KAFKA_HOST is not set
    if [ -z "$KAFKA_HOST" ]; then
        echo "Starting Redpanda..."
        rpk redpanda start --schema-registry-addr 0.0.0.0:8086 --overprovisioned --smp 1 --memory 1G --reserve-memory 0M --node-id 0 --check=false &
        export KAFKA_HOST=localhost
        export KAFKA_PORT=9092
    fi

    # Start Postgres if POSTGRES_HOST is not set
    if [ -z "$POSTGRES_HOST" ]; then
        echo "Starting Postgres..."
        service postgresql start
        export POSTGRES_HOST=localhost
        export POSTGRES_PORT=5432
    fi


    # Wait for Flink to start
    echo "Waiting for Flink to start..."
    until curl -s "http://$FLINK_HOST:$FLINK_PORT" >/dev/null 2>&1; do
        echo "Waiting for Flink REST API to be ready at $FLINK_HOST:$FLINK_PORT..."
        sleep 2
    done

    # Wait for Redpanda (Kafka) to start
#    echo "Waiting for Redpanda (Kafka) to start..."
#    until redpanda ping >/dev/null 2>&1; do
#        echo "Waiting for Redpanda to be ready at $KAFKA_HOST:$KAFKA_PORT..."
#        sleep 2
#    done

    # Wait for Postgres to start
    echo "Waiting for Postgres to start..."
    until pg_isready -h $POSTGRES_HOST -p $POSTGRES_PORT >/dev/null 2>&1; do
        echo "Waiting for Postgres to be ready at $POSTGRES_HOST:$POSTGRES_PORT..."
        sleep 2
    done

    echo "All services are up. Starting the main application..."
    echo "$@"
    bash

    exec java -jar /opt/sqrl-cli.jar "$@"
else
    # Execute any other command provided
    exec "$@"
fi

