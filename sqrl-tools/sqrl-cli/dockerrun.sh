#!/bin/bash
set -e
cd /build

if [ "$1" = "run" ]; then
  service postgresql start

  # Wait for PostgreSQL to be ready
  echo "Waiting for PostgreSQL to be ready..."
  while ! pg_isready -q; do
      sleep 1
  done
fi

echo 'Compiling...this takes about 10 seconds'
java -jar /usr/src/app/sqrl-cli.jar ${@}

# Start PostgreSQL if the argument is 'run'


FILE=/build/build/deploy/flink-plan.json
if [ -f "$FILE" ] && [ "$1" = "compile" ]; then
  mkdir -p /build/build/deploy/
  cp /usr/src/app/flink-job.jar /build/build/deploy/flink-job.jar
  cp /usr/src/app/vertx-server.jar /build/build/deploy/vertx-server.jar
  mkdir -p /flink/build/deploy
  cp $FILE /flink/build/deploy
  echo 'Building deployment assets...this takes about a minute'
  cd /flink
  jar -uf /build/build/deploy/flink-job.jar build
fi