#!/bin/bash
set -e
cd /build

if [ "$1" = "run" ]; then
  echo "Validating script..."
  java -jar /usr/src/app/sqrl-cli.jar validate "${@:2}"

  if [ $? -ne 0 ]; then
    exit 1
  fi
  service postgresql start

  echo "Starting services..."
  while ! pg_isready -q; do
      sleep 1
  done
fi

echo 'Compiling...this takes about 10 seconds'
java -jar /usr/src/app/sqrl-cli.jar ${@}

FILE=/build/build/deploy/flink-plan.json
SQL_FILE=/build/build/deploy/flink-plan.sql
CONFIG_FILE=/build/build/deploy/flink-config.yaml
if [ -f "$FILE" ] && [ "$1" = "compile" ]; then
  echo 'Building deployment assets...this takes about a minute'
  mkdir -p /build/build/deploy/
  cp /usr/src/app/flink-job.jar /build/build/deploy/flink-job.jar
  cp /usr/src/app/vertx-server.jar /build/build/deploy/vertx-server.jar
  mkdir -p /flink/build/deploy
  cp $FILE /flink/build/deploy
  if [ -f "$SQL_FILE" ]; then
    cp $SQL_FILE /flink/build/deploy
  fi
  if [ -f "$CONFIG_FILE" ]; then
    cp $CONFIG_FILE /flink/build/deploy
  fi
  cd /flink
  jar -uf /build/build/deploy/flink-job.jar build
fi