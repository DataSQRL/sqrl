#!/bin/bash
set -e
cd /build
echo 'Compiling...this takes about 10 seconds'
java -jar /usr/src/app/sqrl-cli.jar ${@}

FILE=/build/build/deploy/flink-plan.json
if [ -f "$FILE" ] && [ "$1" = "compile" ]; then
  mkdir -p /build/build/deploy/
  mkdir -p /flink/build/deploy
  cp $FILE /flink/build/deploy
  cp /usr/src/app/flink-job.jar /build/build/deploy/flink-job.jar
  cp /usr/src/app/vertx-server.jar /build/build/deploy/vertx-server.jar
fi