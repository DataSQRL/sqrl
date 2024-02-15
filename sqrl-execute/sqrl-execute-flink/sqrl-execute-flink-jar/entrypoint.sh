#!/bin/sh
export SQRL_JAR_PATH=/sqrl.jar
cd /build
# avoid including sql by default
if [ -f /build/deploy/flink-plan.sql ]; then
    mv /build/deploy/flink-plan.sql /build/flink-plan.sql
fi

if [ -f /build/deploy/flink-job.jar ]; then
    rm /build/deploy/flink-job.jar
fi

gradle clean shadowJar
mkdir -p /build/deploy/
mv /build/build/libs/build-all.jar /build/deploy/flink-job.jar

if [ -f /build/deploy/flink-plan.sql ]; then
    mv /build/flink-plan.sql /build/deploy/flink-plan.sql
fi
gradle clean
echo "Done."
