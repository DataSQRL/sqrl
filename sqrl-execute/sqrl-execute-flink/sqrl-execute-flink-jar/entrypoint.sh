#!/bin/sh
export SQRL_JAR_PATH=/sqrl.jar
cd /build
#cp /build.gradle build.gradle
gradle clean shadowJar
mkdir -p /build/deploy/
mv /build/build/libs/build-all.jar /build/deploy/flink-job.jar
gradle clean
echo "Done."
