#!/bin/bash
set -e
cd /build

echo 'Compiling...this takes about 10 seconds'
java -jar /usr/src/app/sqrl-cli.jar ${@}

if [ "$1" == "run" ]; then
    JAR_NAME="sqrl-run.jar"
    service postgresql start

    exec java -jar "/usr/src/app/$JAR_NAME" "$@"
fi
