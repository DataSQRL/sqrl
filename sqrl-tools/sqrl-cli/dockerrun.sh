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
