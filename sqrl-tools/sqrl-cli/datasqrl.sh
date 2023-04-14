#!/bin/bash
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
java -jar ${parent_path}/target/sqrl-run-0.1-SNAPSHOT-shaded.jar ${@}