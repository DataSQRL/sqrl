#!/bin/bash
#
# Copyright © 2021 DataSQRL (contact@datasqrl.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
