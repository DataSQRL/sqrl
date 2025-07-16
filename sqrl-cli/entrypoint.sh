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
set -o pipefail

# Enable debug mode if DEBUG environment variable is set
if [[ -n "${DEBUG+x}" && -n "$DEBUG" ]]; then
    set -x
fi

cd /build

rm -rf /build/build

# Create logs directory for DataSQRL logging
mkdir -p /tmp/logs

# Todo: there is a target flag we need to parse and set
export DATA_PATH=/build/build/deploy/flink/data
export UDF_PATH=/build/build/deploy/flink/lib

# Get the UID/GID of the /build directory to ensure files are created with correct ownership
BUILD_UID=$(stat -c '%u' /build)
BUILD_GID=$(stat -c '%g' /build)

# Pre-create the build and logs directories with proper ownership to avoid permission errors
mkdir -p /build/build 2>/dev/null || true
chown -R "$BUILD_UID:$BUILD_GID" /build/ 2>/dev/null || true

# Ensure logs directory has proper ownership
chown -R "$BUILD_UID:$BUILD_GID" /tmp/logs 2>/dev/null || true

# Run Java as root
echo "Executing SQRL command: \"$1\" ..."
set +e
java -jar /opt/sqrl/sqrl-cli.jar "${@}"
EXIT_CODE=$?

mv /tmp/logs /build/build/

# After Java execution, fix ownership of any created files in entire /build directory
chown -R "$BUILD_UID:$BUILD_GID" /build/ 2>/dev/null || true

set -e
exit $EXIT_CODE
