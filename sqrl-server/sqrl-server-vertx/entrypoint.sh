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

# Enable debug mode if DEBUG environment variable is set
if [[ -n "${SQRL_DEBUG+x}" && -n "$SQRL_DEBUG" ]]; then
    SQRL_JVM_ARGS="-Dlog4j2.configurationFile=/opt/sqrl/app/log4j2-debug.properties"
    set -x
fi

# Change to config directory (default WORKDIR)
cd /opt/sqrl/config

# Run the application jar from the app directory using SqrlLauncher to enable metrics
exec java $SQRL_JVM_ARGS -cp /opt/sqrl/app/vertx-server.jar com.datasqrl.graphql.SqrlLauncher
