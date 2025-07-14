#!/bin/sh
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

# Enable debug mode if DEBUG environment variable is set
if [[ -n "${DEBUG+x}" && -n "$DEBUG" ]]; then
    set -x
fi

# Change to config directory (default WORKDIR)
cd /opt/sqrl/config

# Run the application jar from the app directory using SqrlLauncher to enable metrics
java -cp /opt/sqrl/app/vertx-server.jar com.datasqrl.graphql.SqrlLauncher