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

# Enable debug mode if SQRL_DEBUG environment variable is set
if [[ -n "${SQRL_DEBUG+x}" && -n "$SQRL_DEBUG" ]]; then
  set -x
fi

cd /workspace

# Get the UID/GID of the /workspace directory to ensure files are created with correct ownership
export WORKSPACE_UID=$(stat -c '%u' /workspace)
export WORKSPACE_GID=$(stat -c '%g' /workspace)

exec java $SQRL_JVM_TOOL_OPTS $SQRL_JVM_ARGS -jar /opt/sqrl/sqrl-cli.jar "${@}"
