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

FROM eclipse-temurin:17

# Create directories for application, configuration, and logs
RUN mkdir -p /opt/sqrl/app /opt/sqrl/config /opt/sqrl/logs

# Copy application jar to separate directory
COPY target/vertx-server.jar /opt/sqrl/app/vertx-server.jar
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

# Set working directory to config directory by default
WORKDIR /opt/sqrl/config

ENTRYPOINT ["/entrypoint.sh"]